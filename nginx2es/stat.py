from time import time
from collections import defaultdict
import logging
import threading
import socket

import numpy as np
import pandas


class Stat(threading.Thread):

    fields = set([
        # dimensions
        'status',
        'host', 'request_path_1', 'request_path_2',
        'upstream_cache_status',
        # metrics
        'request_time', 'upstream_response_time', 'bytes_sent',
    ])

    quantiles = [.50, .75, .90, .99]

    def __init__(self, prefix, host, port=2003, use_udp=False, interval=10):
        super(Stat, self).__init__()
        self.prefix = prefix
        self.host = host
        self.port = port
        self.use_udp = use_udp
        self.daemon = True
        self.eof = threading.Event()
        self.interval = interval
        self.lock = threading.Lock()
        self.buffers = defaultdict(list)
        self.delay = {}
        self.output = None

    def connect(self):
        if self.output is not None:
            self.output.close()
        if self.use_udp:
            socktype = socket.SOCK_DGRAM
        else:
            socktype = socket.SOCK_STREAM
        addrinfo = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC, socktype)
        for af, socktype, proto, canonname, sa in addrinfo:
            try:
                s = socket.socket(af, socktype, proto)
            except socket.error as msg:
                s = None
                continue
            try:
                s.connect(sa)
            except socket.error as msg:
                s.close()
                s = None
                continue
            break
        if s is None:
            raise Exception("Can't connect to carbon!")
        self.output = s.makefile('w')

    def hit(self, row):
        with self.lock:
            if row['status'] == 0:
                # ignore non-http connections
                return
            ts = self.timestamp(row['@timestamp'])
            self.delay[ts] = time() + self.interval
            d = {k: v for k, v in row.items() if k in self.fields}
            self.buffers[ts].append(d)

    def timestamp(self, dt):
        ts = dt.timestamp()
        return int(ts - ts % self.interval)

    def run(self):
        while not self.eof.wait(self.interval - time() % self.interval):
            self.process(self.get_ready_buffers())
        self.process(self.buffers)

    def get_ready_buffers(self):
        ready = {}
        with self.lock:
            current_time = time()
            for ts, delayed_to in list(self.delay.items()):
                if delayed_to > current_time:
                    del self.delay[ts]
                    ready[ts] = self.buffers.pop(ts)
        return ready

    def process(self, buffers):
        for ts, rows in buffers.items():
            try:
                try:
                    self.send_metrics(self.metrics(rows), ts)
                except socket.error:
                    # retry on network error
                    self.send_metrics(self.metrics(rows), ts)
            except:
                logging.error("can't send metrics", exc_info=True)

    def send_metrics(self, metrics, timestamp):
        for name, value in metrics:
            if isinstance(value, float):
                value = '%.3f' % value
            metric_string = "%s %s %s\n" % (name, value, timestamp)
            self.output.write(metric_string)
        self.output.flush()

    def log10_bins(self, series):
        # TODO: add docstring!
        # Values starting from -30, which corresponds to arguments starting from 0.001.
        # The -31 value corresponds to the arguments which are less than 0.001.
        pow10 = (np.log10(series.replace(0, np.nan)) * 10.).fillna(-31).astype(np.int)
        return (10. ** (pow10 / 10) * 1000).map(lambda x: '%d' % x)

    def metrics(self, rows):

        if not rows:
            return

        df = pandas.DataFrame.from_records(rows)

        if 'request_path_1' not in df:
            df['request_path_1'] = '#'

        df['request_path_1'].fillna('#', inplace=True)

        if 'request_path_2' not in df:
            df['request_path_2'] = '#'

        df['request_path_2'].fillna('#', inplace=True)

        if 'upstream_cache_status' not in df:
            df['upstream_cache_status'] = 'NONE'

        df['upstream_cache_status'].fillna('NONE', inplace=True)

        if 'upstream_response_time' not in df:
            df['upstream_response_time'] = np.nan

        # upstream_response_time is a list (nginx could ask several upstreams
        # per single request if the first upstream fails), but I believe it
        # doesn't worth powder and shot to deliver all these times to carbon
        # (just the last - should be enought), the backend logs should be
        # delivered separately instead
        df['upstream_response_time'] = df.upstream_response_time.map(
            lambda x: x[-1] if isinstance(x, list) else x)

        # these histograms could be used to specify colors or calculate
        # aggregatable percentiles approximation
        df['request_time_interval'] = self.log10_bins(df['request_time'])
        df['upstream_response_time_interval'] = self.log10_bins(df['upstream_response_time'])

        # request counts
        for dims, value in df.groupby([
                'host', 'request_path_1', 'request_path_2', 'status',
                'upstream_cache_status',
                'request_time_interval', 'upstream_response_time_interval'
        ]).size().items():
            yield self.metric_name('count', dims), value

        # request time sum / count
        g = df.groupby([
                'host', 'request_path_1', 'request_path_2', 'status',
        ]).request_time
        for dims, value in g.sum().items():
            yield self.metric_name('request_time', 'sum', dims), value
        for dims, value in g.count().items():
            yield self.metric_name('request_time', 'count', dims), value

        # upstream response time sum / count
        g = df[~df.upstream_response_time.isna()].groupby([
                'host', 'request_path_1', 'request_path_2', 'status',
        ]).upstream_response_time
        for dims, value in g.sum().items():
            yield self.metric_name('upstream_response_time', 'sum', dims), value
        for dims, value in g.count().items():
            yield self.metric_name('upstream_response_time', 'count', dims), value

        # sent bytes
        for dims, value in df.groupby([
                'host', 'request_path_1', 'request_path_2', 'status',
        ]).bytes_sent.sum().items():
            yield self.metric_name('bytes_sent', dims), value

        # It doesn't make sense to drill exact percentiles deeper than host, because
        # you don't really want to know request time percentiles for any
        # request path and they can't be re-aggregated from drilled values.
        # Instead, the approximation of different request paths percentiles
        # should be calculated from the histograms (there is a handy
        # quantileExactWeighted() function in clickhouse for that).

        # request_time percentiles
        g = df.groupby('host')
        for (host, p), value in g.request_time.quantile(self.quantiles).items():
            yield self.metric_name('request_time', 'percentiles',
                                   host, 'p%d' % (p * 100)), value

        g = df[~df.upstream_response_time.isna()].groupby('host')
        # upstream_response_time percentiles
        for (host, p), value in g.upstream_response_time.quantile(self.quantiles).items():
            yield self.metric_name('upstream_response_time', 'percentiles',
                                   host, 'p%d' % (p * 100)), value

    def metric_name(self, *args):
        parts = self.prefix.split('.')
        for i in args:
            if isinstance(i, (list, tuple)):
                parts.extend(i)
            else:
                parts.append(i)
        return '.'.join(str(i).replace('.', '_') for i in parts)
