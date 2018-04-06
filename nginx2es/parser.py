"""
Parse json-encoded nginx access.log and put parsed lines to Elasticsearch.
"""

from six.moves.urllib.parse import splitquery, parse_qs
import dateutil.parser
import fast_json as json


class AccessLogParser(object):
    def __init__(self, hostname, extensions=None, geoip=None,
                 timestamp_parser=dateutil.parser.parse):
        self.hostname = hostname
        self.extensions = extensions or []
        self.timestamp_parser = timestamp_parser
        self.geoip = geoip

    def __call__(self, line):

        d = json.loads(line)

        d['@timestamp'] = self.timestamp_parser(d.pop('timestamp'))
        if self.hostname is not None:
            d['@hostname'] = self.hostname

        for i in list(d):
            if d[i] in ('-', ''):
                del d[i]

        if 'request_uri' not in d and 'request' in d:
            s = d['request'].split()
            if len(s) == 3:
                _, d['request_uri'], d['server_protocol'] = s
                del d['request']

        if 'request_uri' in d:

            d['request_path'], d['request_qs'] = splitquery(d['request_uri'])

            if d['request_qs'] is None:
                del d['request_qs']

            else:

                d['query'] = parse_qs(d['request_qs'])

                for i in list(d['query']):
                    if '.' in i:
                        d['query'][i.replace('.', '_')] = d['query'].pop(i)

                lon_alias = 'lng' if 'lng' in d['query'] else 'lon'
                if 'lat' in d['query'] and lon_alias in d['query']:
                    try:
                        d['query_geo'] = {
                            'lat': float(d['query']['lat'][0]),
                            'lon': float(d['query'][lon_alias][0]),
                        }
                    except ValueError:
                        pass

            for n, i in enumerate(d['request_path'].split('/')):
                if i:  # skip the empty 0-th and last components
                    d['request_path_%d' % n] = i

        for i in ['request_length', 'connection_requests', 'bytes_sent', 'connection']:
            if i in d:
                d[i] = int(d[i])

        for i in ['request_time', 'gzip_ratio']:
            if i in d:
                d[i] = float(d[i])

        for i in [
                'forwarded_for', 'upstream_addr', 'upstream_status',
                'upstream_response_time', 'upstream_response_length',
                'upstream_connect_time',
        ]:
            if i not in d:
                continue
            d[i] = [j.strip() for j in d[i].replace(', ', ' : ').split(' : ')]
            d[i] = [j for j in d[i] if j not in ('', '-')]
            if not d[i]:
                del d[i]

        if 'upstream_response_time' in d:
            d['upstream_response_time'] = [
                float(i) for i in d['upstream_response_time']
            ]

        if 'upstream_connect_time' in d:
            d['upstream_connect_time'] = [
                float(i) for i in d['upstream_connect_time']
            ]

        if 'upstream_response_length' in d:
            d['upstream_response_length'] = [
                int(i) for i in d['upstream_response_length']
            ]

        if self.geoip is not None:
            g = self.geoip.record_by_name(d['remote_addr'])
            if g is not None:
                d['geoip'] = {
                    'lat': g['latitude'],
                    'lon': g['longitude'],
                }
                d['city'] = g['city']
                d['region_name'] = g['region_name']

        for ext in self.extensions:
            ext(d)

        return d
