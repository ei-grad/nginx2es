import logging

import threading

from elasticsearch import JSONSerializer
from elasticsearch.helpers import streaming_bulk


class Nginx2ES(object):

    def __init__(self, es, parser, index, chunk_size=500, max_retries=3, max_delay=10.):
        self.es = es
        self.parser = parser
        self.index = index
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.max_delay = max_delay

    def gen(self, file):
        for line_num, (inode, pos, line) in enumerate(file):
            doc = self.parser(line)
            if doc is not None:
                yield {
                    '_id': '%s-%s-%s-%s' % (doc['@hostname'], inode, pos,
                                            doc['@timestamp'].strftime('%s')),
                    '_index': doc['@timestamp'].strftime(self.index),
                    '_type': 'nginx2es',
                    '_source': doc
                }

    def run(self, file):

        buffer = []

        filled = threading.Event()
        flushed = threading.Event()
        eof = threading.Event()
        buffer_lock = threading.Lock()

        def filler():
            for i in self.gen(file):
                while len(buffer) > self.chunk_size:
                    filled.set()
                    flushed.wait()
                with buffer_lock:
                    buffer.append(i)
            eof.set()
            filled.set()

        filler_thread = threading.Thread(target=filler)
        filler_thread.daemon = True
        filler_thread.start()

        def flusher():
            while not eof.is_set():
                filled.wait(self.max_delay)
                if buffer:
                    with buffer_lock:
                        to_flush = list(buffer)
                        buffer.clear()
                    for success, response in streaming_bulk(
                            self.es, to_flush,
                            max_retries=self.max_retries,
                            yield_ok=False,
                    ):
                        logging.error(response)
                flushed.set()

        flusher_thread = threading.Thread(target=flusher)
        flusher_thread.daemon = True
        flusher_thread.start()

        filler_thread.join()
        flusher_thread.join()

    def stdout(self, file):
        s = JSONSerializer()
        for i in self.gen(file):
            print(s.dumps(i))
