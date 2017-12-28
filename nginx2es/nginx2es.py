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
                buffer_lock.acquire()
                buffer.append(i)
                if len(buffer) >= self.chunk_size:
                    filled.set()
                    buffer_lock.release()
                    flushed.wait()
                    flushed.clear()
                else:
                    buffer_lock.release()
            eof.set()
            filled.set()

        filler_thread = threading.Thread(target=filler)
        filler_thread.daemon = True
        filler_thread.start()

        def flusher():

            while not eof.is_set():

                filled.wait(self.max_delay)
                filled.clear()

                to_flush = []

                buffer_lock.acquire()
                if buffer:
                    to_flush = list(buffer)
                    buffer.clear()
                    buffer_lock.release()
                    flushed.set()
                else:
                    buffer_lock.release()

                if to_flush:
                    logging.info('flushing %d records', len(to_flush))
                    for _, response in streaming_bulk(
                            self.es, to_flush,
                            chunk_size=self.chunk_size,
                            max_retries=self.max_retries,
                            raise_on_error=False,
                            raise_on_exception=False,
                            yield_ok=False,
                    ):
                        logging.error(response)

        flusher_thread = threading.Thread(target=flusher)
        flusher_thread.daemon = True
        flusher_thread.start()

        filler_thread.join()
        flusher_thread.join()

    def stdout(self, file):
        s = JSONSerializer()
        for i in self.gen(file):
            print(s.dumps(i))
