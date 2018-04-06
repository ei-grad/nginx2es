import logging
import threading

from elasticsearch import JSONSerializer
from elasticsearch.helpers import streaming_bulk


class Nginx2ES(object):

    def __init__(self, es, parser, index, stat=None,
                 min_timestamp=None,
                 max_timestamp=None,
                 chunk_size=500,
                 max_retries=3, max_delay=10.):
        self.es = es
        self.parser = parser
        self.index = index
        self.min_timestamp = min_timestamp
        self.max_timestamp = max_timestamp
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.max_delay = max_delay
        self.stat = stat

    def gen(self, file):
        for line_num, line in enumerate(file):
            doc = self.parser(line)
            if doc is not None:
                if self.min_timestamp is not None and self.min_timestamp > doc['@timestamp']:
                    continue
                if self.max_timestamp is not None and self.max_timestamp <= doc['@timestamp']:
                    continue
                if self.stat is not None:
                    self.stat.hit(doc)
                yield {
                    '_id': doc.pop('request_id'),
                    '_index': doc['@timestamp'].strftime(self.index),
                    '_type': 'nginx2es',
                    '_source': doc
                }

    def run(self, file):

        buffer = []

        # fire when buffer is filled by filler
        filled = threading.Event()
        # fire when flusher has pulled the buffer contents
        flusher_pull_complete = threading.Event()
        # fire when faced EOF in one-shot mode
        eof = threading.Event()
        buffer_lock = threading.Lock()

        def filler():
            try:
                for i in self.gen(file):
                    buffer_lock.acquire()
                    buffer.append(i)
                    if len(buffer) >= self.chunk_size:
                        filled.set()
                        buffer_lock.release()
                        flusher_pull_complete.wait()
                        flusher_pull_complete.clear()
                    else:
                        buffer_lock.release()
            except Exception:
                logging.error("exception in filler thread", exc_info=True)
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
                    flusher_pull_complete.set()
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
                        logging.error("index request %s for %s: %s",
                                      response['index']['status'],
                                      response['index']['_id'],
                                      response['index']['error'])

        flusher_thread = threading.Thread(target=flusher)
        flusher_thread.daemon = True
        flusher_thread.start()

        filler_thread.join()
        flusher_thread.join()

        if self.stat is not None:
            self.stat.eof.set()
            self.stat.join()

    def stdout(self, file):
        s = JSONSerializer()
        for i in self.gen(file):
            print(s.dumps(i))

        if self.stat is not None:
            self.stat.eof.set()
            self.stat.join()
