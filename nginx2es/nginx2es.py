import logging

from elasticsearch import JSONSerializer
from elasticsearch.helpers import streaming_bulk


class Nginx2ES(object):

    def __init__(self, es, parser, index):
        self.es = es
        self.parser = parser
        self.index = index

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
        for success, response in streaming_bulk(self.es, self.gen(file)):
            if not success:
                logging.error(response)

    def stdout(self, file):
        s = JSONSerializer()
        for i in self.gen(file):
            print(s.dumps(i))
