#!/usr/bin/env python
"""
Parse nginx access.log and put parsed lines to Elasticsearch.

Nginx access.log have to be formatted with this format:

log_format main_ext
    '$remote_addr $http_host $remote_user [$time_local] "$request" '
    '$status $body_bytes_sent "$http_referer" '
    '"$http_user_agent" "$http_x_forwarded_for" '
    'rt=$request_time ua="$upstream_addr" '
    'us="$upstream_status" ut="$upstream_response_time" '
    'ul="$upstream_response_length" '
    'cs=$upstream_cache_status';
"""

from datetime import datetime
from urllib.parse import splitquery, parse_qs
import json
import logging
import re
import signal
import socket
from subprocess import Popen, PIPE
import sys

from elasticsearch import Elasticsearch, JSONSerializer
from elasticsearch.helpers import streaming_bulk

import click

main_ext = re.compile(
    '(?P<remote_addr>[^ ]+) (?P<http_host>[^ ]+) (?P<remote_user>[^ ]+) '
    '\[(?P<time_local>[^\]]+)\] '
    '"(?P<verb>[A-Z]+) (?P<request>[^ ]+) HTTP/(?P<http_version>[0-9\.]+)" '
    '(?P<status>[0-9]+) '
    '(?P<body_bytes_sent>[0-9-]+) '
    '"(?P<http_referer>[^"]+)" '
    '"(?P<http_user_agent>[^"]+)" '
    '"(?P<http_x_forwarded_for>[^"]+)" '
    'rt=(?P<request_time>[0-9\.-]+) '
    'ua="(?P<upstream_addr>[^"]+)" '
    'us="(?P<upstream_status>[^"]+)" '
    'ut="(?P<upstream_response_time>[^"]+)" '
    'ul="(?P<upstream_response_length>[^"]+)" '
    'cs=(?P<upstream_cache_status>[A-Z-]+)'
    '(?P<remainder>.*)')

DEFAULT_TEMPLATE = {
    "template": "nginx-*",
    "settings": {
        "index.refresh_interval": "10s"
    },
    "mappings": {
        "_default_": {
            "_all": {
                "enabled": False
            },
            "date_detection": False,
            "dynamic_templates": [{
                "string_fields": {
                    "match": "*",
                    "match_mapping_type": "string",
                    "mapping": {
                        "type": "keyword",
                        "norms": False
                    }
                }
            }, {
                "long_fields": {
                    "match": "*",
                    "match_mapping_type": "long",
                    "mapping": {
                        "type": "long",
                        "norms": False
                    }
                }
            }],
            "properties": {
                "@timestamp": {
                    "type": "date",
                    "format": "dateOptionalTime"
                },
                "remote_addr": {
                    "type": "ip"
                },
                "geoip": {
                    "type": "geo_point"
                },
                "query_geo": {
                    "type": "geo_point"
                },
                "request": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "norms": False
                        }
                    }
                },
                "request_path": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "norms": False
                        }
                    }
                },
                "request_qs": {
                    "type": "text",
                    "fields": {
                        "raw": {
                            "type": "keyword",
                            "norms": False
                        }
                    }
                }
            }
        }
    }
}


class Nginx2ES(object):
    def __init__(self, hostname, es, index, parse_remainder=None, geoip=None):
        self.hostname = hostname
        self.es = es
        self.index = index
        self.parse_remainder = parse_remainder
        self.ts_format = '%d/%b/%Y:%H:%M:%S %z'
        self.geoip = geoip

    def parse_line(self, line):

        m = main_ext.match(line.strip())

        if m is None:
            logging.warning("[no match] %s", line)
            return None

        d = m.groupdict()

        d['@timestamp'] = datetime.strptime(d['time_local'], self.ts_format)
        d['@host'] = self.hostname

        d['request_path'], d['request_qs'] = splitquery(d['request'])

        if d['request_qs'] is None:
            del d['request_qs']
        else:
            d['query'] = parse_qs(d['request_qs'])
            lon_alias = 'lng' if 'lng' in d['query'] else 'lon'
            if 'lat' in d['query'] and lon_alias in d['query']:
                d['query_geo'] = {
                    'lat': float(d['query']['lat'][0]),
                    'lon': float(d['query'][lon_alias][0]),
                }

        for i in [
                'http_x_forwarded_for', 'upstream_addr', 'upstream_status',
                'upstream_response_time', 'upstream_response_length'
        ]:
            if d[i] == '-':
                del d[i]
            else:
                d[i] = d[i].replace(', ', ' : ').split(' : ')

        if 'upstream_response_time' in d:
            d['upstream_response_time'] = [
                float(i) for i in d['upstream_response_time'] if i != '-'
            ]

        if 'upstream_response_length' in d:
            d['upstream_response_length'] = [
                int(i) for i in d['upstream_response_length'] if i != '-'
            ]

        d['request_time'] = float(d['request_time'])

        if self.geoip is not None:
            g = self.geoip.record_by_name(d['remote_addr'])
            if g is not None:
                d['geoip'] = {
                    'lat': g['latitude'],
                    'lon': g['longitude'],
                }
                d['city'] = g['city']
                d['region_name'] = g['region_name']

        remainder = d.pop('remainder')
        if self.parse_remainder is not None and remainder:
            self.parse_remainder(d, remainder)

        return {
            '_index': d['@timestamp'].strftime(self.index),
            '_type': 'nginx2es',
            '_source': d
        }

    def gen(self, file):
        for line in file:
            doc = self.parse_line(line)
            if doc is not None:
                yield doc

    def run(self, file):
        for success, response in streaming_bulk(self.es, self.gen(file)):
            if not success:
                logging.error(response)

    def test(self, file):
        s = JSONSerializer()
        for i in self.gen(file):
            print(s.dumps(i))


def watch_tail(filename):
    p = Popen(['tail', '-F', filename], stdout=PIPE, encoding='utf-8')
    signal.signal(signal.SIGINT, lambda *_: p.kill())
    signal.signal(signal.SIGTERM, lambda *_: p.kill())
    return p.stdout


def geoip_error(msg):
    sys.stderr.write("can't load geoip database: %s\n" % msg)
    sys.exit(1)


def load_geoip(geoip):

    explicit = False
    for i in sys.argv:
        if i == '--geoip' or i.startswith('--geoip='):
            explicit = True

    try:
        import GeoIP
        try:
            # Description from https://github.com/maxmind/geoip-api-c:
            #
            # * GEOIP_INDEX_CACHE - Cache only the the most frequently accessed
            # index portion of the database, resulting in faster lookups than
            # GEOIP_STANDARD, but less memory usage than GEOIP_MEMORY_CACHE.
            # This is useful for larger databases such as GeoIP Legacy
            # Organization and GeoIP Legacy City. Note: for GeoIP Legacy
            # Country, Region and Netspeed databases, GEOIP_INDEX_CACHE is
            # equivalent to GEOIP_MEMORY_CACHE.
            #
            # * GEOIP_CHECK_CACHE - Check for updated database. If database has
            # been updated, reload file handle and/or memory cache.
            flags = GeoIP.GEOIP_INDEX_CACHE | GeoIP.GEOIP_CHECK_CACHE
            return GeoIP.open(geoip, flags)
        except GeoIP.error as e:
            # if geoip was specified explicitly then the program should exit
            if explicit:
                geoip_error(e)
    except ImportError:
        if explicit:
            geoip_error("geoip module is not installed")
    return None


def check_template(es, name, template, force):
    if force or not es.indices.exists_template(name):
        if template is None:
            template = DEFAULT_TEMPLATE
        else:
            template = json.load(open(template))
        es.indices.put_template(name, DEFAULT_TEMPLATE)


@click.command()
@click.argument('filename', default='/var/log/nginx/access.log')
@click.option(
    '--one-shot',
    is_flag=True,
    help="Parse current access.log contents, no `tail -f`.")
@click.option(
    '--hostname',
    default=socket.gethostname(),
    help="Override hostname to add to documents.")
@click.option(
    '--index',
    default='nginx-%Y.%m.%d',
    help="Index name template (use strftime(3) format).")
@click.option(
    '--elastic', default=['localhost:9200'], help="Elasticsearch address.")
@click.option(
    '--force-create-template',
    is_flag=True,
    help="Force create index template.")
@click.option(
    '--template-name',
    default='nginx',
    help="Template name to use for index template.")
@click.option('--template', help="Index template filename (json).")
@click.option(
    '--geoip',
    default="/usr/share/GeoIP/GeoIPCity.dat",
    help="GeoIP database file path.")
@click.option(
    '--test', is_flag=True, help="Output to stdout instead of elasticsearch.")
def main(filename, one_shot, hostname, index, elastic, force_create_template,
         template, template_name, test, geoip):

    es = Elasticsearch(elastic)

    geoip = load_geoip(geoip)

    nginx2es = Nginx2ES(hostname, es, index, geoip=geoip)

    if test:
        run = nginx2es.test
    else:
        run = nginx2es.run

    if one_shot:
        run(click.open_file(filename))
    else:
        check_template(es, template_name, template, force_create_template)
        run(watch_tail(filename))


if __name__ == "__main__":
    main()
