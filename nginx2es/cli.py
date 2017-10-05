#!/usr/bin/env python

from subprocess import Popen, PIPE
import codecs
import json
import logging
import signal
import socket
import sys

from elasticsearch import Elasticsearch

import click

from .parser import AccessLogParser
from .nginx2es import Nginx2ES


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


def watch_tail(filename):
    p = Popen(['tail', '-F', '-n', '+0', filename], stdout=PIPE)
    signal.signal(signal.SIGINT, lambda *_: p.kill())
    signal.signal(signal.SIGTERM, lambda *_: p.kill())
    return codecs.getreader('utf-8')(p.stdout)


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
    '--test', is_flag=True, help="Output to stdout instead of elasticsearch.")
@click.option(
    '--geoip',
    default="/usr/share/GeoIP/GeoIPCity.dat",
    help="GeoIP database file path.")
@click.option('--log-level', default="INFO", help="log level")
def main(filename, one_shot, hostname, index, elastic, force_create_template,
         template, template_name, test, geoip, log_level):

    logging.basicConfig(level=log_level)

    es = Elasticsearch(elastic)

    geoip = load_geoip(geoip)

    nginx2es = Nginx2ES(es, AccessLogParser(hostname, geoip=geoip), index)

    if test:
        run = nginx2es.test
    else:
        check_template(es, template_name, template, force_create_template)
        run = nginx2es.run

    if one_shot:
        run(click.open_file(filename))
    else:
        run(watch_tail(filename))


if __name__ == "__main__":
    main()
