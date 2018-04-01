#!/usr/bin/env python

import io
import json
import logging
import socket
import sys

from elasticsearch import Elasticsearch

import entrypoints
import click

from .parser import AccessLogParser
from .nginx2es import Nginx2ES
from .watcher import Watcher


DEFAULT_TEMPLATE = {
    "template": "nginx-*",
    "settings": {
        "index.refresh_interval": "10s",
        "index.unassigned.node_left.delayed_timeout": "5m",
    },
    "mappings": {
        "_default_": {
            "_all": {"enabled": False},
            "date_detection": False,
            "dynamic_templates": [
                {
                    "string_fields": {
                        "match": "*",
                        "match_mapping_type": "string",
                        "mapping": {"type": "keyword", "norms": False}
                    }
                },
                {
                    "long_fields": {
                        "match": "*",
                        "match_mapping_type": "long",
                        "mapping": {"type": "long", "norms": False}
                    }
                }
            ],
            "properties": {
                "@timestamp": {"type": "date", "format": "dateOptionalTime"},
                "remote_addr": {"type": "ip"},
                "geoip": {"type": "geo_point"},
                "query_geo": {"type": "geo_point"},
                "request": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword", "norms": False}
                    }
                },
                "request_path": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword", "norms": False}
                    }
                },
                "request_qs": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword", "norms": False}
                    }
                }
            }
        }
    }
}


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


def load_extensions(extensions):

    ret = []

    for ext_name in extensions:
        try:
            ext = entrypoints.get_single(
                "nginx2es.ext", ext_name)
        except entrypoints.NoSuchEntryPoint:
            raise click.BadParameter(
                "%s not found in \"nginx2es.ext\" "
                "entrypoints" % ext_name
            )
        ret.append(ext.load())

    return ret


@click.command()
@click.argument('filename', default='/var/log/nginx/access.log')
@click.option(
    '--chunk-size',
    default=500,
    help='chunk size for bulk requests')
@click.option(
    '--elastic', default=['localhost:9200'],
    multiple=True,
    help="elasticsearch cluster address")
@click.option(
    '--force-create-template',
    is_flag=True,
    help="force create index template")
@click.option(
    '--geoip',
    default="/usr/share/GeoIP/GeoIPCity.dat",
    help="GeoIP database file path")
@click.option(
    '--hostname',
    default=socket.gethostname(),
    help="override hostname to add to documents")
@click.option(
    '--index',
    default='nginx-%Y.%m.%d',
    help="index name strftime pattern")
@click.option(
    '--max-delay', default=10.,
    help="maximum time to wait before flush if count of records in buffer is "
         "less than chunk-size")
@click.option(
    '--max-retries', default=3,
    help="maximum number of times a document will be retried when 429 is "
         "received, set to 0 for no retries on 429")
@click.option(
    '--mode',
    default='tail',
    type=click.Choice(['tail', 'from-start', 'one-shot']),
    help="records read mode")
@click.option(
    '--ext', multiple=True,
    help="add post-processing extension")
@click.option('--template', help="index template filename")
@click.option(
    '--template-name',
    default='nginx',
    help="template name to use for index template")
@click.option(
    '--carbon',
    help="carbon host:port to send http stats")
@click.option(
    '--carbon-interval',
    default=10,
    help="carbon host:port to send http stats")
@click.option(
    '--carbon-delay',
    type=int,
    help="stats delay (defaults to interval)")
@click.option(
    '--carbon-prefix',
    help="carbon metrics prefix (default: 'nginx2es.$hostname'")
@click.option(
    '--timeout',
    default=30,
    help="elasticsearch request timeout")
@click.option(
    '--sentry',
    help="sentry dsn")
@click.option(
    '--stdout', is_flag=True, help="output to stdout instead of elasticsearch")
@click.option('--log-level', default="INFO", help="log level")
def main(
        filename,
        chunk_size,
        elastic,
        force_create_template,
        geoip,
        hostname,
        index,
        max_delay,
        max_retries,
        mode,
        ext,
        template,
        template_name,
        carbon,
        carbon_interval,
        carbon_delay,
        carbon_prefix,
        timeout,
        sentry,
        stdout,
        log_level,
):

    logging.basicConfig(level=log_level.upper(), format='%(asctime)s %(levelname)s %(message)s')

    if sentry:
        import raven
        import raven.conf
        import raven.handlers.logging
        sentry = raven.Client(sentry)
        sentry_handler = raven.handlers.logging.SentryHandler(sentry)
        sentry_handler.setLevel(logging.ERROR)
        raven.conf.setup_logging(sentry_handler)

    logging.debug('elasticsearch: %s', elastic)

    es = Elasticsearch(elastic, timeout=timeout)

    geoip = load_geoip(geoip)

    access_log_parser = AccessLogParser(
        hostname, geoip=geoip, extensions=load_extensions(ext),
    )

    if carbon:
        from nginx2es.stat import Stat
        if carbon_prefix is None:
            carbon_prefix = 'nginx2es.%s' % hostname
        stat_kwargs = {
            'prefix': carbon_prefix,
            'interval': carbon_interval,
        }
        if ':' in carbon:
            carbon, carbon_port = carbon.split(':')
            stat_kwargs['port'] = int(carbon_port)
        stat_kwargs['host'] = carbon
        if carbon_delay:
            stat_kwargs['delay'] = carbon_delay
        stat = Stat(**stat_kwargs)
        if stdout:
            stat.output = sys.stdout
        else:
            stat.connect()
        stat.start()
    else:
        stat = None

    nginx2es = Nginx2ES(es, access_log_parser, index,
                        stat=stat,
                        chunk_size=chunk_size,
                        max_retries=max_retries,
                        max_delay=max_delay)

    if stdout:
        run = nginx2es.stdout
    else:
        check_template(es, template_name, template, force_create_template)
        run = nginx2es.run

    if filename == '-':
        f = io.TextIOWrapper(sys.stdin.buffer, errors='replace')
    else:
        f = open(filename, errors='replace')

    if not f.seekable():
        if '--mode' in sys.argv:
            logging.warning("using --mode argument while reading from stream is incorrect")
        run(f)
    elif mode == 'one-shot':
        run(f)
    else:
        f.close()
        from_start = (mode == 'from-start')
        run(Watcher(filename, from_start))


if __name__ == "__main__":
    main()
