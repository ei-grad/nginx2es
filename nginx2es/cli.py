#!/usr/bin/env python

import argparse
import io
import json
import logging
import socket
import sys

import dateutil

from arconfig import LoadConfigAction, GenConfigAction

from elasticsearch import Elasticsearch, ConnectionError

import entrypoints

from .parser import AccessLogParser
from .nginx2es import Nginx2ES
from .watcher import Watcher
from .mapping import DEFAULT_TEMPLATE
from yarl import URL


def geoip_error(msg):
    sys.stderr.write("can't load geoip database: %s\n" % msg)
    sys.exit(1)


def load_geoip(geoip, explicit):

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
        es.indices.put_template(name, template)


def load_extensions(extensions):

    ret = []

    for ext_name in extensions:
        try:
            ext = entrypoints.get_single(
                "nginx2es.ext", ext_name)
        except entrypoints.NoSuchEntryPoint:
            raise ValueError(
                "%s not found in \"nginx2es.ext\" "
                "entrypoints" % ext_name
            )
        ret.append(ext.load())

    return ret


parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--config",
                    default=argparse.SUPPRESS,
                    action=LoadConfigAction)
parser.add_argument("--gen-config",
                    default=argparse.SUPPRESS,
                    action=GenConfigAction)
parser.add_argument("filename", nargs="?",
                    default=argparse.SUPPRESS,
                    help="file to process (default: /var/log/nginx/access.json)")
parser.add_argument("--chunk-size", type=int, default=500, help="chunk size for bulk requests")
parser.add_argument("--elastic-url", action="append", type=URL,
                    help="Elasticsearch host. Format: https://user:password@host1,host2,host3:9200")
parser.add_argument("--min-timestamp",
                    default=argparse.SUPPRESS,
                    help="skip records with timestamp before the specified")
parser.add_argument("--max-timestamp",
                    default=argparse.SUPPRESS,
                    help="skip records with timestamp after the specified")
parser.add_argument("--geoip", default=argparse.SUPPRESS,
                    help="GeoIP database file path")
parser.add_argument("--hostname", default=socket.gethostname(),
                    help="override hostname to add to documents")
parser.add_argument("--index", default="nginx-%Y.%m.%d",
                    help="index name strftime pattern")
parser.add_argument(
    "--max-delay", default=10., type=int,
    help="maximum time to wait before flush if count of records in buffer is "
         "less than chunk-size")
parser.add_argument(
    "--max-retries", default=3, type=int,
    help="maximum number of times a document will be retried when 429 is "
         "received, set to 0 for no retries on 429")
parser.add_argument("--mode", default="tail",
                    choices=["tail", "from-start", "one-shot"],
                    help="records read mode")
parser.add_argument("--ext", default=[], action="append",
                    help="add post-processing extension")
parser.add_argument("--template",
                    default=argparse.SUPPRESS,
                    help="index template filename")
parser.add_argument("--template-name", default="nginx",
                    help="template name to use for index template")
parser.add_argument("--force-create-template", action="store_true",
                    help="force create index template")
parser.add_argument("--carbon",
                    default=argparse.SUPPRESS,
                    help="carbon host:port to send http stats")
parser.add_argument("--carbon-interval", default=10, type=int,
                    help="carbon host:port to send http stats")
parser.add_argument(
    "--carbon-delay", default=5., type=float,
    help="number of seconds to delay the stat calculation and delivery")
parser.add_argument("--carbon-prefix",
                    default=argparse.SUPPRESS,
                    help="carbon metrics prefix (default: nginx2es.$hostname)")
parser.add_argument("--timeout", type=int, default=30,
                    help="elasticsearch request timeout")
parser.add_argument("--sentry", default=argparse.SUPPRESS, help="sentry dsn")
parser.add_argument("--stdout", action="store_true",
                    help="don't send anything to Elasticsearch or carbon, "
                         "just output to stdout")
parser.add_argument("--log-format",
                    default="%(asctime)s %(levelname)s %(message)s",
                    help="log format")
parser.add_argument("--log-level", default="error", help="log level")


def main():

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), format='%(asctime)s %(levelname)s %(message)s')

    if 'sentry' in args:
        import raven
        import raven.conf
        import raven.handlers.logging
        sentry = raven.Client(args.sentry)
        sentry_handler = raven.handlers.logging.SentryHandler(sentry)
        sentry_handler.setLevel(logging.ERROR)
        raven.conf.setup_logging(sentry_handler)

    es_kwargs = {'timeout': args.timeout}
    if 'elastic_url' in args:
        elastic_hosts = [
            URL.build(
                scheme=args.elastic_url.scheme,
                user=args.elastic_url.user,
                password=args.elastic_url.password,
                host=host,
                port=args.elastic_url.port
            ) for host in args.elastic_url.host.split(",")
        ]

        es_kwargs['hosts'] = elastic_hosts
    es = Elasticsearch(**es_kwargs)

    geoip = load_geoip(
        args.geoip if 'geoip' in args else "/usr/share/GeoIP/GeoIPCity.dat",
        'geoip' in args
    )

    nginx2es_kwargs = {
        "es": es,
        "index": args.index,
        "chunk_size": args.chunk_size,
        "max_retries": args.max_retries,
        "max_delay": args.max_delay,
    }

    nginx2es_kwargs['parser'] = AccessLogParser(
        args.hostname, geoip=geoip, extensions=load_extensions(args.ext),
    )

    if 'carbon' in args:

        from nginx2es.stat import Stat

        stat_kwargs = {
            'interval': args.carbon_interval,
            'delay': args.carbon_delay,
        }
        if 'carbon_prefix' in args:
            stat_kwargs['prefix'] = args.carbon_prefix
        else:
            stat_kwargs['prefix'] = 'nginx2es.%s' % args.hostname
        if ':' in args.carbon:
            args.carbon, carbon_port = args.carbon.split(':')
            stat_kwargs['port'] = int(carbon_port)
        stat_kwargs['host'] = args.carbon

        stat = Stat(**stat_kwargs)

        if args.stdout:
            stat.output = sys.stdout
        else:
            stat.connect()

        stat.start()

        nginx2es_kwargs['stat'] = stat

    else:
        stat = None

    if 'min_timestamp' in args:
        nginx2es_kwargs['min_timestamp'] = dateutil.parser.parse(args.min_timestamp)
    if 'max_timestamp' in args:
        nginx2es_kwargs['max_timestamp'] = dateutil.parser.parse(args.max_timestamp)

    nginx2es = Nginx2ES(**nginx2es_kwargs)

    if args.stdout:
        run = nginx2es.stdout
    else:
        if 'template' in args:
            template = json.load(open(args.template))
        else:
            template = DEFAULT_TEMPLATE
        try:
            check_template(es, args.template_name, template, args.force_create_template)
        except ConnectionError as e:
            logging.error("can't connect to elasticsearch")
            sys.exit(1)
        run = nginx2es.run

    if 'filename' not in args:
        args.filename = '/var/log/nginx/access.json'

    if args.filename == '-':
        f = io.TextIOWrapper(sys.stdin.buffer, errors='replace')
    else:
        f = open(args.filename, errors='replace')

    try:
        if not f.seekable():
            if '--mode' in sys.argv:
                logging.warning("using --mode argument while reading from stream is incorrect")
            run(f)
        elif args.mode == 'one-shot':
            run(f)
        else:
            f.close()
            from_start = (args.mode == 'from-start')
            run(Watcher(args.filename, from_start))
    except (KeyboardInterrupt, BrokenPipeError):
        if stat is not None:
            stat.eof.set()
            stat.join()
        sys.exit(1)
    else:
        if stat is not None:
            stat.eof.set()
            stat.join()


if __name__ == "__main__":
    main()
