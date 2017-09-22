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
import logging

import re

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


class AccessLogParser(object):
    def __init__(self, hostname, parse_remainder=None, geoip=None):
        self.hostname = hostname
        self.parse_remainder = parse_remainder
        self.ts_format = '%d/%b/%Y:%H:%M:%S %z'
        self.geoip = geoip

    def __call__(self, line):

        m = main_ext.match(line.strip())

        if m is None:
            logging.warning("[no match] %s", line)
            return None

        d = m.groupdict()

        d['@timestamp'] = datetime.strptime(d['time_local'], self.ts_format)
        if self.hostname is not None:
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

        return d
