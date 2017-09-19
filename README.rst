Put parsed Nginx access.log to Elasticsearch
============================================

Nginx access.log have to be formatted with this format:

.. code-block:: nginx

    log_format main_ext
        '$remote_addr $http_host $remote_user [$time_local] "$request" '
        '$status $body_bytes_sent "$http_referer" '
        '"$http_user_agent" "$http_x_forwarded_for" '
        'rt=$request_time ua="$upstream_addr" '
        'us="$upstream_status" ut="$upstream_response_time" '
        'ul="$upstream_response_length" '
        'cs=$upstream_cache_status';

GeoIP
-----

Nginx2es could optionally use the GeoIP database if `geoip` module is installed
and the GeoIPCity.dat database file is available.
