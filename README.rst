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

Install
-------

Install with pip:

.. code-block:: bash

    pip install nginx2es

Install with apt:

.. code-block:: bash

    apt-get update
    apt-get install gnupg2 apt-transport-https ca-certificates -y
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 379CE192D401AB61
    echo "deb https://dl.bintray.com/asten/nginx2es xenial main" | tee -a /etc/apt/sources.list.d/nginx2es.list
    apt-get update
    apt-get install nginx2es -y

Features
--------

- Stable log record ID (hostname + file inode number + timestamp + file
  position). It makes possible to import log file more than once (adding some
  additional processing to ``nginx2es``, or dropping a daily index containing
  only a half of records, etc) without creating a duplicate records.

- Parse query params and split request uri path components to separate fields
  for complex log filtering / aggregations.

- Optional use of the GeoIP database (requires the ``geoip`` module and the
  ``GeoIPCity.dat`` database file) - adds ``city`` and ``region_name`` fields.

- Correctly parse log records containing information about multiple upstream
  responses.

- The ``tail -F``-like mode implemented with inotify.
