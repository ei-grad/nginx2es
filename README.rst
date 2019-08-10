Put parsed Nginx access.log to Elasticsearch
============================================

Nginx access.log have to be formatted with this format:

.. code-block:: nginx

    log_format  main  escape=json '{'
      '"timestamp": "$time_iso8601",'
      '"request_id": "$request_id",'
      '"message": "$remote_addr $http_host $request $status $bytes_sent $request_time",'
      '"bytes_sent": "$bytes_sent",'
      '"connection": "$connection",'
      '"connection_requests": "$connection_requests",'
      '"content_type": "$http_content_type",'
      '"forwarded_for": "$http_x_forwarded_for",'
      '"gzip_ratio": "$gzip_ratio",'
      '"host": "$http_host",'
      '"referrer": "$http_referer",'
      '"remote_addr": "$remote_addr",'
      '"remote_user": "$remote_user",'
      '"request_length": "$request_length",'
      '"request_method": "$request_method",'
      '"request_time": "$request_time",'
      '"request_uri": "$request_uri",'
      '"server_protocol": "$server_protocol",'
      '"status": "$status",'
      '"upstream_addr": "$upstream_addr",'
      '"upstream_cache_status": "$cache_status",'
      '"upstream_connect_time": "$upstream_connect_time",'
      '"upstream_response_length": "$upstream_response_length",'
      '"upstream_response_time": "$upstream_response_time",'
      '"upstream_status": "$upstream_status",'
      '"user_agent": "$http_user_agent",'
      '"request_id": "$request_id",'
    '}';

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
