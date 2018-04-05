#!/bin/bash

docker run --privileged -i -v `pwd`:/usr/src -v /tmp:/tmp edadeal/fpm:xenial \
   fpm -s python -t deb \
       --python-package-name-prefix python3 \
       --prefix /usr \
       --python-install-lib lib/python3/dist-packages \
       --python-bin python3 \
       --deb-systemd nginx2es.service \
       --deb-upstream-changelog ChangeLog \
       .

package_cloud push ei-grad/nginx2es/ubuntu/xenial python3-nginx2es*.deb

rm -f python3-nginx2es*.deb
