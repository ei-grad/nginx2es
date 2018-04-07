#!/bin/bash

docker run --privileged -i --rm -v `pwd`:/usr/src -v /tmp:/tmp edadeal/fpm:xenial \
   fpm -s python -t deb \
       --python-package-name-prefix python3 \
       --prefix /usr \
       --python-install-lib lib/python3/dist-packages \
       --python-bin python3 \
       --deb-systemd nginx2es.service \
       --deb-upstream-changelog ChangeLog \
       .

docker run --privileged -i --rm -v `pwd`:/usr/src -v /tmp:/tmp edadeal/fpm:xenial bash -c '
set -x
for i in elasticsearch entrypoints fast-json inotify-simple arconfig; do
    fpm -s python -t deb \
       --python-package-name-prefix python3 \
       --prefix /usr \
       --python-install-lib lib/python3/dist-packages \
       --python-bin python3 \
       --python-pip pip3 \
       $i
done
'

package_cloud push --skip-errors ei-grad/nginx2es/ubuntu/xenial *.deb
