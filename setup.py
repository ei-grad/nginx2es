# encoding: utf-8
from __future__ import absolute_import, print_function
from setuptools import setup, find_packages


__version__ = '0.8'
__author__ = 'Andrew Grigorev <andrew@ei-grad.ru>'

setup(
    name='nginx2es',
    version=__version__,
    author=__author__,
    author_email='andrew@ei-grad.ru',
    license="MIT",
    description="Put nginx logs to Elasticsearch and send stats to carbon",
    platforms="all",
    packages=find_packages(),
    install_requires=(
        'arconfig==0.1.3',
        'Click==6.2',
        'elasticsearch==6.2.0',
        'entrypoints==0.2.3',
        'fast-json==0.3.0',
        'inotify-simple==1.1.7',
        'numpy==1.11.0',
        'pandas==0.17.1',
        'python-dateutil==2.4.2',
        'pytz==2014.10',
        'raven==6.6.0',
        'six==1.10.0',
        'ujson==1.35',
        'urllib3==1.22',
    ),
    entry_points={
        'console_scripts': [
            'nginx2es = nginx2es.cli:main',
        ],
    },
    extras_require={
        ':python_version < "4.0"': 'typing >= 3.5.2',
    },
)
