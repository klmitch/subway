#!/usr/bin/env python

import os

from setuptools import setup


def readreq(filename):
    result = []
    with open(filename) as f:
        for req in f:
            req = req.lstrip()
            if req.startswith('-e ') or req.startswith('http:'):
                idx = req.find('#egg=')
                if idx >= 0:
                    req = req[idx + 5:].partition('#')[0].strip()
                else:
                    pass
            else:
                req = req.partition('#')[0].strip()
            if not req:
                continue
            result.append(req)
    return result


def readfile(filename):
    with open(filename) as f:
        return f.read()


setup(
    name='subway',
    version='0.1.0',
    author='Kevin L. Mitchell',
    author_email='kevin.mitchell@rackspace.com',
    url='https://github.com/klmitch/subway',
    description="Subway Rate Limit Distributor",
    long_description=readfile('README.rst'),
    license='Apache License (2.0)',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP',
    ],
    py_modules=['subway'],
    install_requires=readreq('.requires'),
    tests_require=readreq('.test-requires'),
    entry_points={
        'console_scripts': [
        ],
    },
)
