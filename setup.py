#!/usr/bin/python
# Copyright 2011-2012 Nexenta Systems Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages

from swift_lfs import __version__ as version


name = 'swift_lfs'


setup(
    name=name,
    version=version,
    description='Swift LFS middleware',
    license='Apache License (2.0)',
    author='Nexenta Systems Inc',
    author_email='victor.rodionov@nexenta.com',
    url='https://github.com/Nexenta/lfs',
    packages=find_packages(exclude=['test_lfs']),
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Environment :: No Input/Output (Daemon)',
        ],
    requires=['swift(>=1.4.7)'],
    entry_points={
        'paste.filter_factory': [
            'swift_lfs=swift_lfs.lfs:filter_factory',
            ],
        },
)
