LFS Middleware
=======================

This wsgi middleware is to be used with [Openstack Swift](http://github.com/openstack/swift).

How to Build to Debian Packages
===============================

    python setup.py --command-packages=stdeb.command bdist_deb
