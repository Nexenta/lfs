# Copyright (c) 2011-2012 Nexenta Systems Inc.
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

import os
import time
from threading import Thread

from swift.common.utils import readconf, mkdirs, whataremyips
from swift.common.exceptions import SwiftConfigurationError

from swift_lfs.exceptions import LFSException


def get_lfs(conf, ring, datadir, default_port, logger):
    """
    Returns LFS for current node

    :param conf: server configuration
    :param ring: server ring file
    :param datadir: server data directory
    :param default_port: default server port
    :param logger: server logger
    :returns : LFS storage class
    :raises SwiftConfigurationError: if fs is invalid
    """
    fs = conf.get('fs', 'xfs')
    try:
        module_name = 'swift_lfs.fs.%s' % fs
        cls_name = 'LFS%s' % fs.upper()
        module = __import__(module_name, fromlist=[cls_name])
        cls = getattr(module, cls_name)
        if '__file__' in conf and fs in conf:
            fs_conf = readconf(conf['__file__'], fs)
            conf = dict(conf, **fs_conf)
        return cls(conf, ring, datadir, default_port, logger)
    except ImportError, e:
        raise SwiftConfigurationError(
            _('Cannot load LFS. Invalid FS: %s. %s') % (fs, e))


class LFS(object):
    """Base class for all FS"""

    def __init__(self, conf, ring, datadir, default_port, logger):
        self.logger = logger
        self.datadir = datadir
        self.conf = conf
        self.root = conf.get('devices', '/srv/node/')
        port = int(conf.get('bind_port', default_port))
        my_ips = whataremyips()
        # devices is a list of tuple (<device name>, <device mirror_copies>)
        self.devices = []
        for dev in ring.devs:
            if dev['ip'] in my_ips and dev['port'] == port:
                mirror_copies = int(dev.get('mirror_copies', 1))
                self.devices.append((dev['device'], mirror_copies))
        self.faulted_devices = set()
        self.degraded_devices = set()
        self.unavailable_devices = set()

    def setup_node(self):
        pass

    def setup_datadir(self, device):
        """
        Setup datadir, devises/device/datadir

        :param device: device
        :returns : path to datadir
        """
        path = os.path.join(self.root, device, self.datadir)
        mkdirs(path)
        return path

    def setup_tmp(self, device):
        """
        Setup tmp, devises/device/tmp

        :param device: device
        :returns : path to tmp
        """
        path = os.path.join(self.root, device, 'tmp')
        mkdirs(path)
        return path

    def setup_partition(self, device, partition):
        """
        Creates partition directory, devises/device/datadir/partition

        :param device: device
        :param partition: partition
        :returns : path to partition directory
        """
        path = os.path.join(self.root, device, self.datadir, partition)
        mkdirs(path)
        return path

    def remove_device_from_devices(self, device):
        for devices in (self.degraded_devices, self.faulted_devices,
                        self.unavailable_devices):
            if device in devices:
                devices.remove(device)

    def get_device_status(self, devices=None):
        """
        Return statuses of devices

        :param devices: list of devices
        :returns : dict ({ <device name> : (<device status>, <mirror_count>})
                   with device statuses or None if there is not any device
        """
        if devices and not isinstance(devices, list):
            raise LFSException("Devices should be a list")
        dev_statuses = {}
        if not devices:
            devices = [self.pool]
        for pool in devices:
            if pool in self.faulted_devices:
                status = 'faulted'
            elif pool in self.degraded_devices:
                status = 'degraded'
            elif pool in self.unavailable_devices:
                status = 'unavailable'
            else:
                status = 'online'
            dev_statuses[pool] = status
        if not dev_statuses:
            dev_statuses = None
        return dev_statuses


class LFSStatus(Thread):
    """
    Status Checker thread which checks the status of filesystem and calls back
    to LFS if it sees any issues.
    This thread periodically calls the check_func, if the filesystem is
    healthy. If check_func returns non-zero, it temporary stops checking
    until the fault is cleared.

    :param interval: interval in seconds for checking FS
    :param logger: logger object
    :param func: method for checking FS. Takes exactly one argument which
                 should be a tuple. Returns 0 if FS is healthy
    :param args: tuple arguments to check_func
    """

    def __init__(self, interval, logger, func, args):
        super(LFSStatus, self).__init__()
        self.interval = interval
        self.func = func
        self.args = args
        self.logger = logger
        self.daemon = True

    def run(self):
        while True:
            try:
                ret = self.func(*self.args)
                if ret is not None:
                    # ret must be a tuple (<callback function>, <args>)
                    ret[0](*ret[1])
            except Exception, e:
                self.logger.exception(e)
            time.sleep(self.interval)
