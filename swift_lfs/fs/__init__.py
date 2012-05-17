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

SWIFT_DEVICE_ONLINE = 1
SWIFT_DEVICE_MISCONFIGURED = 2
SWIFT_DEVICE_DEGRADED = 3
SWIFT_DEVICE_FAULTED = 4


def get_lfs(conf, ring, datadir, logger):
    """
    Returns LFS for current node

    :param conf: server configuration
    :param ring: server ring file
    :param datadir: server data directory
    :param logger: server logger
    :returns : LFS storage class
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
        return cls(conf, ring, datadir, logger)
    except ImportError, e:
        raise SwiftConfigurationError(
            _('Cannot load LFS. Invalid FS: %s. %s') % (fs, e))


class LFS(object):
    """Base class for all FS"""

    def __init__(self, conf, ring, datadir, logger):
        self.logger = logger
        self.conf = conf
        self.root = conf.get('devices', '/srv/node')
        self.datadir = datadir
        my_ips = whataremyips()
        # devices is a list of tuple (<device name>, <device mirror_copies>)
        self.devices = [(dev['device'], dev.get('mirror_copies', 1))
        for dev in ring.devs if dev['ip'] in my_ips]
        self.faulted_devices = []
        self.degraded_devices = []
        self.misconfigured_devices = []

    def setup_partition(self, device, partition):
        path = os.path.join(self.root, device, self.datadir, partition)
        if not os.path.exists(path):
            mkdirs(path)
        return path

    def setup_objdir(self, device, partition, name_hash):
        """Creation of objdir is done by object server by default."""
        pass

    def tmp_dir(self, device, partition, name_hash):
        return os.path.join(self.root, device, self.datadir, 'tmp')

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
            devices = [dev for dev, mr_count in self.devices]
        for dev, mr_count in self.devices:
            status = SWIFT_DEVICE_ONLINE
            if dev in devices:
                if dev in self.faulted_devices:
                    status = SWIFT_DEVICE_FAULTED
                elif dev in self.degraded_devices:
                    status = SWIFT_DEVICE_DEGRADED
                elif dev in self.misconfigured_devices:
                    status = SWIFT_DEVICE_MISCONFIGURED
                dev_statuses[dev] = (status, mr_count)
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
    :param check_func: method for checking FS. Takes exactly one argument
                       which should be a tuple. Returns 0 if FS is healthy
    :param check_args: tuple argument to check_func
    """

    def __init__(self, interval, check_func, check_args):
        Thread.__init__(self)
        self.interval = interval
        self.check_func = check_func
        self.check_args = check_args
        self.faulty = False
        self.daemon = True

    def run(self):
        while True:
            if not self.faulty:
                ret = self.check_func(self.check_args)
                if ret != None:
                    # ret must be a tuple (<callback function>, <args>)
                    self.faulty = True
                    ret[0](ret[1])
            time.sleep(self.interval)

    def clear_fault(self):
        """Clears the fault, so that status check thread can resume."""
        self.faulty = False
