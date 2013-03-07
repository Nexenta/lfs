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
import sys

from swift_lfs.fs import LFS, LFSStatus
from swift_lfs.exceptions import LFSException

try:
    from nspyzfs import NSPyZFSError, dataset, pool
except ImportError:
    raise LFSException(_("Can't import required module nspyzfs"))


class LFSZFS(LFS):

    fs = 'zfs'

    def __init__(self, conf, ring, srvdir, default_port, logger):
        super(LFSZFS, self).__init__(conf, ring, srvdir, default_port, logger)
        self.status_check_interval = int(conf.get('status_check_interval', 30))
        self.compression = conf.get('compression', 'off')

        self.mountpoint = os.path.join(self.devices, self.device)
        self.filesystem = self.device

        self.status_checker = LFSStatus(
            self.status_check_interval, self.logger, self.check_pools)

    def setup_node(self):
        """
        Creates filesystem for service and runs device status checker thread.
        """
        if not dataset.exists_fs(self.filesystem):
            dataset.create_fs(self.filesystem, True,
                              mountpoint=self.mountpoint, canmount='on',
                              compression=self.compression)
        if dataset.get(self.filesystem, 'mountpoint') != self.mountpoint:
            dataset.set(self.filesystem, 'mountpoint', self.mountpoint)
        if dataset.get(self.filesystem, 'mounted') != 'yes':
            sys.exit("ERROR: Cannot mount %s" % self.filesystem)
        if dataset.get(self.filesystem, 'compression') != self.compression:
            dataset.set(self.filesystem, 'compression', self.compression)
        self.status_checker.start()

    def check_pools(self):
        try:
            status = pool.status(self.device)
        except NSPyZFSError, e:
            self.logger.exception(_("Can't get status for zfs pool %s"), e)
            return None
        need_cb = False
        self.remove_device_from_devices(self.device)
        health = status['health']
        if health == 'DEGRADED':
            self.degraded_devices.add(self.device)
            need_cb = True
        elif health in ('FAULTED', 'SPLIT'):
            self.faulted_devices.add(self.device)
            need_cb = True
        elif health == 'UNAVAIL':
            self.unavailable_devices.add(self.device)
            need_cb = True
        elif health == 'UNKNOWN':
            need_cb = True
        if need_cb:
            return self.error_callback, tuple()
        return None

    def error_callback(self):
        if self.degraded_devices:
            self.logger.warning(
                _("DEGARDED pools: %s") % ', '.join(self.degraded_devices))
        if self.faulted_devices:
            self.logger.warning(
                _("FAULTED pools: %s") % ', '.join(self.faulted_devices))
        if self.unavailable_devices:
            self.logger.warning(
                _("UNAVAILABLE pools: %s") %
                ', '.join(self.unavailable_devices))
        self.status_checker.clear_fault()

    def clear_fault(self):
        pass
