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

from swift.common.utils import TRUE_VALUES

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
        self.pool = conf['pool']
        self.top_fs = conf.get('top_fs')
        if not self.top_fs:
            sys.exit("ERROR: top_fs not defined")
        if not dataset.exists_fs(self.top_fs):
            sys.exit("ERROR: top_fs %s not exists" % self.top_fs)
        self.device_fs_compression = conf.get('device_fs_compression', 'off')
        self.fs_for_datadir = conf.get('fs_for_datadir', 'no') in TRUE_VALUES
        self.datadir_compression = conf.get('datadir_compression', 'off')
        self.fs_for_tmp = conf.get('fs_for_tmp', 'no') in TRUE_VALUES
        self.tmp_compression = conf.get('tmp_compression', 'off')
        self.fs_per_partition = conf.get('fs_per_partition', 'no') in \
                                TRUE_VALUES
        self.partition_compression = conf.get('partittion_compression', 'off')
        self.status_checker = LFSStatus(
            self.status_check_interval, self.logger, self.check_pools,
            (self.pool, ))

    def _setup_fs(self, fs_name, mountpoint, compression):
        if not dataset.exists_fs(fs_name):
            dataset.create_fs(fs_name, True, mountpoint=mountpoint,
                              canmount='on', compression=compression)

        if dataset.get(fs_name, 'mountpoint') != mountpoint:
            dataset.set(fs_name, 'mountpoint', mountpoint)

        if dataset.get(fs_name, 'mounted') != 'yes':
            # TODO: try to mount
            sys.exit("ERROR: Cannot mount %s" % fs_name)

        if dataset.get(fs_name, 'compression') != compression:
            dataset.set(fs_name, 'compression', compression)

    def device_fs_name(self, device):
        """ Returns fs name for device """
        return self.top_fs.rstrip('/') + '/' + device

    def setup_node(self):
        """ Creates filesystem for each device from node ring """
        for device, _junk in self.devices:
            # Setup fs for device
            device_fs = self.device_fs_name(device)
            mountpoint = os.path.join(self.root, device)
            self._setup_fs(device_fs, mountpoint, self.device_fs_compression)
            # Setup fs for datadir
            if self.fs_for_datadir:
                datadir_fs = self.device_fs_name(device) + '/' + self.datadir
                mountpoint = os.path.join(self.root, device, self.datadir)
                self._setup_fs(datadir_fs, mountpoint,
                               self.datadir_compression)
            # Setup fs for tmp
            if self.fs_for_tmp:
                tmp_fs = self.device_fs_name(device) + '/tmp'
                mountpoint = os.path.join(self.root, device, 'tmp')
                self._setup_fs(tmp_fs, mountpoint, self.tmp_compression)
        self.status_checker.start()

    def setup_partition(self, device, partition):
        """
        Setup partition directory, devices/device/datadir/partition, if
        fs_per_partition enabled create and setup partition file system

        :param device: device
        :param partition: partition
        :returns : path to partition directory
        """
        if not self.fs_per_partition:
            return super(LFSZFS, self).setup_partition(device, partition)
        if not self.fs_for_datadir:
            partition_fs = '%s/%s' % (self.device_fs_name(device), partition)
        else:
            partition_fs = '%s/%s/%s' % (self.device_fs_name(device),
                                         self.datadir, partition)
        path = os.path.join(self.root, device, self.datadir, partition)
        if not dataset.exists_fs(partition_fs):
            self._setup_fs(partition_fs, path, self.partition_compression)
        return path

    def check_pools(self, pool_name):
        try:
            status = pool.status(pool_name)
        except NSPyZFSError, e:
            self.logger.exception(_("Can't get status for zfs pool %s"), e)
            return None
        need_cb = False
        self.remove_device_from_devices(pool_name)
        health = status['health']
        if health == 'DEGRADED':
            self.degraded_devices.add(pool_name)
            need_cb = True
        elif health in ('FAULTED', 'SPLIT'):
            self.faulted_devices.add(pool_name)
            need_cb = True
        elif health == 'UNAVAIL':
            self.unavailable_devices.add(pool_name)
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
            self.logger.warning(_("UNAVAILABLE pools: %s") %
                                ', '.join(self.unavailable_devices))
        self.status_checker.clear_fault()

    def clear_fault(self):
        pass
