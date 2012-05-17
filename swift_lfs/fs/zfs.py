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

from swift.common.utils import mkdirs, storage_directory, TRUE_VALUES

from swift_lfs.fs import LFS, LFSStatus
from swift_lfs.exceptions import LFSException

try:
    import nspyzfs
except ImportError:
    raise LFSException(_("Can't import required module nspyzfs"))


def zfs_create(pool, fs_name, mount_point):
    """
    Creates the ZFS filesystem with the given path.

    :param pool: ZFS pool name
    :param fs_name: ZFS file system name
    :param mount_point: file system mount point
    """
    kwargs = {}
    if mount_point:
        kwargs['mountpoint'] = mount_point
    nspyzfs.create_filesystems('%s/%s' % (pool, fs_name), **kwargs)


class LFSZFS(LFS):

    fs = 'zfs'

    def __init__(self, conf, ring, srvdir, logger):
        super(LFSZFS, self).__init__(conf, ring, srvdir, logger)
        self.topfs = conf.get('topfs')
        self.check_interval = int(conf.get('check_interval', '30'))
        mkdirs(self.root)

        # Create the Top level ZFS.
        # self.devices are list of tuple (zpool, mirror_copies)
        for pool, mr_count in self.devices:
            zfs_create(pool, self.topfs, '%s/%s' % (self.root, pool))

        if not self.topfs:
            raise LFSException(_("Cannot locate ZFS filesystem for the " \
                                 "Server. Exiting.."))

        self.fs_per_part = False
        self.fs_per_obj = False
        if self.conf.get('fs_per_obj', 'false') in TRUE_VALUES:
            self.fs_per_part = True
            self.fs_per_obj = True
        elif self.conf.get('fs_per_part', 'false') in TRUE_VALUES:
            self.fs_per_part = True

        self.status_checker = LFSStatus(self.check_interval,
            self.check_pools, ())
        self.status_checker.start()

    def check_pools(self, args):
        need_cb = False

        for pool, mr_count in self.devices:
            pool_config = nspyzfs.zpool_status(pool)[0]

            if pool_config.get_mirrorcount() > mr_count:
                self.misconfigured_devices.append(pool)
            else:
                if pool in self.misconfigured_devices:
                    self.misconfigured_devices.remove(pool)

            ret = pool_config.get_state()
            if ret == nspyzfs.ZPOOL_STATE_DEGRADED:
                if not pool in self.degraded_devices:
                    self.degraded_devices.append(pool)
                    need_cb = True
            elif ret == nspyzfs.ZPOOL_STATE_FAULTED:
                if not pool in self.faulted_devices:
                    self.faulted_devices.append(pool)
                    need_cb = True
            elif ret == nspyzfs.ZPOOL_STATE_UNKNOWN:
                need_cb = True
            else:
                if pool in self.faulted_devices:
                    self.faulted_devices.remove(pool)
                elif pool in self.degraded_devices:
                    self.degraded_devices.remove(pool)

        if need_cb:
            return (self.zfs_error_callback, ())

        return None

    def zfs_error_callback(self, args):
        self.logger.warning(_("DEGARDED pools: %s") %
                            ', '.join(self.degraded_devices))
        self.logger.warning("FAULTED pools: %s" %
                            ', '.join(self.faulted_devices))
        self.status_checker.clear_fault()

    def tmp_dir(self, pool, partition, name_hash):
        if self.fs_per_obj:
            return os.path.join(self.root, pool,
                storage_directory(self.srvdir, partition, name_hash), 'tmp')
        elif self.fs_per_part:
            return os.path.join(self.root, pool, self.srvdir, partition, 'tmp')
        return os.path.join(self.root, pool, self.srvdir, 'tmp')

    def setup_partition(self, pool, partition):
        path = os.path.join(self.root, pool, self.srvdir, partition)
        if not os.path.exists(path):
            if self.fs_per_part:
                fs = '%s/%s/%s' % (self.topfs, self.srvdir, partition)
                zfs_create(pool, fs, path)
            else:
                mkdirs(path)
        return path

    def setup_objdir(self, pool, partition, name_hash):
        path = os.path.join(self.root, pool,
                storage_directory(self.srvdir, partition, name_hash))
        if not os.path.exists(path) and self.fs_per_obj:
            fs = '%s/%s/%s/%s' % (self.topfs, self.srvdir, partition,
                                  name_hash)
            zfs_create(pool, fs, path)
