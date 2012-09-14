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

""" Tests swift_lfs.fs """

import os
import errno
import unittest
from hashlib import md5
import cPickle as pickle
from gzip import GzipFile
from shutil import rmtree
from tempfile import mkdtemp

from swift.common import utils
from swift.common.utils import mkdirs
from swift.common.ring import Ring, RingData

from swift_lfs.fs import SWIFT_DEVICE_ONLINE, SWIFT_DEVICE_MISCONFIGURED,\
    SWIFT_DEVICE_DEGRADED, SWIFT_DEVICE_FAULTED
from swift_lfs import fs as lfs


class FakeLogger(object):
    pass


class TestLFS(unittest.TestCase):
    """ Tests swift_lfs.fs.LFS """

    def setUp(self):
        """ Set up for testing swift_lfs.fs.LFS """
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_common_lfs')
        self.conf = {'fs': 'xfs', 'swift_dir': self.testdir,
                     'devices': self.testdir}
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        pickle.dump(RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
            [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
              'port': '6010', 'mirror_copies': 1},
                    {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                     'port': '6020', 'mirror_copies': 1}], 30),
            GzipFile(os.path.join(self.testdir, 'test_ring.ring.gz'), 'wb'))
        self.ring = Ring(os.path.join(self.testdir, 'test_ring.ring.gz'))
        self.storage = lfs.get_lfs(self.conf, self.ring, 'test_lfs',
            FakeLogger())

    def tearDown(self):
        """ Tear down for testing swift_lfs.fs.get_lfs """
        try:
            rmtree(self.testdir)
        except OSError, err:
            if err.errno != errno.ENOENT:
                raise

    def test_setup_partition(self):
        """ Tests swift_lfs.fs.LFS.setup_partition """
        self.storage.setup_partition('d', 'p')
        partition_path = os.path.join(self.storage.root, 'd', 'test_lfs', 'p')
        self.assertTrue(os.path.exists(partition_path))
        self.assertTrue(os.path.isdir(partition_path))

    def test_setup_datadir(self):
        """ Tests swift_lfs.fs.LFS.setup_objdir """
        datadir = os.path.join(self.storage.root, 'd', 'test_lfs')
        self.assertEqual(self.storage.setup_datadir('d'), datadir)

    def test_tmp_dir(self):
        """ Tests swift_lfs.fs.LFS.tmp_dir """
        name_hash = md5('n').hexdigest()
        self.assertEqual(self.storage.tmp_dir('d', 'p', name_hash),
            os.path.join(self.storage.root, 'd', self.storage.datadir, 'tmp'))

    def test_get_device_status(self):
        """ Tests swift_lfs.fs.LFS.get_device_status """
        self.assertRaises(Exception,
            lambda: self.storage.get_device_status('1'))

        self.assertEqual(self.storage.get_device_status(),
                {'sdb1': (SWIFT_DEVICE_ONLINE, 1),
                 'sda1': (SWIFT_DEVICE_ONLINE, 1)})

        self.storage.faulted_devices = ['sdb1']
        self.assertEqual(self.storage.get_device_status(),
                {'sdb1': (SWIFT_DEVICE_FAULTED, 1),
                 'sda1': (SWIFT_DEVICE_ONLINE, 1)})

        self.storage.faulted_devices = []
        self.storage.degraded_devices = ['sda1']
        self.assertEqual(self.storage.get_device_status(),
                {'sdb1': (SWIFT_DEVICE_ONLINE, 1),
                 'sda1': (SWIFT_DEVICE_DEGRADED, 1)})

        self.storage.degraded_devices = []
        self.storage.misconfigured_devices = ['sdb1']
        self.assertEqual(self.storage.get_device_status(),
                {'sdb1': (SWIFT_DEVICE_MISCONFIGURED, 1),
                 'sda1': (SWIFT_DEVICE_ONLINE, 1)})


if __name__ == '__main__':
    unittest.main()
