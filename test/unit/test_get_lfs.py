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
import unittest
import cPickle as pickle
from gzip import GzipFile
from shutil import rmtree
from tempfile import mkdtemp

from swift.common import utils
from swift.common.utils import mkdirs
from swift.common.ring import Ring, RingData
from swift.common.exceptions import SwiftConfigurationError

from swift_lfs import fs as lfs
from swift_lfs.exceptions import LFSException


class FakeLogger(object):
    pass


class TestGetLFS(unittest.TestCase):
    """ Test swift_lfs.fs.get_lfs """

    def setUp(self):
        """ Set up for testing swift_lfs.fs.get_lfs """
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_common_lfs_get_lfs')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        pickle.dump(RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
            [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
              'port': '6010', 'mirror_copies': 1},
                    {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                     'port': '6020', 'mirror_copies': 1}], 30),
            GzipFile(os.path.join(self.testdir, 'test_ring.ring.gz'), 'wb'))
        self.ring = Ring(os.path.join(self.testdir, 'test_ring.ring.gz'))

    def tearDown(self):
        """ Tear down for testing swift_lfs.fs.get_lfs """
        rmtree(os.path.dirname(self.testdir))

    def test_get_lfs_invalid_fs(self):
        conf = {'fs': 'afs', 'swift_dir': self.testdir,
                'devices': self.testdir}
        self.assertRaises(SwiftConfigurationError,
            lambda: lfs.get_lfs(conf, 'test_ring.ring.gz', 'test_lfs',
                FakeLogger()))
        try:
            lfs.get_lfs(conf, 'test_ring.ring.gz', 'test_lfs', FakeLogger())
        except SwiftConfigurationError, e:
            self.assertEqual(str(e),
                'Cannot load LFS. Invalid FS: afs. No module named afs')

    def test_get_lfs_xfs(self):
        conf = {'fs': 'xfs', 'swift_dir': self.testdir,
                'devices': self.testdir}
        storage = lfs.get_lfs(conf, self.ring, 'test_lfs', FakeLogger())
        self.assertTrue(isinstance(storage, lfs.xfs.LFSXFS))

    def test_get_lfs_zfs(self):
        conf = {'fs': 'zfs', 'swift_dir': self.testdir,
                'devices': self.testdir}
        try:
            import nspyzfs
            storage = lfs.get_lfs(conf, self.ring, 'test_lfs', FakeLogger())
            self.assertTrue(isinstance(storage, lfs.zfs.LFSZFS))
        except ImportError:
            self.assertRaises(LFSException,
                lambda:lfs.get_lfs(conf, self.ring, 'devices', 'test_lfs'))


if __name__ == '__main__':
    unittest.main()
