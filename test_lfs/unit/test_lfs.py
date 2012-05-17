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
import unittest
from tempfile import mkdtemp
import cPickle as pickle
from gzip import GzipFile
from shutil import rmtree

from swift.common.utils import mkdirs
from swift.common.ring import RingData

from swift_lfs import lfs

class FakeApp(object):
    def __call__(self, env, start_response):
        return "FAKE APP"


RESPONSE = ''

def start_response(*args):
    global RESPONSE
    RESPONSE = args


class TestLFSMiddleware(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'lfs_middleware')
        mkdirs(self.testdir)
        pickle.dump(RingData([[0, 1, 0, 1], [1, 0, 1, 0]],
            [{'id': 0, 'zone': 0, 'device': 'sda1', 'ip': '127.0.0.1',
              'port': '6010', 'mirror_copies': 1},
                    {'id': 1, 'zone': 1, 'device': 'sdb1', 'ip': '127.0.0.1',
                     'port': '6020', 'mirror_copies': 1}], 30),
            GzipFile(os.path.join(self.testdir, 'account.ring.gz'), 'wb'))
        conf = {'storage_type': 'account', 'swift_dir': self.testdir}
        self.app = lfs.LFSMiddleware(FakeApp(), conf)

    def tearDown(self):
        rmtree(self.testdir)

    def test_STATUS(self):
        env = {'REQUEST_METHOD': 'STATUS', 'PATH_INFO': ''}
        resp = self.app(env, start_response)[0]
        self.assertEqual(RESPONSE[0][:3], '200')
        resp_body = resp.split()
        resp_body.sort()
        self.assertEqual('\n'.join(resp_body), 'sda1:online:1\nsdb1:online:1')

        for c in ('a', 'b'):
            env['PATH_INFO'] = '/sd%s1' % c
            resp = self.app(env, start_response)[0]
            self.assertEqual(RESPONSE[0][:3], '200')
            self.assertEqual(resp, 'sd%s1:online:1' % c)

        env['PATH_INFO'] = '/sdz1'
        self.app(env, start_response)[0]
        self.assertEqual(RESPONSE[0][:3], '404')


if __name__ == '__main__':
    unittest.main()
