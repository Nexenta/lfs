# Copyright (c) 2011-2012 Nexenta Systems Inc
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

from urllib import unquote

from webob import Request, Response
from webob.exc import HTTPBadRequest, HTTPNotFound

from swift.common.ring import Ring
from swift.account.server import DATADIR as ACCOUNT_DATADIR
from swift.container.server import DATADIR as CONTAINER_DATADIR
from swift.obj.server import DATADIR as OBJECT_DATADIR
from swift.common.utils import get_logger
try:
    from swift.manifest.server import DATADIR as MANIFEST_DATADIR
except ImportError:
    MANIFEST_DATADIR = 'manifests'
try:
    from swift.chunk.server import DATADIR as CHUNK_DATADIR
except ImportError:
    CHUNK_DATADIR = 'chunks'

from swift_lfs.fs import get_lfs, SWIFT_DEVICE_ONLINE,\
    SWIFT_DEVICE_MISCONFIGURED, SWIFT_DEVICE_DEGRADED, SWIFT_DEVICE_FAULTED


DATADIRS = {
    'account': ACCOUNT_DATADIR,
    'container': CONTAINER_DATADIR,
    'object': OBJECT_DATADIR,
    'manifest': MANIFEST_DATADIR,
    'chunk': CHUNK_DATADIR,
}

DEFAULT_PORT = {
    'account': 6002,
    'container': 6001,
    'object': 6000,
    'manifest': 6003,
    'chunk': 6004,
}


class LFSMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        logger = get_logger(conf, log_route='swift_lfs')
        swift_dir = conf.get('swift_dir', '/etc/swift')
        storage_type = conf.get('storage_type')
        if storage_type not in DATADIRS:
            raise Exception(_('Configuration error: Requires "storage_type" '\
                              'be set to: %s; was set to "%s"' %\
                              (', '.join(DATADIRS.keys()), storage_type)))
        ring = Ring(swift_dir, ring_name=storage_type)
        self.storage = get_lfs(conf, ring, DATADIRS[storage_type],
                               DEFAULT_PORT[storage_type], logger)
        self.storage.setup_node()

    def STATUS(self, request, storage):
        """
        STATUS handler

        :param request: webob.Request object
        :param storage: LFS storage class
        :returns : webob.Response class
        """
        devices = []
        dev_path = unquote(request.path)
        if dev_path == '/' or not dev_path:
            devices = None
        else:
            devices.append(dev_path[1:])
        try:
            dev_status = storage.get_device_status(devices)
        except Exception, err:
            return HTTPBadRequest(request=request, content_type='text/plain',
                body=str(err))
        if dev_status is None:
            return HTTPNotFound(request=request)
        out_content = []
        for device, params in dev_status.items():
            stat, mirror_count = params
            if stat == SWIFT_DEVICE_ONLINE:
                status = 'online'
            elif stat == SWIFT_DEVICE_MISCONFIGURED:
                status = 'misconfigured'
            elif stat == SWIFT_DEVICE_DEGRADED:
                status = 'degraded'
            elif stat == SWIFT_DEVICE_FAULTED:
                status = 'faulted'
            else:
                status = 'unknown'
            out_content.append('%s:%s:%d' % (device, status, mirror_count))
        return Response(body='\n'.join(out_content), request=request,
            charset='utf-8', content_type='text/plain')

    def __call__(self, env, start_response):
        if env['REQUEST_METHOD'] == 'STATUS':
            req = Request(env)
            res = self.STATUS(req, self.storage)
            return res(env, start_response)
        env['swift.storage'] = self.storage
        env['swift.setup_datadir'] = self.storage.setup_datadir
        env['swift.setup_tmp'] = self.storage.setup_tmp
        env['swift.setup_partition'] = self.storage.setup_partition
        return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def create_filter(app):
        return LFSMiddleware(app, conf)
    return create_filter
