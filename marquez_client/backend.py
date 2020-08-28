# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

log = logging.getLogger(__name__)


class Backend:
    def put(self, path, headers, json):
        pass

    def post(self, path, headers, json=None):
        pass

"""'''

c = Clients.createMarquezWOClient(Backend, config)
c.createNamespace()
c.createRun()

MarquezWO(BackendHttp, config)
def create_namespace(params from config):
    valid parms
    backend.put()

def create_source(params from config):
    valid parms
    backend.put()

@staticmethod
def put():
    dfsdfksjfl

    http.put()
    .,klsdfslfkj


MarquezWO(BackendFile, config)
def create_namespace(params from config):
    valid parms
    backend.write()


MarquezWO(BackendKafka, config)
def create_namespace(params from config):
    valid parms
    backend.publish()

'''"""