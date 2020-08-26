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

import unittest
import marquez_client
import logging
import logging.config
import yaml
import json
import mock
import json

_NAMESPACE = 'default'
log = logging.getLogger(__name__)

class StructuredMessage:
    def __init__(self, message, /, **kwargs):
        self.message = message
        self.kwargs = kwargs

    def __str__(self):
        return '%s >>> %s' % (self.message, json.dumps(self.kwargs))


_ = StructuredMessage   # optional, to improve readability


class TestAirflowDAG(unittest.TestCase):
    def setUp(self):
        log.debug("TestAirflowDAG.setup(): ")

        with open('tests/logConfig.yaml', 'rt') as file:
            yamlConfig = yaml.safe_load(file.read())
            logging.config.dictConfig(yamlConfig)
            log.info("loaded logConfig.yaml")

        self.client = marquez_client.MarquezClient("http://localhost:5000")
        log.info("created marquez_client.")

    @mock.patch("marquez_client.MarquezClient._get")
    def test_create_dag(self, mock_get):
        log.debug("TestAirflowDAG.test_create_dag: ")

        mock_get.return_value = {"name": _NAMESPACE}

        for i in range(100):
            namespace = self.client.get_namespace(_NAMESPACE)
            assert namespace['name'] == _NAMESPACE

        log.info("Done!")


if __name__ == '__main__':
    unittest.main()
