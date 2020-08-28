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
import logging
import logging.config
import yaml
import mock
import os
import uuid
import random

from marquez_client.models import (SourceType, DatasetType, JobType)
from marquez_client.clients import Clients

_NAMESPACE = 'default'

log = logging.getLogger(__name__)


class TestAirflowDAG(unittest.TestCase):
    def setUp(self):
        log.debug("TestAirflowDAG.setup(): ")

        with open('tests/logConfig.yaml', 'rt') as file:
            yamlConfig = yaml.safe_load(file.read())
            logging.config.dictConfig(yamlConfig)
            log.info("loaded logConfig.yaml")

        # backend = os.environ.get('MARQUEZ_BACKEND', MARQUEZ_BACKEND)

        self.client = Clients.new_write_only_client()
        log.info("created marquez_client.")

    def test_create_dag(self):
        log.debug("TestAirflowDAG::test_create_dag")

        NAMESPACE = "my-namespace"
        OWNER = "me"
        SOURCE = "my-source"
        DATASET = "my-dataset"
        PHYSICAL = "public.my_table"
        run_id = str(uuid.uuid4())
        JOB = "my-job"

        for i in range(1000):
            NAMESPACE = "my-namespace"
            OWNER = "me"
            SOURCE = "my-source"
            DATASET = f'my-dataset-{i}'
            PHYSICAL = f'public.my_table-{i}'
            run_id = str(uuid.uuid4())
            JOB = f'my-job-{i%10}'

            self.client.create_namespace(NAMESPACE, OWNER)
            self.client.create_source(
                SOURCE,
                SourceType.POSTGRESQL,
                "jdbc:postgresql://localhost:5432/test?user=fred&ssl=true")
            self.client.create_dataset(
                NAMESPACE, DATASET, DatasetType.DB_TABLE,
                PHYSICAL, SOURCE, run_id)
            self.client.create_job(NAMESPACE, JOB, JobType.BATCH)
            self.client.create_job_run(NAMESPACE, JOB, run_id)
            self.client.mark_job_run_as_started(run_id)

            udiff = (i % 10 - random.randrange(10))

            if udiff >= -1 or udiff <= 1:
                self.client.mark_job_run_as_failed(run_id)
            else:
                self.client.mark_job_run_as_completed(run_id)

        log.debug("Done!")


'''
        if (backend == 'http'):
            self.client = \
                marquez_client.MarquezClient("http://localhost:5000")
        else:
            self.client = marquez_client.MarquezClientWO();

    @mock.patch("marquez_client.MarquezClient._get")
    def test_create_dag(self, mock_get):
        log.debug("TestAirflowDAG.test_create_dag: ")

        mock_get.return_value = {"name": _NAMESPACE}

        for i in range(100):
            namespace = self.client.get_namespace(_NAMESPACE)
            assert namespace['name'] == _NAMESPACE
            log.info("True")

        log.info("Done!")

    def test_create_dag_http(self):
        log.debug("TestAirflowDAG.test_create_dag: ")

        for i in range(100):
            namespace = self.client.get_namespace(_NAMESPACE)
            assert namespace['name'] == _NAMESPACE

        log.info("Done!")
'''


if __name__ == '__main__':
    unittest.main()
