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

import json
import os
import requests
import time
import uuid
import logging

from .models import DatasetType, SourceType, JobType
from marquez_client import errors
from marquez_client.constants import (DEFAULT_TIMEOUT_MS)
from marquez_client.version import VERSION
from six.moves.urllib.parse import quote

_API_PATH = '/api/v1'
_USER_AGENT = f'marquez-python/{VERSION}'
_HEADERS = {'User-Agent': _USER_AGENT}

log = logging.getLogger(__name__)


# Marquez Client WO
class MarquezClientWO(object):
    def __init__(self, url, timeout_ms=None):
        self._timeout = self._to_seconds(
            timeout_ms or os.environ.get
            ('MARQUEZ_TIMEOUT_MS', DEFAULT_TIMEOUT_MS))

        self._api_base = _API_PATH
        log.debug(self._api_base)

    # Namespace API
    def create_namespace(self, namespace_name, owner_name, description=None):
        log.debug("create_namespace()")

        MarquezClientWO._check_name_length(namespace_name, 'namespace_name')
        MarquezClientWO._check_name_length(owner_name, 'owner_name')

        payload = {
            'ownerName': owner_name
        }

        if description:
            payload['description'] = description

        return self._put(
            self._url('/namespaces/{0}', namespace_name),
            payload=payload
        )

    # Source API
    def create_source(self, source_name, source_type, connection_url,
                      description=None):
        log.debug("create_source()")

        MarquezClientWO._check_name_length(source_name, 'source_name')
        MarquezClientWO._is_instance_of(source_type, SourceType)

        MarquezClientWO._is_valid_connection_url(connection_url)

        payload = {
            'type': source_type.value,
            'connectionUrl': connection_url
        }

        if description:
            payload['description'] = description

        return self._put(self._url('/sources/{0}', source_name),
                         payload=payload)

    # Datasets API
    def create_dataset(self, namespace_name, dataset_name, dataset_type,
                       physical_name, source_name, run_id,
                       description=None, schema_location=None,
                       fields=None, tags=None):
        log.debug("create_dataset()")

        MarquezClientWO._check_name_length(namespace_name, 'namespace_name')
        MarquezClientWO._check_name_length(dataset_name, 'dataset_name')
        MarquezClientWO._is_instance_of(dataset_type, DatasetType)

        if dataset_type == DatasetType.STREAM:
            MarquezClientWO._is_none(schema_location, 'schema_location')

        MarquezClientWO._is_none(run_id, 'run_id')
        MarquezClientWO._check_name_length(physical_name, 'physical_name')
        MarquezClientWO._check_name_length(source_name, 'source_name')

        payload = {
            'type': dataset_type.value,
            'physicalName': physical_name,
            'sourceName': source_name,
            'runId': run_id,
        }

        if description:
            payload['description'] = description

        if fields:
            payload['fields'] = fields

        if tags:
            payload['tags'] = tags

        if schema_location:
            payload['schemaLocation'] = schema_location

        return self._put(
            self._url('/namespaces/{0}/datasets/{1}', namespace_name,
                      dataset_name),
            payload=payload
        )

    def tag_dataset(self, namespace_name, dataset_name, tag_name):
        log.debug("tag_dataset()")

        MarquezClientWO._check_name_length(namespace_name, 'namespace_name')
        MarquezClientWO._check_name_length(dataset_name, 'dataset_name')

        if not tag_name:
            raise ValueError('tag_name must not be None')

        return self._post(
            self._url('/namespaces/{0}/datasets/{1}/tags/{2}',
                      namespace_name, dataset_name, tag_name)
        )

    def tag_dataset_field(self, namespace_name, dataset_name, field_name,
                          tag_name):
        log.debug("tag_dataset_field()")

        MarquezClientWO._check_name_length(namespace_name, 'namespace_name')
        MarquezClientWO._check_name_length(dataset_name, 'dataset_name')
        MarquezClientWO._check_name_length(field_name, 'field_name')
        MarquezClientWO._check_name_length(tag_name, 'tag_name')

        return self._post(
            self._url('/namespaces/{0}/datasets/{1}/fields/{2}/tags/{3}',
                      namespace_name, dataset_name, field_name, tag_name)
        )

    # Job API
    def create_job(self, namespace_name, job_name, job_type,
                   location=None, input_dataset=None,
                   output_dataset=None, description=None, context=None):
        log.debug("create_job()")

        MarquezClientWO._check_name_length(namespace_name, 'namespace_name')
        MarquezClientWO._check_name_length(job_name, 'job_name')
        MarquezClientWO._is_instance_of(job_type, JobType)

        payload = {
            'inputs': input_dataset or [],
            'outputs': output_dataset or [],
            'type': job_type.name
        }

        if context:
            payload['context'] = context

        if location:
            payload['location'] = location

        if description:
            payload['description'] = description

        return self._put(
            self._url('/namespaces/{0}/jobs/{1}', namespace_name, job_name),
            payload=payload
        )

    def create_job_run(self, namespace_name, job_name, run_id,
                       nominal_start_time=None,
                       nominal_end_time=None, run_args=None,
                       mark_as_running=False):
        log.debug("create_job_run()")

        MarquezClientWO._check_name_length(namespace_name, 'namespace_name')
        MarquezClientWO._check_name_length(job_name, 'job_name')

        payload = {}

        if nominal_start_time:
            payload['nominalStartTime'] = nominal_start_time

        if nominal_end_time:
            payload['nominalEndTime'] = nominal_end_time

        if run_args:
            payload['runArgs'] = run_args

        response = self._post(
            self._url('/namespaces/{0}/jobs/{1}/runs?external_id={2}',
                      namespace_name, job_name, run_id),
            payload=payload)

        if mark_as_running:
            response = self.mark_job_run_as_started(run_id)

        return response

    def mark_job_run_as_started(self, run_id):
        log.debug("mark_job_run_as_started()")

        return self.__mark_job_run_as(run_id, 'start')

    def mark_job_run_as_completed(self, run_id):
        log.debug("mark_job_run_as_completed()")

        return self.__mark_job_run_as(run_id, 'complete')

    def mark_job_run_as_failed(self, run_id):
        log.debug("mark_job_run_as_failed()")

        return self.__mark_job_run_as(run_id, 'fail')

    def mark_job_run_as_aborted(self, run_id):
        log.debug("mark_job_run_as_aborted()")

        return self.__mark_job_run_as(run_id, 'abort')

    def __mark_job_run_as(self, run_id, action):
        log.debug("__mark_job_run_as()")

        MarquezClientWO._is_valid_uuid(run_id, 'run_id')

        return self._post(
            self._url('/jobs/runs/{0}/{1}', run_id, action), payload={}
        )

    # Common
    def _url(self, path, *args):
        encoded_args = [quote(arg.encode('utf-8'), safe='') for arg in args]
        return f'{self._api_base}{path.format(*encoded_args)}'

    def _post(self, url, payload, as_json=True):
        log.debug("_post()")

        post_details = {}

        post_details['method'] = 'POST'
        post_details['path'] = url
        post_details['headers'] = _HEADERS
        post_details['payload'] = payload

        log.info(post_details)

    def _put(self, url, payload=None, as_json=True):
        log.debug("_put()")

        put_details = {}

        put_details['method'] = 'PUT'
        put_details['path'] = url
        put_details['headers'] = _HEADERS
        put_details['payload'] = payload

        log.info(put_details)

    @staticmethod
    def _now_ms():
        return int(round(time.time() * 1000))

    def _response(self, response, as_json):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            self._raise_api_error(e)

        return response.json() if as_json else response.text

    def _raise_api_error(self, e):
        # TODO: https://github.com/MarquezProject/marquez-python/issues/55
        raise errors.APIError() from e

    @staticmethod
    def _to_seconds(timeout_ms):
        return float(timeout_ms) / 1000.0

    @staticmethod
    def _is_none(variable_value, variable_name):
        if not variable_value:
            raise ValueError(f"{variable_name} must not be None")

    @staticmethod
    def _check_name_length(variable_value, variable_name):
        MarquezClientWO._is_none(variable_value, variable_name)

        # ['namespace_name', 'owner_name', 'source_name'] <= 64
        # ['dataset_name', 'field_name', 'job_name', 'tag_name'] <= 255
        if variable_name in ['namespace_name', 'owner_name', 'source_name']:
            if len(variable_value) > 64:
                raise ValueError(f"{variable_name} length is"
                                 f" {len(variable_value)}, must be <= 64")
        else:
            if len(variable_value) > 255:
                raise ValueError(f"{variable_name} length is"
                                 f" {len(variable_value)}, must be <= 255")

    @staticmethod
    def _is_valid_uuid(variable_value, variable_name):
        MarquezClientWO._is_none(variable_value, variable_name)

        try:
            uuid.UUID(str(variable_value))
        except ValueError:
            raise ValueError(f"{variable_name} must be a valid UUID")

    @staticmethod
    def _is_instance_of(variable_value, variable_enum_type):
        if not isinstance(variable_value, variable_enum_type):
            raise ValueError(f"{variable_value} must be an instance"
                             f" of {variable_enum_type}")

    @staticmethod
    def _is_valid_connection_url(connection_url):
        MarquezClientWO._is_none(connection_url, 'connection_url')
