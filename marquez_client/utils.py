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

from marquez_client.models import DatasetFieldType, DatasetType


def make_field(name, data_type, description=None):
    if isinstance(data_type, str):
        if not DatasetFieldType.__members__.__contains__(data_type):
            raise ValueError(f'Invalid field type: {data_type}')
    elif isinstance(data_type, DatasetFieldType):
        data_type = data_type.name
    else:
        raise ValueError('data_type must be a str or a DatasetFieldType')

    DatasetType.__members__.get(data_type)
    field = {
        'name': name,
        'type': data_type
    }
    if description:
        field['description'] = description
    return field
