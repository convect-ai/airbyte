#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#
import json

import dateutil.parser
import datetime
import os
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Dict
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from .stream import GraphqlStream
from airbyte_cdk.sources.streams.core import package_name_from_class
from airbyte_cdk.sources.streams.core import IncrementalMixin
from .utils import ResourceLoader
from .auth import WayfairAuthenticator
from . import URL_MAP
from functools import cache

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


# Basic full refresh stream
class WayfairStream(GraphqlStream, ABC):
    """
    """

    def __init__(self, config: Dict):
        super().__init__(authenticator=WayfairAuthenticator(config))
        self.config = config

    @property
    def page_limit(self):
        return 100  # min(max(int(self.config.get("page_limit", 250)),1),1000)

    @property
    def url_base(self):
        return URL_MAP[self.config.get("environment")]["api"]

    @cache
    def get_graphql_query(self):
        query = ResourceLoader(package_name_from_class(self.__class__)).get_graphql_gql(self.name)
        query = f"{query.decode('utf-8')}".replace("\n", " ")
        return query

    def get_json_schema(self) -> Mapping[str, Any]:
        # self.logger.info(f"load json schema for {self.__class__}")
        depth_limit = 3  # min(max(int(self.config.get("depth_limit",3)),1),5)
        return ResourceLoader(package_name_from_class(self.__class__)).\
            get_schema(self.name, max_depth=depth_limit)

    def graphql_query(self,
                      stream_state: Mapping[str, Any],
                      stream_slice: Mapping[str, Any] = None,
                      next_page_token: Mapping[str, Any] = None,
                      ) -> str:
        return self.get_graphql_query()

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def request_headers(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        return {
            'content-type': 'application/json',
            'cache-control': 'no-cache'
        }

    def request_kwargs(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Mapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        json_response = response.json()
        data = json_response.get('data',[])
        errors = json_response.get('errors',[])
        records = data.get(self.data_field, []) if self.data_field is not None else data
        for record in records:
            yield record

    @property
    @abstractmethod
    def data_field(self) -> str:
        """The name of the field in the response which contains the data"""


# Basic incremental stream
class IncrementalWayfairStream(WayfairStream, IncrementalMixin, ABC):

    @property
    def state_checkpoint_interval(self) -> int:
        return self.page_limit

    @property
    def cursor_field(self) -> str:
        return "id"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {
            self.cursor_field: max(
                latest_record.get(self.cursor_field, ""),
                current_stream_state.get(self.cursor_field, ""),
            )
        }

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state[self.cursor_field] = value[self.cursor_field]


class PurchaseOrders(IncrementalWayfairStream):
    """
    Purchase Order stream
    """

    cursor_field = "poDate"
    primary_key = "poNumber"
    data_field = "purchaseOrders"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        create stream slices based on time window between cursor time (with look back time horizon) and current time
        Outputs:
        [
            {"poDate_start": 2021-04-01,
             "poDate_end": 2021-05-01,
            }
        ]
        """
        if stream_state is None:
            stream_state = {}
        curr_date_time = datetime.datetime.utcnow()
        cursor_datetime = dateutil.parser.isoparse(stream_state.get(self.cursor_field, self.config.get("start_date")))
        # Look back 24 hours beyond the current cursor
        stream_start_datetime = cursor_datetime - datetime.timedelta(hours=24)
        stream_slices_number = 12  # min(max(int(self.config.get('stream_slices_number', 1)),1),100)
        time_window_in_seconds = (curr_date_time - stream_start_datetime).total_seconds() / stream_slices_number
        for i in range(stream_slices_number):
            if i == (stream_slices_number - 1):
                # last time window
                yield {
                    f"{self.cursor_field}_start": stream_start_datetime + datetime.timedelta(seconds=int(i * time_window_in_seconds)),
                    f"{self.cursor_field}_end": None
                }
            else:
                yield {
                    f"{self.cursor_field}_start": stream_start_datetime + datetime.timedelta(seconds=int(i * time_window_in_seconds)),
                    f"{self.cursor_field}_end": stream_start_datetime + datetime.timedelta(seconds=int((i + 1) * time_window_in_seconds)),
                }

    def graphql_next_page_token(self, current_page_token: Mapping[str, Any], response: requests.Response) -> Optional[Mapping[str, Any]]:
        # self.logger.info(f"graphql_next_page_token:\n"
        #                  f"current_page_token={current_page_token}\n"
        #                  f"response.json={response.json()}")
        current_page_token = current_page_token or {}
        json_response = response.json()
        data = json_response.get('data',[])
        errors = json_response.get('errors',[])
        records = data.get(self.data_field, []) if self.data_field is not None else data
        if len(records) == 0:
            # current page no data then return None
            return None
        current_offset = current_page_token.get("offset", 0)
        next_offset = current_offset+len(records)
        return {
            "offset": next_offset
        }

    def graphql_variables(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None,
                          next_page_token: Mapping[str, Any] = None) -> str:
        # self.logger.info(f"stream_state={stream_state}")
        # self.logger.info(f"stream_slice={stream_slice}")
        stream_slice = stream_slice or {}
        _filters = []
        _offset = 0
        if next_page_token:
            _offset = next_page_token.get("offset",0)
        if self.config.get("open_order_only"):
            _filter = {
                "field":"open",
                "equals": "true"
            }
            _filters.append(_filter)
        if len(stream_slice) > 0:
            poDate_start = stream_slice.get("poDate_start")
            poDate_end = stream_slice.get("poDate_end")
            if poDate_start:
                _filter ={
                    "field":"poDate",
                    "greaterThanOrEqualTo": poDate_start.isoformat()
                }
                _filters.append(_filter)
            if poDate_end:
                _filter = {
                    "field":"poDate",
                    "lessThan": poDate_end.isoformat()
                }
                _filters.append(_filter)
        graphql_variables = {
            "filters": _filters,
            "limit": self.page_limit,
            "ordering": [{
                "asc": self.cursor_field
            }],
            "offset": _offset,
            "dryRun": False
        }
        # self.logger.info(f"graphql_varaibles={graphql_variables}")
        return json.dumps(graphql_variables)


# Source
class SourceWayfair(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        authenticator = WayfairAuthenticator(config)
        try:
            access_token = authenticator.get_access_token()
            if access_token:
                url = URL_MAP[config.get("environment")]["check"]
                headers = {
                    'Authorization': 'Bearer ' + access_token,
                    'Content-Type': 'application/json',
                }
                response = requests.request('GET', url, headers=headers)
                response.raise_for_status()
                response_json = response.json()
                if len(response_json['errors'])>0:
                    return False, response_json['errors']
                logger.info(f"{response_json['data']}")
                return True, None
        except (requests.exceptions.RequestException, IndexError) as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [PurchaseOrders(config)]
