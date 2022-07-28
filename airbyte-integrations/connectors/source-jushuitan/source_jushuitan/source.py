#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import datetime
import itertools
import json
from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

from .auth import get_sign
from .utils import chunk_date_range

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
class JushuitanStream(HttpStream, ABC):
    """
    TODO remove this comment

    This class represents a stream output by the connector.
    This is an abstract base class meant to contain all the common functionality at the API level e.g: the API base URL, pagination strategy,
    parsing responses etc..

    Each stream should extend this class (or another abstract subclass of it) to specify behavior unique to that stream.

    Typically for REST APIs each stream corresponds to a resource in the API. For example if the API
    contains the endpoints
        - GET v1/customers
        - GET v1/employees

    then you should have three classes:
    `class JushuitanStream(HttpStream, ABC)` which is the current class
    `class Customers(JushuitanStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(JushuitanStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalJushuitanStream((JushuitanStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    page_size: Optional[int] = 50
    # transformer: TypeTransformer = TypeTransformer(
    #     TransformConfig.DefaultSchemaNormalization | TransformConfig.CustomSchemaNormalization
    # )

    def __init__(self, config: Dict[str, Any], authenticator=None):
        super().__init__(authenticator=authenticator)
        self.config = config

    # @transformer.registerCustomTransform
    # def transform_dt(
    #     original_value: Any, field_schema: Dict[str, Any]
    # ) -> Any:
    #     """
    #     Convert the datetime string e.g., 2021-12-0113:58:41 to 2021-12-01 13:58:41.
    #     """
    #     if field_schema.get("format", "") == "date-time" and original_value:
    #         # do something
    #         try:
    #             new_value = pendulum.from_format(
    #                 original_value, fmt="YYYY-MM-DDHH:mm:ss"
    #             ).to_datetime_string()
    #             return new_value
    #         except ValueError:
    #             print("error", original_value)

    #     return original_value

    @property
    def http_method(self) -> str:
        return "POST"

    @property
    def url_base(self) -> str:
        use_sandbox = self.config.get("use_sandbox", False)
        if not use_sandbox:
            return "https://openapi.jushuitan.com/"

        return "https://dev-api.jushuitan.com/"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        resp_json = response.json()
        has_next = resp_json.get("data").get("has_next")
        if not has_next:
            return None

        next_page = resp_json.get("data").get("page_index")

        return {"page": int(next_page) + 1}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        payload = {"page_size": self.page_size}

        if next_page_token:
            payload["page_index"] = next_page_token.get("page")
        return payload

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        resp_json = response.json()
        try:
            records = resp_json.get("data").get("datas") or []
        except Exception:
            print("error respons", resp_json)

        yield from records

    def common_body_data(self) -> Mapping[str, str]:
        """
        Construct the common body data for every payload
        """
        access_token = self.config["access_token"]
        app_key = self.config["app_key"]
        timestamp = str(int(datetime.datetime.now().timestamp()))  # unix time in unit of seconds
        version = "2"
        charset = "utf-8"

        return {"access_token": access_token, "app_key": app_key, "timestamp": timestamp, "version": version, "charset": charset}

    def request_body_data(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Optional[Union[Mapping, str]]:
        # we add all the query parameters under biz fields and sign the entire payload to get the sign field
        query_params = self.request_params(stream_state, stream_slice, next_page_token)

        common_data = self.common_body_data()
        body_data = {**common_data, "biz": json.dumps(query_params)}

        # sign the payload
        sign = get_sign(self.config["app_secret"], body_data)

        body_data["sign"] = sign
        return body_data

    def _create_prepared_request(
        self, path: str, headers: Mapping = None, params: Mapping = None, json: Any = None, data: Any = None
    ) -> requests.PreparedRequest:
        """
        Override to ignore all request_params
        """
        params = {}
        return super()._create_prepared_request(path, headers, params, json, data)

    def should_retry(self, response: requests.Response) -> bool:
        """
        Retry for code 199 and 200. This is when we hit the rate limiter
        """
        resp_json = response.json()
        return_code = resp_json.get("code")
        if return_code in {199, 200}:
            return True
        return False


class Shop(JushuitanStream):
    primary_key = "shop_id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "open/shops/query"


class Warehouse(JushuitanStream):
    primary_key = "wms_co_id"

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "open/wms/partner/query"


# Basic incremental stream
class TimeSliceStream(JushuitanStream, ABC):
    """
    Abstract class for time slice based streams.
    Most Jushuitan's query endpoint supports time slice based params with (t, t+7) as the starting and ending date
    for modification time.
    """

    state_checkpoint_interval = 50
    look_back_window = 7

    @property
    def cursor_field(self) -> str:
        """
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return "modified_end"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        # these two are both strings and should be comparable

        current_cur = max(current_stream_state.get(self.cursor_field, self.config.get("start_date")), latest_record.get("modified"))

        return {self.cursor_field: current_cur}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        # add modified_begin and modified_end fields
        modified_start, modified_end = stream_slice.get("modified_start"), stream_slice.get("modified_end")
        params.update({"modified_begin": modified_start.to_datetime_string(), "modified_end": modified_end.to_datetime_string()})
        return params

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        # generate a list of time based slices (t,t+7), with the earliest t = starting date
        stream_state = stream_state or {}
        start_date = stream_state.get(self.cursor_field, self.config.get("start_date"))

        periods_iter = chunk_date_range(pendulum.parse(start_date), pendulum.duration(days=7))
        yield from map(lambda x: {"modified_start": x.start, "modified_end": x.end}, periods_iter)


class SKUMap(TimeSliceStream):

    primary_key = ["shop_id", "sku_id", "shop_sku_id"]

    def path(self, **kwargs) -> str:
        return "open/skumap/query"


class SKU(TimeSliceStream):

    primary_key = "sku_id"

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "open/sku/query"


class Inventory(TimeSliceStream):

    primary_key = ["sku_id", "wms_co_id", "modified"]

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "open/inventory/query"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        warehouse_id = latest_record["wms_co_id"]
        modified = latest_record["modified"]

        if warehouse_id not in current_stream_state:
            return {**current_stream_state, **{warehouse_id: {"modified_end": modified}}}
        else:
            max_modified = max(current_stream_state[warehouse_id]["modified_end"], modified)

            current_stream_state.update({warehouse_id: {"modified_end": max_modified}})

            return current_stream_state

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)

        # add the warehouse information
        warehouse_id = stream_slice["wms_co_id"]
        params.update({"wms_co_id": warehouse_id})

        return params

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        For each warehouse, we generate one slice
        """
        stream_state = stream_state or {}
        warehouse_stream = Warehouse(config=self.config)

        global_start_date = pendulum.parse(self.config.get("start_date"))
        # we add wms_co_id = 0 to fetch the inventory summation across all warehouses
        record_iter = itertools.chain([{"wms_co_id": 0}], warehouse_stream.read_records(sync_mode=SyncMode.full_refresh))
        for warehouse_record in record_iter:
            warehouse_id = warehouse_record.get("wms_co_id")
            warehouse_inventory_state = stream_state.get(warehouse_id, {})
            start_date = warehouse_inventory_state.get("modified_end", global_start_date)
            date_periods = chunk_date_range(start_date=start_date, interval=pendulum.duration(days=self.look_back_window))
            warehouse_slices = map(lambda x: {"modified_start": x.start, "modified_end": x.end, "wms_co_id": warehouse_id}, date_periods)

            yield from warehouse_slices

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_slice: Mapping[str, Any] = None,
        stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        records = super().read_records(sync_mode, cursor_field, stream_slice, stream_state)
        # append the current warehouse id to each record
        yield from map(lambda x: {**x, **{"wms_co_id": stream_slice.get("wms_co_id")}}, records)

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        TODO: Override this method to define how a response is parsed.
        :return an iterable containing each record in the response
        """
        resp_json = response.json()
        try:
            records = resp_json.get("data").get("inventorys") or []
        except Exception:
            print("error respons", resp_json)

        yield from records


# Source
class SourceJushuitan(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        stream = Shop(config=config)
        # try:
        res = list(stream.read_records(sync_mode=SyncMode.full_refresh))

        print(res)
        # except Exception as e:
        #     logger.debug(e)
        #     return False, e
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        TODO: Replace the streams below with your own streams.

        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        # TODO remove the authenticator if not required.
        return [Shop(config=config), Warehouse(config=config), SKUMap(config=config), SKU(config=config), Inventory(config=config)]
