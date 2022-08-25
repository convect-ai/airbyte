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
Jushuitan is an ERP system for e-commerce.

Documentation: https://openweb.jushuitan.com/doc
"""


# Basic full refresh stream
class JushuitanStream(HttpStream, ABC):

    page_size: Optional[int] = 50

    def __init__(self, config: Dict[str, Any], authenticator=None):
        super().__init__(authenticator=authenticator)
        self.config = config

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
        resp_json = response.json()
        has_next = resp_json.get("data").get("has_next")
        if not has_next:
            return None

        next_page = resp_json.get("data").get("page_index")

        return {"page": int(next_page) + 1}

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        payload = {"page_size": self.page_size}

        if next_page_token:
            payload["page_index"] = next_page_token.get("page")
        return payload

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        resp_json = response.json()
        try:
            records = resp_json.get("data").get("datas") or []
        except Exception:
            print("error response", resp_json)

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
        resp_json = response.json()
        try:
            records = resp_json.get("data").get("inventorys") or []
        except Exception:
            print("error respons", resp_json)

        yield from records


# Source
class SourceJushuitan(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        stream = Shop(config=config)
        try:
            _ = list(stream.read_records(sync_mode=SyncMode.full_refresh))

        except Exception as e:
            logger.debug(e)
            return False, e
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [Shop(config=config), Warehouse(config=config), SKUMap(config=config), SKU(config=config), Inventory(config=config)]
