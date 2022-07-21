#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import time
from abc import ABC
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream

from .utils import ShipoutAuthenticator 

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
class ShipoutStream(HttpStream, ABC):
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
    `class ShipoutStream(HttpStream, ABC)` which is the current class
    `class Customers(ShipoutStream)` contains behavior to pull data for customers using v1/customers
    `class Employees(ShipoutStream)` contains behavior to pull data for employees using v1/employees

    If some streams implement incremental sync, it is typical to create another class
    `class IncrementalShipoutStream((ShipoutStream), ABC)` then have concrete stream implementations extend it. An example
    is provided below.

    See the reference docs for the full list of configurable options.
    """

    page_size = 50
    sandbox_endpoint = "https://opendev.shipout.com/api/"
    base_endpoint = "https://open.shipout.com/api/"

    def __init__(self, config: Dict):
        authenticator = ShipoutAuthenticator(config, path=self.path())
        super().__init__(authenticator=authenticator)
        self.config = config

    @property
    def url_base(self) -> str:
        use_sandbox = self.config.get("use_sandbox", False)
        if use_sandbox:
            return self.sandbox_endpoint

        return self.base_endpoint

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

        res = response.json()
        data = res.get("data")
        current = data.get("current")
        records = data.get("records")
        if len(records) == 0:
            return None
        return {
            "page": int(current) + 1,
        }

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        json_response = response.json()
        data = json_response.get("data")
        if data:
            if isinstance(data, list):
                for record in data:
                    yield record
            else:
                records = data.get("records")
                if records:
                    for record in records:
                        yield record
        else:
            raise Exception("No data found in response")

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        """
        TODO: Override this method to define any query parameters to be set. Remove this method if you don't need to define request params.
        Usually contains common params e.g. pagination size etc.
        """
        if next_page_token is None:
            curPageNo = 1
        else:
            curPageNo = next_page_token.get("page")
        return {"curPageNo": curPageNo, "pageSize": self.page_size}



class Warehouses(ShipoutStream):
    """
    TODO: Change class name to match the table/data source this stream corresponds to.
    """

    # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
    primary_key = "warehouseId"

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        """ """
        return "open-api/oms/info/warehouse/list"


class Products(ShipoutStream):
    """ """

    primary_key = "skuId"

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "open-api/oms/product/queryList"


class Stocks(ShipoutStream):
    """ """

    primary_key = "_platform_object_id"

    def path(
        self, *, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "open-api/oms/stock/list"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state, stream_slice, next_page_token)
        # sync all inventory records, not only those in-stock
        params["showSkuType"] = 0
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        """
        :return an iterable containing each record in the response
        """
        json_response = response.json()
        data = json_response.get("data")
        if data:
            records = data.get("records")
            if records:
                for record in records:
                    record[self.primary_key] = record.get("warehouseId") + "-" + record.get("omsSku")
                    yield record
        else:
            raise Exception("No data found in response")


# Source
class SourceShipout(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
        for an example.

        :param config:  the user-input config object conforming to the connector's spec.yaml
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """
        try:
            resp = list(Warehouses(config).read_records(sync_mode=None))
            warehouse_id = resp[0].get("warehouseId")
            if warehouse_id is not None:
                return True, None
        except (requests.exceptions.RequestException, IndexError) as e:
            return False, e

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        :param config: A Mapping of the user input configuration as defined in the connector spec.
        """
        stream_instances = [
            Warehouses(config),
            Products(config),
            Stocks(config),
        ]
        return stream_instances
