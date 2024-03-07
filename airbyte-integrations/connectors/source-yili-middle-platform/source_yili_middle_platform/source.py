#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import base64
import hashlib
import hmac
import json
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import NoAuth

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.yaml file.
"""


class YiliMiddlePlatformStream(HttpStream):
    url_base = 'https://datagrid-api.digitalyili.com'
    primary_key = None
    http_method = 'POST'

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.host = 'datagrid-api.digitalyili.com'
        self.env = config['env']
        self.stage = config['stage']
        self.page_size = 1000
        self.current_page = 0
        self.records_processed = 0

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f'/list/{self.apiId}'

    def request_headers(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        headers = {
            'Accept': 'application/json;charset=utf-8',
            'X-Ca-Key': self.appKey,
            'Host': self.host,
            'X-Ca-Stage': self.stage,
            'X-Ca-Signature-Headers': 'x-ca-key,x-ca-signature-method,x-ca-stage',
            'Content-Type': 'application/octet-stream;charset=utf-8',
            'X-Ca-Signature-Method': 'HmacSHA256',
        }
        body = json.dumps({
            "returnFields": self.returnFields,
            "pageStart": self.records_processed,
            "pageSize": self.page_size
        })
        contentMD5 = self.getContentMD5(body)
        headers["Content-Md5"] = contentMD5
        stringToSign = self.getStringToSign(headers, f"{self.path()}?appKey={self.appKey}&appSecret={self.appSecret}&env={self.env}")
        sign = self.getSign(stringToSign, self.appSecret)
        headers["X-Ca-Signature"] = sign
        return headers

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {'appKey': self.appKey, 'appSecret': self.appSecret, 'env': self.env}

    def request_body_json(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Mapping]:
        return {
            "returnFields": self.returnFields,
            "pageStart": self.records_processed,
            "pageSize": self.page_size
        }

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        results = response.json().get("results", [])
        self.records_processed += len(results)
        return results

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        results = response.json().get("results", [])
        if len(results) < self.page_size:
            return None
        self.current_page += 1
        return {"page": self.current_page}

    def getContentMD5(self, body):
        return base64.b64encode(hashlib.md5(bytes(body, 'utf-8')).digest()).decode()

    def getStringToSign(self, headers, path):
        stringToSign = f'''POST
{headers.get("Accept")}
{headers.get("Content-Md5")}
{headers.get("Content-Type")}

x-ca-key:{headers.get("X-Ca-Key")}
x-ca-signature-method:{headers.get("X-Ca-Signature-Method")}
x-ca-stage:{headers.get("X-Ca-Stage")}
{path}'''
        return stringToSign

    def getSign(self, stringToSign, appSecret):
        return base64.b64encode(hmac.new(
            bytes(appSecret, 'utf-8'),
            msg=bytes(stringToSign, 'utf-8'),
            digestmod=hashlib.sha256
        ).digest()).decode()

    def _send(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        print(f"Request URL: {request.url}")
        print(f"Request Method: {request.method}")
        print(f"Request Headers: {request.headers}")
        if request.body:
            print(f"Request Body: {request.body}")

        response = super()._send(request, request_kwargs)

        print(f"Response Status Code: {response.status_code}")
        print(f"Response Headers: {response.headers}")
        print(f"Response Body: {response.text[:100]}")

        return response


class ProductionPlans(YiliMiddlePlatformStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.apiId = config['product_plan_api_id']
        self.appKey = config['product_plan_app_key']
        self.appSecret = config['product_plan_app_secret']
        self.table_name = 'production_plan'
        self.returnFields = ["z_year_month", "product_no", "product_name", "logical_node_no", "logical_node_name", "z_plan_prod_qty",
                             "prod_distribute_type"]


class DemandPlans(YiliMiddlePlatformStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.apiId = config['demand_plan_api_id']
        self.appKey = config['demand_plan_app_key']
        self.appSecret = config['demand_plan_app_secret']
        self.table_name = 'demand_plan'
        self.returnFields = ["product_no", "product_name", "product_type", "warehouse_code", "warehouse_name",
                             "z_year_month", "target_warehouse_code", "target_warehouse_name", "z_plan_type", "z_weight",
                             "deletion_flag", "create_user", "create_time", "update_user", "update_time", "ds"]


class WarehouseTruckingCosts(YiliMiddlePlatformStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.apiId = config['warehouse_trucking_cost_api_id']
        self.appKey = config['warehouse_trucking_cost_app_key']
        self.appSecret = config['warehouse_trucking_cost_app_secret']
        self.table_name = 'warehouse_trucking_cost'
        self.returnFields = ["year", "month", "carrier", "ship_method", "inv_org_code", "sale_order_closed_time",
                             "daily_dispatch_order_quantity", "kan_level_interval", "shipment_id", "dealer_name", "quantity",
                             "convert_into_ton", "mileage", "cost_type", "contract_unit_price", "oil_price_linkage",
                             "settle_accounts_unit_price", "base_freight", "oil_unit_price", "surcharge",
                             "settle_accounts_amt", "veh_num", "prod_type", "domain_name", "line_id", "net_weight",
                             "dest_location_id", "prod_name", "sale_region", "ds"]


class DealerFactoryMappings(YiliMiddlePlatformStream):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.apiId = config['dealer_factory_mapping_api_id']
        self.appKey = config['dealer_factory_mapping_app_key']
        self.appSecret = config['dealer_factory_mapping_app_secret']
        self.table_name = 'dealer_factory_mapping'
        self.returnFields = ["dealer_name", "sale_area_lvl2_name", "sale_area_lvl3_name", "sale_area_lvl4_name",
                             "sale_area_lvl5_name", "daily_factory_name", "often_factory_name", "ds"]


# Source
class SourceYiliMiddlePlatform(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # NoAuth just means there is no authentication required for this API and is included for completeness.
        # Skip passing an authenticator if no authentication is required.
        # Other authenticators are available for API token-based auth and Oauth2.
        auth = NoAuth()
        return [
            ProductionPlans(authenticator=auth, config=config),
            DemandPlans(authenticator=auth, config=config),
            WarehouseTruckingCosts(authenticator=auth, config=config),
            DealerFactoryMappings(authenticator=auth, config=config)
        ]
