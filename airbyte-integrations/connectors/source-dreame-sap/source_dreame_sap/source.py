#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import base64
import json
from abc import ABC
from datetime import datetime
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

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


class DreameSAPStream(HttpStream, ABC):
    url_base = 'http://dreamepodap01.dreame.tech:50000/'
    primary_key = None
    http_method = 'POST'
    input_name = None
    input_params = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        # url_base based on the env of the spec
        if config['env'] == 'prod':
            self.url_base = 'http://dreamepopap01.dreame.tech:50000/'
            self.host = 'dreamepopap01.dreame.tech:50000'
        if config['env'] == 'dev':
            self.url_base = 'http://dreamepodap01.dreame.tech:50000/'
            self.host = 'dreamepodap01.dreame.tech:50000'
        if config['env'] == 'uat':
            self.url_base = 'http://dreamepoqap01.dreame.tech:50000/'
            self.host = 'dreamepoqap01.dreame.tech:50000'

    def path(
            self,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f'/RESTAdapter/Ext_ERP002/'

    def request_headers(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> Mapping[str, Any]:
        user = 'PO_USER'
        password = 'zm123456'
        user_pass = f"{user}:{password}"
        user_pass_encoded = base64.b64encode(user_pass.encode()).decode()
        return {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + user_pass_encoded
        }

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def request_body_data(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Optional[Union[Mapping, str]]:
        input_params = self.input_params
        if next_page_token:
            input_params["IMPORT"].update({"IV_PAGENO": str(next_page_token["page"]), "IV_PAGESIZE": str(next_page_token["page_size"])})
        body = {"IS_INPUT": {
            "NAME": self.input_name,
            "RECEIVER": "SAP",
            "SENDER": "flow",
            "INPUT": json.dumps(input_params)
        }}
        return json.dumps(body)

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        result = response.json()
        output = json.loads(result['ES_OUTPUT']['OUTPUT'].replace('\n', '').replace('\r', '').replace('\t', ''))
        data = output['TABLES']['T_DATA']
        return data

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None


class IncrementalDreameSAPStream(DreameSAPStream, ABC):

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.page_size = 10000
        self.current_page = 1

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        result = response.json()
        output = json.loads(result['ES_OUTPUT']['OUTPUT'].replace('\n', '').replace('\r', '').replace('\t', ''))
        data = output['TABLES']['T_DATA']
        if len(data) < self.page_size:
            return None
        self.current_page += 1
        return {"page": self.current_page, "page_size": self.page_size}


class ChildStreamMixin:
    parent_stream_class: Optional[Any] = None

    def stream_slices(self, sync_mode, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        items = self.parent_stream_class(config=self.config).read_records(sync_mode=sync_mode)
        for item in items:
            yield {"parent_id": item["id"]}


class MaterialMasterData(DreameSAPStream):
    input_name = "ZFMMM_073"
    primary_key = "MATNR"

    def request_body_data(self, **kwargs) -> Optional[Union[Mapping, str]]:
        self.input_params = {
            "IMPORT": {"IV_DATUM": datetime.now().strftime('%Y%m%d')},
            "TABLES": {"T_WERKS": [{"LOW": "1100"}, {"LOW": "1101"}, {"LOW": "1102"}, {"LOW": "1111"}]}
        }
        return super().request_body_data(**kwargs)


class InventoryData(IncrementalDreameSAPStream):
    input_name = "ZFMMM_074"
    primary_key = ["MATNR", "WERKS"]

    def request_body_data(self, **kwargs) -> Optional[Union[Mapping, str]]:
        self.input_params = {
            "TABLES": {
                "T_WERKS": [
                    {"LOW": "1100"},
                    {"LOW": "1101"},
                    {"LOW": "1102"},
                    {"LOW": "1111"}
                ],
                "T_LGORT": [
                    {"LOW": "1014"},
                    {"LOW": "1015"},
                    {"LOW": "1016"},
                    {"LOW": "3007"},
                    {"LOW": "3018"},
                    {"LOW": "3029"},
                    {"LOW": "3000"},
                    {"LOW": "1017"},
                    {"LOW": "3000"},
                    {"LOW": "3050"},
                    {"LOW": "3002"},
                    {"LOW": "3069"},
                    {"LOW": "3055"}
                ]
            }
        }
        return super().request_body_data(**kwargs)


class PurchasingStrategy(DreameSAPStream):
    input_name = "ZFMMM_076"
    primary_key = ["WERKS", "MATNR"]

    def request_body_data(self, **kwargs) -> Optional[Union[Mapping, str]]:
        self.input_params = {"TABLES": {"T_WERKS": [{"LOW": "1100"}, {"LOW": "1101"}, {"LOW": "1102"}, {"LOW": "1111"}]}}
        return super().request_body_data(**kwargs)


class PurchaseOrder(DreameSAPStream):
    input_name = "ZFMMM_075"
    primary_key = ["EBELN", "EBELP"]

    def request_body_data(self, **kwargs) -> Optional[Union[Mapping, str]]:
        # IV_START today YYYY-MM-DD
        iv_start = datetime.now().strftime('%Y-%m-%d')
        # IV_END today + 2 years YYYY-MM-DD
        iv_end = (datetime.now().replace(year=datetime.now().year + 2)).strftime('%Y-%m-%d')
        # 省略实现细节，应根据具体API调整参数
        self.input_params = {"IMPORT": {"IV_START": iv_start, "IV_END": iv_end},
                             "TABLES": {"T_WERKS": [{"LOW": "1100"}, {"LOW": "1101"}, {"LOW": "1102"}, {"LOW": "1111"}]}}
        return super().request_body_data(**kwargs)


class Bom(DreameSAPStream):
    input_name = "ZFMPP_020"
    primary_key = ["WERKS", "MATNR", "IDNRK"]
    parent_stream_class = MaterialMasterData
    factories = ['1100', '1101', '1102', '1111']

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config)
        self.config = config

    def stream_slices(self, sync_mode, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        # 直接从父流读取记录，并应用筛选逻辑
        parent_stream = self.parent_stream_class(config=self.config)
        for item in parent_stream.read_records(sync_mode=sync_mode):
            if item.get('MTART') == 'Z001':
                # 为每个 iv_matnr 生成 4 个 slices，每个对应一个工厂
                for factory in self.factories:
                    yield {"parent_id": item["MATNR"], "factory": factory}

    def request_body_json(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> Optional[Mapping]:
        # 使用 stream_slice 中的 parent_id 作为 IV_MATNR
        iv_matnr = stream_slice["parent_id"] if stream_slice else None
        factory = stream_slice["factory"] if stream_slice else None
        self.input_params = {
            "IMPORT": {
                "IV_MATNR": iv_matnr,
                "IV_WERKS": factory
            }
        }
        return super().request_body_json(**kwargs)


# Source
class SourceDreameSAP(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # NoAuth just means there is no authentication required for this API and is included for completeness.
        # Skip passing an authenticator if no authentication is required.
        # Other authenticators are available for API token-based auth and Oauth2.
        auth = NoAuth()
        return [
            MaterialMasterData(authenticator=auth, config=config),
            InventoryData(authenticator=auth, config=config),
            PurchasingStrategy(authenticator=auth, config=config),
            PurchaseOrder(authenticator=auth, config=config),
            Bom(authenticator=auth, config=config)
        ]
