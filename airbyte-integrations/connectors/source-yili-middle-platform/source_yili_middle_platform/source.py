#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple


import requests
import json
import base64
import hmac
import hashlib
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

class YiliMiddlePlatform(HttpStream):
    url_base = 'https://datagrid-api.digitalyili.com'
    primary_key = None
    http_method = 'POST'

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__()
        self.host = 'datagrid-api.digitalyili.com'
        self.apiId = config['apiId']
        self.appKey = config['appKey']
        self.appSecret = config['appSecret']
        self.env = config['env']
        self.stage = config['stage']
        self.returnFields = config['returnFields']

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
            'Accept':'application/json;charset=utf-8',
            'X-Ca-Key':self.appKey,
            'Host':self.host,
            'X-Ca-Stage':self.stage,
            'X-Ca-Signature-Headers':'x-ca-key,x-ca-signature-method,x-ca-stage',
            'Content-Type':'application/octet-stream;charset=utf-8',
            'X-Ca-Signature-Method':'HmacSHA256',
        }
        body = json.dumps({"returnFields": self.returnFields})
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

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        # The response is a simple JSON whose schema matches our stream's schema exactly, 
        # so we just return a list containing the response
        return [response.json()]

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination, 
        # so we return None to indicate there are no more pages in the response
        return None
    
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

# Source
class SourceYiliMiddlePlatform(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # NoAuth just means there is no authentication required for this API and is included for completeness.
        # Skip passing an authenticator if no authentication is required.
        # Other authenticators are available for API token-based auth and Oauth2. 
        auth = NoAuth()
        return [YiliMiddlePlatform(authenticator=auth, config=config)]
