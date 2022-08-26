#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

from airbyte_cdk.sources.streams.http.auth import HttpAuthenticator
import hashlib
from typing import Any, Mapping
import time

# https://open.shipout.com/portal/zh/documentation/introduction.html#%E9%89%B4%E6%9D%83


def get_sign(path, version, timestamp, app_secret):
    data = {"path": path, "version": version, "timestamp": timestamp}
    list_data = list(data.keys())
    string_sign = ""
    sort_list = sorted(list_data)
    for item in sort_list:
        string_sign += str(item)
        string_sign += str(data[item])
    string_sign += app_secret
    sign = hashlib.md5(string_sign.encode("utf-8")).hexdigest().upper()
    return sign


class ShipoutAuthenticator(HttpAuthenticator):
    version = "1.0.0"

    def __init__(self, config: Mapping[str, Any], path: str):
        cred = config["credentials"]
        self._token = cred["authorization"]
        self._app_key = cred["app_key"]
        self._app_secret = cred["app_secret"]
        self._path = path

    def get_auth_header(self) -> Mapping[str, Any]:
        timestamp = int(time.time())
        path = self._path
        if not path.startswith("/"):
            path = "/" + path
        headers = {
            "Authorization": f"{self._token}",
            "timestamp": f"{timestamp}",
            "appKey": self._app_key,
            "version": self.version,
            "sign": get_sign(path=path, version=self.version, timestamp=timestamp, app_secret=self._app_secret),
        }

        return headers