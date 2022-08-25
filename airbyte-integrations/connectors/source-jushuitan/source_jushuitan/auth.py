#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import hashlib
from typing import Any, Dict


def get_sign(app_secret: str, data: Dict[str, Any]) -> str:
    """
    Sign the data using app secret.
    Code snippet copied from https://openweb.jushuitan.com/doc?docId=40
    """
    list_data = list(data.keys())
    string_sign = app_secret
    sort_list = sorted(list_data)

    for item in sort_list:
        if item == "sign":
            continue
        string_sign += str(item)
        string_sign += str(data[item])
    sign = hashlib.md5(string_sign.encode("utf-8")).hexdigest()
    return sign
