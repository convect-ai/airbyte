#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import pytest
from source_shipout.utils import get_sign


def test_sign():
    assert (
        get_sign(path="/http/order/findById", version="1.0.0", timestamp="1639795057257", app_secret="FC300595425B48C39FA3F56F3787D9CA")
        == "C9A285632306F49A2C9CBF7DCFE67231"
    )
