#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

URL_MAP = {
    "Sandbox": {
        "auth": "https://sso.auth.wayfair.com/oauth/token",
        "api": "https://sandbox.api.wayfair.com/v1/graphql",
        "audience": "https://api.wayfair.com/",
        "check": "https://sandbox.api.wayfair.com/v1/demo/clock",
    },
    "Production": {
        "auth": "https://sso.auth.wayfair.com/oauth/token",
        "api": "https://api.wayfair.com/v1/graphql",
        "audience": "https://sandbox.api.wayfair.com/",
        "check": "https://api.wayfair.com/v1/demo/clock",
    },
}

from .source import SourceWayfair

__all__ = ["SourceWayfair"]
