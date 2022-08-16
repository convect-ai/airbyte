from airbyte_cdk.sources.streams.http.requests_native_auth.oauth import Oauth2Authenticator
from typing import Any, List, Mapping, MutableMapping, Tuple
from . import URL_MAP
import json
import requests


class WayfairAuthenticator(Oauth2Authenticator):
    def __init__(self, config: Mapping[str, Any]):

        self.config = config
        credentials = self.config.get("credentials", None)
        auth_url = URL_MAP[self.config.get("environment")]["auth"]
        audience_url = URL_MAP[self.config.get("environment")]["audience"]
        assert auth_url is not None
        assert audience_url is not None
        assert credentials is not None
        client_id = credentials.get("client_id",None)
        client_secret = credentials.get("client_secret", None)
        assert client_id is not None
        assert client_secret is not None
        self.audience = audience_url
        super().__init__(token_refresh_endpoint=auth_url,
                         client_id=client_id,
                         client_secret=client_secret,
                         refresh_token='')

    def get_auth_header(self) -> Mapping[str, Any]:
        return {
            "authorization": f"Bearer {self.get_access_token()}"
        }

    def get_refresh_request_body(self) -> Mapping[str, Any]:
        """Override to define additional parameters"""
        payload: MutableMapping[str, Any] = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "audience": self.audience,
        }

        if self.scopes:
            payload["scopes"] = self.scopes

        return payload

    def refresh_access_token(self) -> Tuple[str, int]:
        """
        returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        try:
            response = requests.request(method="POST", url=self.token_refresh_endpoint,
                                        data=json.dumps(self.get_refresh_request_body()),
                                        headers={
                                            'content-type': 'application/json',
                                            'cache-control': 'no-cache',
                                        })
            response.raise_for_status()
            response_json = response.json()
            return response_json[self.access_token_name], response_json[self.expires_in_name]
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e
