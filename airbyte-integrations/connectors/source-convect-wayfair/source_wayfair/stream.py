import logging
import vcr
import vcr.cassette as Cassette
from gql.transport.exceptions import TransportError, TransportProtocolError, TransportServerError, TransportQueryError
import os
import requests
from requests.exceptions import HTTPError
from airbyte_cdk.models import SyncMode
from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Union
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.sources.streams.http.rate_limiting import default_backoff_handler, user_defined_backoff_handler
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException, UserDefinedBackoffException
from requests.auth import AuthBase
from airbyte_cdk.sources.streams.http import HttpStream

logging.getLogger("vcr").setLevel(logging.ERROR)


class GraphqlStream(HttpStream, ABC):

    def __init__(self, authenticator: AuthBase = None):
        super().__init__(authenticator)

    @abstractmethod
    def graphql_next_page_token(self, current_page_token: Mapping[str, Any], response: requests.Response) -> Optional[Mapping[str, Any]]:
        """

        """

    def path(
            self,
            *,
            stream_state: Mapping[str, Any] = None,
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        """
        Returns the URL path for the API endpoint e.g: if you wanted to hit https://myapi.com/v1/some_entity then this should return "some_entity"
        """
        return ""

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    @abstractmethod
    def graphql_query(self,
                      stream_state: Mapping[str, Any],
                      stream_slice: Mapping[str, Any] = None,
                      next_page_token: Mapping[str, Any] = None,
                      ) -> str:
        """

        """

    @abstractmethod
    def graphql_variables(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> str:
        """

        """

    def _create_graphql_request(self, headers, graphql_query, graphsql_variables, params: Mapping = None
                                ) -> requests.PreparedRequest:

        graphql_payload = '''{"query": "%s","variables": %s}''' % (graphql_query, graphsql_variables)
        args = {
            "method": "POST",
            "url": self.url_base,
            "headers": headers,
            "data": graphql_payload,
            "params": params,
        }
        return self._session.prepare_request(requests.Request(**args))

    def read_records(
            self,
            sync_mode: SyncMode,
            cursor_field: List[str] = None,
            stream_slice: Mapping[str, Any] = None,
            stream_state: Mapping[str, Any] = None,
    ) -> Iterable[Mapping[str, Any]]:
        stream_state = stream_state or {}
        pagination_complete = False

        next_page_token = None
        while not pagination_complete:
            request = self._create_graphql_request(
                headers=self.request_headers(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
                graphql_query=self.graphql_query(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
                graphsql_variables=self.graphql_variables(stream_state=stream_state, stream_slice=stream_slice,
                                                            next_page_token=next_page_token),
                params=self.request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token),
            )

            request_kwargs = self.request_kwargs(stream_state=stream_state, stream_slice=stream_slice,
                                                    next_page_token=next_page_token)
            if self.use_cache:
                # use context manager to handle and store cassette metadata
                with self.cache_file as cass:
                    self.cassete = cass
                    # vcr tries to find records based on the request, if such records exist, return from cache file
                    # else make a request and save record in cache file
                    response = self._send_request(request, request_kwargs)

            else:
                response = self._send_request(request, request_kwargs)
            yield from self.parse_response(response, stream_state=stream_state, stream_slice=stream_slice)

            next_page_token = self.graphql_next_page_token(next_page_token, response)
            if not next_page_token:
                pagination_complete = True

        # Always return an empty generator just in case no records were ever yielded
        yield from []
