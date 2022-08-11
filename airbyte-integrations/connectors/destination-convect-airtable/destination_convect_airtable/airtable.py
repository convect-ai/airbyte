import requests
import logging
import json
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

logger = logging.getLogger("airbyte")


class Airtable:
    @staticmethod
    def check_table_exists(auth: TokenAuthenticator, base_id: str, table: str) -> None:
        url = f"https://api.airtable.com/v0/{base_id}/{table}?pageSize=1"
        try:
            response = requests.get(url, headers=auth.get_auth_header())
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise Exception("Invalid API key")
            elif e.response.status_code == 404:
                raise Exception(f"Table '{table}' not found")
            else:
                raise Exception(f"Error getting table {table}: {e}")


    @staticmethod
    def write_record(auth: TokenAuthenticator, base_id: str, table: str, record: object) -> None:
        url = f"https://api.airtable.com/v0/{base_id}/{table}"
        body = {
            "records": [
                {
                    "fields": record
                }
            ]
        }
        try:
            response = requests.post(url, headers=auth.get_auth_header(), json=body)
            logger.info(f"Airtable response: {response}")
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error(f"Error while posting to {url}: {json.dumps(body)}", e)
            raise e
