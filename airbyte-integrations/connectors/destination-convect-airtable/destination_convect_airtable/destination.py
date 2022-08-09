#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#

import logging

from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator
from .airtable import Airtable

logger = logging.getLogger("airbyte")

STREAM_TABLE_PREFIX = "odp_"


class DestinationConvectAirtable(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:
        auth = TokenAuthenticator(token=config["api_key"])

        for message in input_messages:
            if message.type == Type.STATE:
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                logger.info(f"Processing message from stream '{record.stream}': {record.data}")

                stream = record.stream
                source_table = stream[len(STREAM_TABLE_PREFIX):]
                config_table_name = "table_" + source_table

                if config_table_name in config and config[config_table_name]:
                    dest_table = config[config_table_name]
                    Airtable.write_record(auth, config["base_id"], dest_table, record.data)
            else:
                logger.info(f"Received message of unknown type: {message.__dict__}")

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        auth = TokenAuthenticator(token=config["api_key"])
        for key in config:
            if key.startswith("table_"):
                table = config[key]
                if table:
                    try:
                        Airtable.check_table_exists(auth, config["base_id"], table)
                    except Exception as e:
                        return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
        return AirbyteConnectionStatus(status=Status.SUCCEEDED)
