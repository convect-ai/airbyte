#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_wayfair import SourceWayfair

if __name__ == "__main__":
    source = SourceWayfair()
    launch(source, sys.argv[1:])
