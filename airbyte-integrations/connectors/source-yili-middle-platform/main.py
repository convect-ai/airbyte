#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_yili_middle_platform import SourceYiliMiddlePlatform

if __name__ == "__main__":
    source = SourceYiliMiddlePlatform()
    launch(source, sys.argv[1:])
