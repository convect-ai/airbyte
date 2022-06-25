#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from destination_convect_jushuitan import DestinationConvectJushuitan

if __name__ == "__main__":
    DestinationConvectJushuitan().run(sys.argv[1:])
