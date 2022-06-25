#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


import sys

from destination_convect_wayfair import DestinationConvectWayfair

if __name__ == "__main__":
    DestinationConvectWayfair().run(sys.argv[1:])
