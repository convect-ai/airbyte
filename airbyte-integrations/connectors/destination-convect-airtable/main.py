#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import sys

from destination_convect_airtable import DestinationConvectAirtable

if __name__ == "__main__":
    DestinationConvectAirtable().run(sys.argv[1:])
