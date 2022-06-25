#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from pydantic import BaseModel, Field


class ParquetFormat(BaseModel):
    'This connector utilises <a href="https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html" target="_blank">PyArrow (Apache Arrow)</a> for Parquet parsing.'

    class Config:
        title = "parquet"

    filetype: str = Field(Config.title, const=True)

    buffer_size: int = Field(
        default=0,
        description="Perform read buffering when deserializing individual column chunks. By default every group column will be loaded fully to memory. This option can help to optimize a work with memory if your data is particularly wide or failing during detection of OOM errors.",
    )
    # here when pass format to the parquet_parser,we need to convert to List[str] format
    columns: str = Field(
        default="{}",
        description='JSON string in format {"stream_name": ["column1", "column2"]}. For each stream, if you only want to sync a subset of the columns from the file(s), add the columns you want here. Default behaviour is to sync all columns.',
    )

    batch_size: int = Field(
        default=64 * 1024,  # 64K records
        description="Maximum number of records per batch. Batches may be smaller if there aren’t enough rows in the file. This option can help to optimize a work with memory if your data is particularly wide or failing during detection of OOM errors.",
    )
