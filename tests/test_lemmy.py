from lemmyw04b6eb792ca4a1 import query
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
    ExternalParentId,
)
import pytest

import logging
logging.basicConfig(level=logging.DEBUG)

@pytest.mark.asyncio
async def test_query():
    params = {
        "max_oldness_seconds": 1200,
        "maximum_items_to_collect": 5,
        "min_post_length": 10
    }
    async for item in query(params):
        print(item)
        assert isinstance(item, Item)

import asyncio
asyncio.run(test_query())