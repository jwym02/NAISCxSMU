"""Optional DB integration test for search_text / FTS (run with INTEGRATION_TESTS=1)."""
from __future__ import annotations

import asyncio
import os
import unittest


@unittest.skipUnless(
    os.environ.get("INTEGRATION_TESTS") == "1",
    "Set INTEGRATION_TESTS=1; requires TimescaleDB from docker-compose",
)
class SearchIntegrationTests(unittest.TestCase):
    def test_search_normalized_events_fts(self) -> None:
        async def _run():
            from app.shared.db import search_normalized_events_fts

            return await search_normalized_events_fts("machine", limit=5)

        rows = asyncio.run(_run())
        self.assertIsInstance(rows, list)


if __name__ == "__main__":
    unittest.main()
