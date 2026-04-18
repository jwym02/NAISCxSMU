"""Tests for NL→SQL guardrails."""
from __future__ import annotations

import unittest

from fastapi import HTTPException

from app.query.sql_validate import validate_generated_sql


class SqlValidateTests(unittest.TestCase):
    def test_allows_normalized_events(self) -> None:
        validate_generated_sql(
            "SELECT * FROM normalized_events WHERE timestamp > NOW() - INTERVAL '1 day' LIMIT 10"
        )

    def test_rejects_multi_statement(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            validate_generated_sql("SELECT 1; DROP TABLE normalized_events")
        self.assertEqual(ctx.exception.status_code, 400)

    def test_rejects_bad_table(self) -> None:
        with self.assertRaises(HTTPException) as ctx:
            validate_generated_sql("SELECT * FROM pg_user LIMIT 1")
        self.assertIn("disallowed", str(ctx.exception.detail).lower())


if __name__ == "__main__":
    unittest.main()
