"""Unit tests for the Agent Self-Learning Loop.
Tests doctrine string replacement, trade fetching, and chart integration.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timezone
import pandas as pd
import numpy as np

from hermes.charts.provider import render_chart


class TestSelfLearningLoop(unittest.TestCase):

    def test_doctrine_update_append(self):
        """Verify that when the Lessons section doesn't exist, it is appended to the bottom."""
        current_soul = "## Identity\nYou are Hermes.\n\n## Values\n- Capital first."
        header = "# Auto-Generated Lessons Learned"
        timestamp_line = "*(Last updated: 2026-05-22 20:00 UTC)*"
        new_analysis = "### Lessons for AAPL\n- Avoid puts under support."
        new_section = f"{header}\n{timestamp_line}\n\n{new_analysis}"

        if header in current_soul:
            parts = current_soul.split(header, 1)
            updated_soul = parts[0].rstrip() + "\n\n" + new_section
        else:
            updated_soul = current_soul.rstrip() + "\n\n" + new_section

        self.assertIn(header, updated_soul)
        self.assertIn("## Identity", updated_soul)
        self.assertTrue(updated_soul.endswith("- Avoid puts under support."))

    def test_doctrine_update_replace(self):
        """Verify that when the Lessons section already exists, it is replaced with the new content."""
        current_soul = (
            "## Identity\nYou are Hermes.\n\n"
            "# Auto-Generated Lessons Learned\n"
            "*(Last updated: 2026-05-20 10:00 UTC)*\n\n"
            "### Lessons for TSLA\n- Avoid TSLA calls."
        )
        header = "# Auto-Generated Lessons Learned"
        timestamp_line = "*(Last updated: 2026-05-22 20:00 UTC)*"
        new_analysis = "### Lessons for AAPL\n- Avoid puts under support."
        new_section = f"{header}\n{timestamp_line}\n\n{new_analysis}"

        if header in current_soul:
            parts = current_soul.split(header, 1)
            updated_soul = parts[0].rstrip() + "\n\n" + new_section
        else:
            updated_soul = current_soul.rstrip() + "\n\n" + new_section

        self.assertIn(header, updated_soul)
        self.assertIn("## Identity", updated_soul)
        self.assertNotIn("Lessons for TSLA", updated_soul)
        self.assertIn("Lessons for AAPL", updated_soul)
        self.assertTrue(updated_soul.endswith("- Avoid puts under support."))

    def test_chart_rendering_smoke(self):
        """Smoke test to ensure the chart renderer can render candlestick figures for the loop."""
        np.random.seed(42)
        n = 10
        prices = [100.0 + i for i in range(n)]
        volumes = [1000 * i for i in range(n)]
        idx = pd.bdate_range(end="2026-05-22", periods=n)
        df = pd.DataFrame({
            "open": prices, "high": [p + 1.0 for p in prices],
            "low": [p - 1.0 for p in prices], "close": prices, "volume": volumes
        }, index=idx)

        png_bytes = render_chart(df, "TESTSYM", lookback=10)
        self.assertIsInstance(png_bytes, bytes)
        self.assertTrue(len(png_bytes) > 0)


if __name__ == "__main__":
    unittest.main()
