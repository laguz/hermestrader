"""Tradier MCP server.

``server.py`` exposes the ``TradierBroker`` surface as a Model Context
Protocol server so any MCP client (Claude Desktop, Cowork, custom agents)
can call the broker over stdio. Independent of Service-1 / Service-2 —
this is for ad-hoc tooling, not the trading agent itself.

Run with: ``python -m hermes.mcp.server``
"""
