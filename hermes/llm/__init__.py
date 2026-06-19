"""LLM clients for the HermesOverseer.

The overseer is provider-agnostic: it consumes any object exposing
`.chat(messages, images=...)` and never references a concrete client class.
Two transports ship here:

- `OpenAICompatibleLLM` — any backend speaking OpenAI's
  `/v1/chat/completions` (LM Studio, Ollama local, vLLM, llama.cpp, and the
  Gemini / Claude OpenAI-compatible shims).
- `OllamaCloudLLM` — api.ollama.com via the native `ollama` library, whose
  auth differs from the OpenAI-compatible endpoint.

`hermes.service1_agent.agent_construction._build_llm` is the single place that
maps an operator-selected provider to one of these.
"""
from .clients import LLMConnectionError, OllamaCloudLLM, OpenAICompatibleLLM

__all__ = ["OpenAICompatibleLLM", "OllamaCloudLLM", "LLMConnectionError"]
