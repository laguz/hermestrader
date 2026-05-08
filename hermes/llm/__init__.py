"""Local-model LLM clients for the HermesOverseer.

The overseer expects any object with `.chat(messages, images=...)`.
We ship one concrete implementation here — `OpenAICompatibleLLM` — which
talks to LM Studio, Ollama, vLLM, llama.cpp server, or any other backend
that exposes OpenAI's `/v1/chat/completions` endpoint.
"""
from .clients import OpenAICompatibleLLM, LLMConnectionError

__all__ = ["OpenAICompatibleLLM", "LLMConnectionError"]
