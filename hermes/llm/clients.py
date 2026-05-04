"""
LLM clients used by the HermesOverseer.

Two implementations:
  - OpenAICompatibleLLM  — for LM Studio, Ollama local, vLLM, llama.cpp, OpenAI
  - OllamaCloudLLM       — for api.ollama.com using the native ollama Python library
                           (the OpenAI-compatible endpoint on Ollama Cloud uses
                            different auth than the native API, so we use the
                            official client instead)
"""
from __future__ import annotations

import base64
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests

logger = logging.getLogger("hermes.llm.openai_compat")


class LLMConnectionError(RuntimeError):
    """Raised when the configured local model can't be reached or rejects."""


def _image_to_data_url(img: Any) -> Optional[str]:
    """Coerce whatever the chart_provider returns into a `data:` URL.

    Accepts:
      - bytes / bytearray         → assumes PNG, base64-encodes
      - str starting with 'http'  → passed through (Tradier-hosted chart, etc.)
      - str starting with 'data:' → passed through unchanged
      - dict with 'b64'/'mime'    → flexible escape hatch for richer providers
    """
    if img is None:
        return None
    if isinstance(img, (bytes, bytearray)):
        b64 = base64.b64encode(bytes(img)).decode("ascii")
        return f"data:image/png;base64,{b64}"
    if isinstance(img, str):
        if img.startswith("http://") or img.startswith("https://") or img.startswith("data:"):
            return img
    if isinstance(img, dict):
        b64 = img.get("b64") or img.get("base64")
        mime = img.get("mime") or img.get("content_type") or "image/png"
        if b64:
            return f"data:{mime};base64,{b64}"
        url = img.get("url")
        if url:
            return url
    return None


class OpenAICompatibleLLM:
    """Minimal sync chat client. Matches the surface HermesOverseer calls."""

    def __init__(
        self,
        base_url: str,
        model: str,
        *,
        api_key: Optional[str] = None,
        temperature: float = 0.2,
        timeout_s: float = 60.0,
        max_tokens: Optional[int] = 1024,
    ):
        if not base_url or not model:
            raise ValueError("OpenAICompatibleLLM requires base_url and model")
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key or None
        self.temperature = float(temperature)
        self.timeout_s = float(timeout_s)
        self.max_tokens = max_tokens

    # ------------------------------------------------------------------ HTTP
    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        return h

    def _attach_images(
        self,
        messages: Sequence[Dict[str, Any]],
        images: Iterable[Any],
    ) -> List[Dict[str, Any]]:
        urls = [u for u in (_image_to_data_url(i) for i in images) if u]
        if not urls:
            return list(messages)
        # OpenAI's vision format wraps the user message content as an array of
        # parts. We attach to the LAST user message in the conversation.
        out = [dict(m) for m in messages]
        for i in range(len(out) - 1, -1, -1):
            if out[i].get("role") == "user":
                text = out[i].get("content") or ""
                if not isinstance(text, str):
                    # Already in parts form — append images to it.
                    parts = list(text)
                else:
                    parts = [{"type": "text", "text": text}]
                for url in urls:
                    parts.append({
                        "type": "image_url",
                        "image_url": {"url": url},
                    })
                out[i] = {"role": out[i].get("role", "user"), "content": parts}
                return out
        # No user message? Append a fresh one carrying the images alone.
        out.append({"role": "user",
                    "content": [{"type": "image_url", "image_url": {"url": u}} for u in urls]})
        return out

    # ---------------------------------------------------------------- Public
    def chat(self,
             messages: Sequence[Dict[str, Any]],
             images: Optional[Iterable[Any]] = None,
             *,
             max_tokens: Optional[int] = None,
             timeout_s: Optional[float] = None) -> str:
        """Run a chat completion. Returns the assistant's text content.

        `max_tokens` and `timeout_s` override the instance defaults for this
        single call — used by `ping()` to keep validation round-trips fast
        even when the configured chat timeout is generous.

        Raises `LLMConnectionError` for any transport / 4xx / 5xx failure so
        the overseer's `_safe_json` fallback can decide what to do.
        """
        body: Dict[str, Any] = {
            "model": self.model,
            "messages": self._attach_images(messages, images or []),
            "temperature": self.temperature,
        }
        effective_max = max_tokens if max_tokens is not None else self.max_tokens
        if effective_max is not None:
            body["max_tokens"] = effective_max

        url = f"{self.base_url}/chat/completions"
        try:
            r = requests.post(url, json=body, headers=self._headers(),
                              timeout=timeout_s if timeout_s is not None else self.timeout_s)
        except requests.RequestException as exc:
            raise LLMConnectionError(f"unreachable: {exc}") from exc

        if not r.ok:
            try:
                detail = r.json()
            except Exception:                                   # noqa: BLE001
                detail = r.text
            raise LLMConnectionError(
                f"{r.status_code} {r.reason} from {url}: {detail}"
            )

        try:
            data = r.json()
            return data["choices"][0]["message"]["content"]
        except Exception as exc:                                # noqa: BLE001
            raise LLMConnectionError(
                f"malformed completion response: {exc}; body={r.text[:400]!r}"
            ) from exc

    # --------------------------------------------------------------- Helpers
    def list_models(self, *, timeout_s: Optional[float] = None) -> List[Dict[str, Any]]:
        """List models the configured server is currently serving.

        Standard OpenAI shape: GET /models returns {data: [{id, ...}, ...]}.
        Used by the watcher's "Refresh models" button so the operator can
        pick a freshly-loaded model id instead of typing it by hand — e.g.
        when Nous Research ships a new Hermes revision and you load it in
        LM Studio, this populates the dropdown without a code change.
        """
        url = f"{self.base_url}/models"
        try:
            r = requests.get(url, headers=self._headers(),
                             timeout=timeout_s if timeout_s is not None else 15.0)
        except requests.RequestException as exc:
            raise LLMConnectionError(f"unreachable: {exc}") from exc
        if not r.ok:
            try:
                detail = r.json()
            except Exception:                                   # noqa: BLE001
                detail = r.text
            raise LLMConnectionError(
                f"{r.status_code} {r.reason} from {url}: {detail}"
            )
        try:
            data = r.json()
        except Exception as exc:                                # noqa: BLE001
            raise LLMConnectionError(
                f"malformed /models response: {exc}; body={r.text[:400]!r}"
            ) from exc
        # Some backends wrap the list in `data`, others return a bare list.
        items = data.get("data") if isinstance(data, dict) else data
        if not isinstance(items, list):
            return []
        return [
            {
                "id": (m.get("id") or "").strip(),
                "owned_by": m.get("owned_by"),
                "created": m.get("created"),
            }
            for m in items if isinstance(m, dict) and m.get("id")
        ]

    def ping(self, *, timeout_s: Optional[float] = None) -> Dict[str, Any]:
        """Cheap connectivity check used by /api/llm/test in the watcher.

        Sends a tiny prompt with max_tokens=32 so a healthy round-trip
        finishes in seconds. The instance's timeout still applies — LM
        Studio's cold-load of a fresh GGUF can take a long time on the
        first request, so the operator should give the test endpoint a
        generous timeout (120s+ is reasonable on consumer hardware).
        """
        reply = self.chat(
            [
                {"role": "system", "content": "Respond with exactly: OK"},
                {"role": "user",   "content": "ping"},
            ],
            images=None,
            max_tokens=32,
            timeout_s=timeout_s,
        )
        return {
            "ok": True,
            "base_url": self.base_url,
            "model": self.model,
            "reply": (reply or "").strip()[:200],
        }


class OllamaCloudLLM:
    """Native Ollama Cloud client using the official `ollama` Python library.

    Ollama Cloud (api.ollama.com) auth works differently from its
    OpenAI-compatible shim — the native client handles it correctly via a
    custom Authorization header, which is what the official docs show.
    """

    def __init__(
        self,
        model: str,
        api_key: str,
        *,
        temperature: float = 0.2,
        max_tokens: int = 1024,
        timeout_s: float = 120.0,
    ):
        if not model or not api_key:
            raise ValueError("OllamaCloudLLM requires model and api_key")
        self.model = model
        self.api_key = api_key
        self.temperature = float(temperature)
        self.max_tokens = int(max_tokens)
        self.timeout_s = float(timeout_s)

        try:
            from ollama import Client
            self._client = Client(
                host="https://api.ollama.com",
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
        except ImportError as exc:
            raise LLMConnectionError(
                "ollama package not installed — run: pip install ollama"
            ) from exc

    @staticmethod
    def _images_to_ollama(images: Optional[Iterable[Any]]) -> list:
        """Convert image payloads to the list of base64 strings ollama expects.

        The ollama Python library accepts images as raw bytes or base64-encoded
        strings attached to individual messages.  We extract base64 from whatever
        the chart_provider returns (bytes, data-URL, or dict with 'b64' key).
        """
        import base64 as _b64
        result = []
        for img in (images or []):
            if img is None:
                continue
            if isinstance(img, (bytes, bytearray)):
                result.append(_b64.b64encode(bytes(img)).decode("ascii"))
            elif isinstance(img, str):
                if img.startswith("data:"):
                    # data:<mime>;base64,<b64data>
                    try:
                        result.append(img.split(",", 1)[1])
                    except IndexError:
                        pass
                else:
                    # Assume it's already a raw base64 string or a URL — pass through
                    result.append(img)
            elif isinstance(img, dict):
                b64 = img.get("b64") or img.get("base64")
                if b64:
                    result.append(b64)
        return result

    def chat(
        self,
        messages: Sequence[Dict[str, Any]],
        images: Optional[Iterable[Any]] = None,
        *,
        max_tokens: Optional[int] = None,
        timeout_s: Optional[float] = None,
    ) -> str:
        """Send a chat request to Ollama Cloud. Returns the assistant text.

        When `images` is provided, the base64-encoded images are attached to
        the last user message so vision-capable models (llava, gemma3, etc.)
        can analyse them.
        """
        effective_max = max_tokens if max_tokens is not None else self.max_tokens
        ollama_images = self._images_to_ollama(images)

        # Build the message list, attaching images to the last user message.
        msg_list = [dict(m) for m in messages]
        if ollama_images:
            for i in range(len(msg_list) - 1, -1, -1):
                if msg_list[i].get("role") == "user":
                    msg_list[i]["images"] = ollama_images
                    break
            else:
                msg_list.append({"role": "user", "content": "", "images": ollama_images})

        try:
            response = self._client.chat(
                model=self.model,
                messages=msg_list,
                options={
                    "temperature": self.temperature,
                    "num_predict": effective_max,
                },
            )
            # ollama library returns an object; access like a dict or attribute
            if isinstance(response, dict):
                return response["message"]["content"]
            return response.message.content
        except Exception as exc:                                    # noqa: BLE001
            raise LLMConnectionError(f"Ollama Cloud error: {exc}") from exc

    def ping(self, *, timeout_s: Optional[float] = None) -> Dict[str, Any]:
        """Quick connectivity check."""
        reply = self.chat(
            [
                {"role": "system", "content": "Respond with exactly: OK"},
                {"role": "user",   "content": "ping"},
            ],
            max_tokens=32,
        )
        return {
            "ok": True,
            "base_url": "https://api.ollama.com",
            "model": self.model,
            "reply": (reply or "").strip()[:200],
        }

    def list_models(self, *, timeout_s: Optional[float] = None) -> List[Dict[str, Any]]:
        """List models available on Ollama Cloud for this key."""
        try:
            result = self._client.list()
            models = result.get("models") if isinstance(result, dict) else getattr(result, "models", [])
            return [
                {"id": getattr(m, "model", None) or m.get("model", ""), "owned_by": "ollama"}
                for m in (models or [])
            ]
        except Exception as exc:                                    # noqa: BLE001
            raise LLMConnectionError(f"list_models failed: {exc}") from exc
