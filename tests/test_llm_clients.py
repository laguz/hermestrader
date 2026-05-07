import sys
from unittest.mock import MagicMock

# Mock requests and ollama before they are imported by hermes.llm.clients
mock_requests = MagicMock()
sys.modules["requests"] = mock_requests

mock_ollama = MagicMock()
sys.modules["ollama"] = mock_ollama

import pytest
from hermes.llm.clients import OpenAICompatibleLLM, OllamaCloudLLM, _image_to_data_url

def test_image_to_data_url():
    # bytes
    assert _image_to_data_url(b"hello") == "data:image/png;base64,aGVsbG8="
    # http URL
    assert _image_to_data_url("http://example.com/img.png") == "http://example.com/img.png"
    # data URL
    assert _image_to_data_url("data:image/png;base64,abc") == "data:image/png;base64,abc"
    # dict with b64
    assert _image_to_data_url({"b64": "abc", "mime": "image/jpeg"}) == "data:image/jpeg;base64,abc"
    # dict with url
    assert _image_to_data_url({"url": "http://example.com"}) == "http://example.com"
    # None
    assert _image_to_data_url(None) is None

def test_openai_compatible_llm_init():
    llm = OpenAICompatibleLLM(
        base_url="http://localhost:1234/",
        model="test-model",
        api_key="sk-test",
        temperature=0.5,
        timeout_s=30.0,
        max_tokens=512
    )
    assert llm.base_url == "http://localhost:1234"
    assert llm.model == "test-model"
    assert llm.api_key == "sk-test"
    assert llm.temperature == 0.5
    assert llm.timeout_s == 30.0
    assert llm.max_tokens == 512

def test_ollama_cloud_llm_init():
    llm = OllamaCloudLLM(
        model="ollama-model",
        api_key="ollama-key",
        temperature=0.3,
        max_tokens=256,
        timeout_s=45.0
    )
    assert llm.model == "ollama-model"
    assert llm.api_key == "ollama-key"
    assert llm.temperature == 0.3
    assert llm.max_tokens == 256
    assert llm.timeout_s == 45.0
    mock_ollama.Client.assert_called_with(
        host="https://api.ollama.com",
        headers={"Authorization": "Bearer ollama-key"}
    )

def test_openai_compatible_llm_none_handling():
    llm = OpenAICompatibleLLM(
        base_url="http://localhost:1234",
        model="test-model",
        temperature=None,
        timeout_s=None,
        max_tokens=None
    )
    assert llm.temperature == 0.2
    assert llm.timeout_s == 60.0
    assert llm.max_tokens == 1024

def test_ollama_cloud_llm_none_handling():
    llm = OllamaCloudLLM(
        model="ollama-model",
        api_key="ollama-key",
        temperature=None,
        max_tokens=None,
        timeout_s=None
    )
    assert llm.temperature == 0.2
    assert llm.max_tokens == 1024
    assert llm.timeout_s == 120.0

def test_openai_compatible_llm_headers():
    llm = OpenAICompatibleLLM("http://local", "model", api_key="test-key")
    headers = llm._headers()
    assert headers["Content-Type"] == "application/json"
    assert headers["Authorization"] == "Bearer test-key"

    llm_no_key = OpenAICompatibleLLM("http://local", "model", api_key=None)
    assert "Authorization" not in llm_no_key._headers()

def test_openai_compatible_llm_attach_images():
    llm = OpenAICompatibleLLM("http://local", "model")
    messages = [{"role": "user", "content": "hello"}]
    images = [b"imgdata"]

    out = llm._attach_images(messages, images)
    assert len(out) == 1
    assert out[0]["role"] == "user"
    assert isinstance(out[0]["content"], list)
    assert out[0]["content"][0]["type"] == "text"
    assert out[0]["content"][1]["type"] == "image_url"
    assert "data:image/png;base64," in out[0]["content"][1]["image_url"]["url"]

def test_ollama_cloud_llm_images_to_ollama():
    from hermes.llm.clients import OllamaCloudLLM
    images = [
        b"bytes",
        "data:image/png;base64,encoded",
        {"b64": "dict_b64"}
    ]
    out = OllamaCloudLLM._images_to_ollama(images)
    assert len(out) == 3
    assert out[0] == "Ynl0ZXM="
    assert out[1] == "encoded"
    assert out[2] == "dict_b64"

def test_openai_compatible_llm_chat():
    llm = OpenAICompatibleLLM("http://local", "model")

    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {
        "choices": [{"message": {"content": "assistant reply"}}]
    }
    mock_requests.post.return_value = mock_response

    reply = llm.chat([{"role": "user", "content": "hi"}])
    assert reply == "assistant reply"
    mock_requests.post.assert_called_once()

def test_openai_compatible_llm_list_models():
    llm = OpenAICompatibleLLM("http://local", "model")

    mock_response = MagicMock()
    mock_response.ok = True
    mock_response.json.return_value = {
        "data": [{"id": "model-1", "owned_by": "me", "created": 123}]
    }
    mock_requests.get.return_value = mock_response

    models = llm.list_models()
    assert len(models) == 1
    assert models[0]["id"] == "model-1"

def test_ollama_cloud_llm_chat():
    llm = OllamaCloudLLM("model", "key")

    # self._client is mock_ollama.Client() instance
    mock_client_inst = mock_ollama.Client.return_value
    mock_client_inst.chat.return_value = {
        "message": {"content": "ollama reply"}
    }

    reply = llm.chat([{"role": "user", "content": "hi"}])
    assert reply == "ollama reply"
    mock_client_inst.chat.assert_called_once()

def test_ollama_cloud_llm_list_models():
    llm = OllamaCloudLLM("model", "key")
    mock_client_inst = mock_ollama.Client.return_value

    mock_model = MagicMock()
    mock_model.model = "m1"
    mock_client_inst.list.return_value = {"models": [mock_model]}

    models = llm.list_models()
    assert len(models) == 1
    assert models[0]["id"] == "m1"
