import sys
from unittest.mock import MagicMock

# Mock dependencies before importing hermes.llm.clients
mock_requests = MagicMock()
sys.modules["requests"] = mock_requests
mock_ollama = MagicMock()
sys.modules["ollama"] = mock_ollama

import pytest
from hermes.llm.clients import _image_to_data_url, OllamaCloudLLM, OpenAICompatibleLLM

def test_image_to_data_url_none():
    assert _image_to_data_url(None) is None

def test_image_to_data_url_bytes():
    assert _image_to_data_url(b"abc") == "data:image/png;base64,YWJj"

def test_image_to_data_url_bytearray():
    assert _image_to_data_url(bytearray(b"abc")) == "data:image/png;base64,YWJj"

def test_image_to_data_url_http():
    url = "http://example.com/chart.png"
    assert _image_to_data_url(url) == url

def test_image_to_data_url_https():
    url = "https://example.com/chart.png"
    assert _image_to_data_url(url) == url

def test_image_to_data_url_data():
    url = "data:image/png;base64,YWJj"
    assert _image_to_data_url(url) == url

def test_image_to_data_url_str_unknown():
    assert _image_to_data_url("ftp://example.com/chart.png") is None
    assert _image_to_data_url("just a string") is None

def test_image_to_data_url_dict_b64_mime():
    img = {"b64": "YWJj", "mime": "image/jpeg"}
    assert _image_to_data_url(img) == "data:image/jpeg;base64,YWJj"

def test_image_to_data_url_dict_base64_content_type():
    img = {"base64": "YWJj", "content_type": "image/gif"}
    assert _image_to_data_url(img) == "data:image/gif;base64,YWJj"

def test_image_to_data_url_dict_default_mime():
    img = {"b64": "YWJj"}
    assert _image_to_data_url(img) == "data:image/png;base64,YWJj"

def test_image_to_data_url_dict_url():
    img = {"url": "http://example.com/chart.png"}
    assert _image_to_data_url(img) == "http://example.com/chart.png"

def test_image_to_data_url_dict_empty():
    assert _image_to_data_url({}) is None
    assert _image_to_data_url({"other": "field"}) is None

def test_image_to_data_url_invalid_type():
    assert _image_to_data_url(123) is None
    assert _image_to_data_url([]) is None

# Tests for OllamaCloudLLM._images_to_ollama
def test_images_to_ollama_empty():
    assert OllamaCloudLLM._images_to_ollama(None) == []
    assert OllamaCloudLLM._images_to_ollama([]) == []

def test_images_to_ollama_list_with_none():
    assert OllamaCloudLLM._images_to_ollama([None, b"abc"]) == ["YWJj"]

def test_images_to_ollama_bytes_bytearray():
    assert OllamaCloudLLM._images_to_ollama([b"abc", bytearray(b"def")]) == ["YWJj", "ZGVm"]

def test_images_to_ollama_data_url():
    # data:<mime>;base64,<b64data>
    assert OllamaCloudLLM._images_to_ollama(["data:image/png;base64,YWJj"]) == ["YWJj"]
    # Test IndexError catch
    assert OllamaCloudLLM._images_to_ollama(["data:image/png;base64"]) == []

def test_images_to_ollama_str_pass_through():
    assert OllamaCloudLLM._images_to_ollama(["YWJj", "http://example.com/chart.png"]) == ["YWJj", "http://example.com/chart.png"]

def test_images_to_ollama_dict():
    imgs = [
        {"b64": "YWJj"},
        {"base64": "ZGVm"},
        {"other": "nothing"}
    ]
    assert OllamaCloudLLM._images_to_ollama(imgs) == ["YWJj", "ZGVm"]

# Tests for OpenAICompatibleLLM._attach_images
def test_openai_attach_images_empty_images():
    llm = OpenAICompatibleLLM("http://localhost:1234", "test-model")
    messages = [{"role": "user", "content": "hello"}]
    # Test with empty list
    result = llm._attach_images(messages, [])
    assert result == messages
    # Verify it returns a new list (though the plan doesn't strictly require this, it's good practice)
    assert result is not messages

def test_openai_attach_images_invalid_images():
    llm = OpenAICompatibleLLM("http://localhost:1234", "test-model")
    messages = [{"role": "user", "content": "hello"}]
    # Test with items that _image_to_data_url returns None for
    result = llm._attach_images(messages, [None, 123, "not a url"])
    assert result == messages

def test_openai_attach_images_no_user_message():
    llm = OpenAICompatibleLLM("http://localhost:1234", "test-model")
    images = [b"abc"]

    # Case 1: Empty messages
    result = llm._attach_images([], images)
    assert len(result) == 1
    assert result[0]["role"] == "user"
    assert result[0]["content"] == [{"type": "image_url", "image_url": {"url": "data:image/png;base64,YWJj"}}]

    # Case 2: Only system message
    messages = [{"role": "system", "content": "you are a bot"}]
    result = llm._attach_images(messages, images)
    assert len(result) == 2
    assert result[0] == messages[0]
    assert result[1]["role"] == "user"
    assert result[1]["content"] == [{"type": "image_url", "image_url": {"url": "data:image/png;base64,YWJj"}}]

def test_openai_attach_images_to_string_content():
    llm = OpenAICompatibleLLM("http://localhost:1234", "test-model")
    messages = [{"role": "user", "content": "describe this"}]
    images = [b"abc"]
    result = llm._attach_images(messages, images)
    assert len(result) == 1
    assert result[0]["role"] == "user"
    assert result[0]["content"] == [
        {"type": "text", "text": "describe this"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,YWJj"}}
    ]

def test_openai_attach_images_to_parts_content():
    llm = OpenAICompatibleLLM("http://localhost:1234", "test-model")
    messages = [{
        "role": "user",
        "content": [{"type": "text", "text": "existing text"}]
    }]
    images = [b"abc"]
    result = llm._attach_images(messages, images)
    assert len(result) == 1
    assert result[0]["content"] == [
        {"type": "text", "text": "existing text"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,YWJj"}}
    ]

def test_openai_attach_images_multiple_images():
    llm = OpenAICompatibleLLM("http://localhost:1234", "test-model")
    messages = [{"role": "user", "content": "two charts"}]
    images = [b"abc", b"def"]
    result = llm._attach_images(messages, images)
    assert len(result) == 1
    assert len(result[0]["content"]) == 3 # 1 text + 2 images
    assert result[0]["content"][1]["image_url"]["url"] == "data:image/png;base64,YWJj"
    assert result[0]["content"][2]["image_url"]["url"] == "data:image/png;base64,ZGVm"
