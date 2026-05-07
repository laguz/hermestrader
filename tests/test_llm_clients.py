import sys
from unittest.mock import MagicMock

# Mock dependencies before importing hermes.llm.clients
mock_requests = MagicMock()
sys.modules["requests"] = mock_requests
mock_ollama = MagicMock()
sys.modules["ollama"] = mock_ollama

import pytest
from hermes.llm.clients import _image_to_data_url, OllamaCloudLLM

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
