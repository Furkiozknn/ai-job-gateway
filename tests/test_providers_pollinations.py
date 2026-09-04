"""The hosted provider, exercised entirely against mocked HTTP.

Nothing here reaches the network: every request is served by an
httpx.MockTransport. The provider's whole job is refusing to trust the
response, and each refusal is pinned.
"""

from __future__ import annotations

import logging
import struct
import zlib

import httpx
import pytest

from ai_job_gateway.providers import ProviderError
from ai_job_gateway.providers_pollinations import (
    PollinationsImageProvider,
    detect_image_format,
)


def _png(width: int = 4, height: int = 4) -> bytes:
    """A minimal valid PNG, built by hand so the test suite needs no Pillow."""
    def chunk(tag: bytes, data: bytes) -> bytes:
        return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    raw = b"".join(b"\x00" + b"\x10\x20\x30" * width for _ in range(height))
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b""))


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)


def _provider(tmp_path, handler, **kwargs) -> PollinationsImageProvider:
    return PollinationsImageProvider(output_dir=tmp_path / "out", client=_client(handler), **kwargs)


async def test_a_valid_png_is_written_and_described(tmp_path):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        return httpx.Response(200, content=_png(), headers={"content-type": "image/png"})

    result = await _provider(tmp_path, handler).run("job-1", {"prompt": "a red square", "width": 256, "height": 128, "seed": 7})
    assert result["format"] == "png"
    assert result["execution"] == "hosted"
    assert result["service"] == "Pollinations.ai"
    assert "third-party" in result["disclosure"]
    assert (tmp_path / "out" / "job-1.png").read_bytes() == _png()
    assert "image.pollinations.ai/prompt/a%20red%20square" in seen["url"]
    assert "width=256" in seen["url"] and "height=128" in seen["url"] and "seed=7" in seen["url"]


async def test_an_html_error_page_with_a_200_status_is_not_saved_as_an_image(tmp_path):
    def handler(request):
        return httpx.Response(200, content=b"<html>down for maintenance</html>", headers={"content-type": "text/html"})

    with pytest.raises(ProviderError, match="not an image"):
        await _provider(tmp_path, handler).run("job-1", {"prompt": "x"})
    assert not (tmp_path / "out").exists() or not list((tmp_path / "out").iterdir())


async def test_a_correct_content_type_with_a_non_image_body_is_refused(tmp_path):
    """A content type is a claim; the magic bytes are the evidence."""
    def handler(request):
        return httpx.Response(200, content=b"definitely not pixels", headers={"content-type": "image/png"})

    with pytest.raises(ProviderError, match="not a PNG, JPEG or WebP"):
        await _provider(tmp_path, handler).run("job-1", {"prompt": "x"})


@pytest.mark.parametrize("status,phrase", [(400, "rejected"), (404, "rejected"), (500, "is failing"), (503, "is failing")])
async def test_http_errors_are_reported_by_kind(tmp_path, status, phrase):
    def handler(request):
        return httpx.Response(status, content=b"nope")

    with pytest.raises(ProviderError, match=phrase):
        await _provider(tmp_path, handler).run("job-1", {"prompt": "x"})


@pytest.mark.parametrize("status", [401, 403])
async def test_an_auth_demand_is_called_out_specifically(tmp_path, status):
    def handler(request):
        return httpx.Response(status)

    with pytest.raises(ProviderError, match="require authentication"):
        await _provider(tmp_path, handler).run("job-1", {"prompt": "x"})


async def test_a_timeout_is_a_provider_error_not_a_traceback(tmp_path):
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    with pytest.raises(ProviderError, match="did not respond"):
        await _provider(tmp_path, handler).run("job-1", {"prompt": "x"})


async def test_a_connection_failure_names_the_service(tmp_path):
    def handler(request):
        raise httpx.ConnectError("no route", request=request)

    with pytest.raises(ProviderError, match="could not reach Pollinations.ai"):
        await _provider(tmp_path, handler).run("job-1", {"prompt": "x"})


async def test_an_oversized_body_is_aborted_and_nothing_is_written(tmp_path):
    def handler(request):
        return httpx.Response(200, content=_png() + b"\x00" * 5000, headers={"content-type": "image/png"})

    with pytest.raises(ProviderError, match="download limit"):
        await _provider(tmp_path, handler, max_download_bytes=1000).run("job-1", {"prompt": "x"})
    assert not list((tmp_path / "out").glob("*")) if (tmp_path / "out").exists() else True


async def test_an_empty_body_is_refused(tmp_path):
    def handler(request):
        return httpx.Response(200, content=b"", headers={"content-type": "image/png"})

    with pytest.raises(ProviderError, match="empty body"):
        await _provider(tmp_path, handler).run("job-1", {"prompt": "x"})


@pytest.mark.parametrize("params", [
    {}, {"prompt": ""}, {"prompt": "   "}, {"prompt": "x" * 1001}, {"prompt": "a\x1b[31mb"},
    {"prompt": "ok", "width": 10}, {"prompt": "ok", "height": 99999}, {"prompt": "ok", "width": "512"},
    {"prompt": "ok", "seed": -1}, {"prompt": "ok", "seed": 2**40},
])
async def test_invalid_params_never_reach_the_network(tmp_path, params):
    def handler(request):  # pragma: no cover - must not be called
        raise AssertionError("a request was made despite invalid params")

    with pytest.raises(ProviderError):
        await _provider(tmp_path, handler).run("job-1", params)


async def test_the_prompt_is_never_logged(tmp_path, caplog):
    def handler(request):
        return httpx.Response(200, content=_png(), headers={"content-type": "image/png"})

    secret = "codename bluebird"
    with caplog.at_level(logging.DEBUG):
        await _provider(tmp_path, handler).run("job-1", {"prompt": secret})
    assert secret not in caplog.text
    assert "Pollinations.ai" in caplog.text


async def test_a_hostile_job_id_cannot_escape_the_output_directory(tmp_path):
    def handler(request):
        return httpx.Response(200, content=_png(), headers={"content-type": "image/png"})

    result = await _provider(tmp_path, handler).run("../../../etc/evil", {"prompt": "x"})
    written = tmp_path / "out"
    assert (written / result["output_path"].split("/")[-1]).exists()
    assert str(written) in result["output_path"]
    assert ".." not in result["output_path"].split("/")[-1]


async def test_a_redirect_is_followed(tmp_path):
    def handler(request):
        if request.url.path.startswith("/prompt/"):
            return httpx.Response(302, headers={"location": "https://cdn.example/x.png"})
        return httpx.Response(200, content=_png(), headers={"content-type": "image/png"})

    assert (await _provider(tmp_path, handler).run("job-1", {"prompt": "x"}))["format"] == "png"


def test_format_detection_by_magic_bytes():
    assert detect_image_format(_png()) == "png"
    assert detect_image_format(b"\xff\xd8\xff\xe0" + b"\x00" * 10) == "jpg"
    assert detect_image_format(b"RIFF\x00\x00\x00\x00WEBPVP8 ") == "webp"
    assert detect_image_format(b"GIF89a") is None
    assert detect_image_format(b"") is None
