"""A real, keyless, hosted image provider: Pollinations.ai.

This is the first provider in the gateway that does actual work, and the
only one that leaves the machine. Both facts are disclosed everywhere they
matter: in this module's name, in the provider's result payload, and in the
README. The prompt is user content that is sent to a third party; it is
never written to the log.

The response is never trusted. A 200 with ``Content-Type: text/html`` is an
error page, not an image; a correct content type can still front a truncated
body; a response with no length header can stream forever. All three are
handled before anything is written where a caller would find it.
"""

from __future__ import annotations

import logging
import os
import re
import urllib.parse
from pathlib import Path
from typing import Any

import httpx

from .providers import Provider, ProviderError

logger = logging.getLogger(__name__)

SERVICE = "Pollinations.ai"
BASE_URL = "https://image.pollinations.ai/prompt/"
ACCEPTED_CONTENT_TYPES = {"image/jpeg", "image/jpg", "image/png", "image/webp"}
MAX_PROMPT_LENGTH = 1000
MIN_DIMENSION, MAX_DIMENSION = 64, 2048
DEFAULT_MAX_DOWNLOAD_BYTES = 64 * 1024 * 1024

_SAFE_ID = re.compile(r"[^A-Za-z0-9_-]")


def default_output_dir() -> Path:
    """``GATEWAY_OUTPUT_DIR`` if set, else ``./output`` under the working dir."""
    raw = os.environ.get("GATEWAY_OUTPUT_DIR", "").strip()
    return Path(raw).expanduser() if raw else Path.cwd() / "output"


def _require_prompt(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProviderError("params.prompt must be a non-empty string")
    if len(value) > MAX_PROMPT_LENGTH:
        raise ProviderError(f"params.prompt must be at most {MAX_PROMPT_LENGTH} characters")
    if any(ord(c) < 32 or ord(c) == 127 for c in value):
        raise ProviderError("params.prompt must not contain control characters")
    return value


def _require_dimension(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        if isinstance(value, float) and value.is_integer():
            value = int(value)
        else:
            raise ProviderError(f"params.{field} must be an integer")
    if not MIN_DIMENSION <= value <= MAX_DIMENSION:
        raise ProviderError(f"params.{field} must be between {MIN_DIMENSION} and {MAX_DIMENSION}")
    return value


def _optional_seed(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value < 2**31:
        raise ProviderError("params.seed must be an integer between 0 and 2147483647")
    return value


def detect_image_format(body: bytes) -> str | None:
    """Format from magic bytes, or ``None``. A content type is a claim; this is evidence."""
    if body.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if body.startswith(b"\xff\xd8\xff"):
        return "jpg"
    if body[:4] == b"RIFF" and body[8:12] == b"WEBP":
        return "webp"
    return None


class PollinationsImageProvider(Provider):
    """Text-to-image via Pollinations.ai's public endpoint. Hosted; no key."""

    name = "pollinations"

    def __init__(
        self,
        *,
        output_dir: Path | str | None = None,
        timeout_seconds: float = 60.0,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.output_dir = Path(output_dir) if output_dir is not None else None
        self.timeout_seconds = timeout_seconds
        self.max_download_bytes = max_download_bytes
        self._client = client

    async def run(self, job_id: str, params: dict) -> dict:
        prompt = _require_prompt(params.get("prompt"))
        width = _require_dimension(params.get("width", 1024), "width")
        height = _require_dimension(params.get("height", 1024), "height")
        seed = _optional_seed(params.get("seed"))

        query: dict[str, Any] = {"width": width, "height": height, "nologo": "true"}
        if seed is not None:
            query["seed"] = seed
        url = BASE_URL + urllib.parse.quote(prompt, safe="")

        # Service and shape only. The prompt is user content that is already
        # leaving the machine once; it does not also need to land in a log.
        logger.info("generate-image: outbound request to %s (%dx%d) for job %s", SERVICE, width, height, job_id)
        body, fmt = await self._fetch(url, query)
        path = self._write(job_id, body, fmt)
        return {
            "output_path": str(path),
            "format": fmt,
            "bytes": len(body),
            "requested_width": width,
            "requested_height": height,
            "seed": seed,
            "execution": "hosted",
            "service": SERVICE,
            "disclosure": f"The prompt was sent to {SERVICE}, a third-party service. No API key was used.",
        }

    async def _fetch(self, url: str, query: dict[str, Any]) -> tuple[bytes, str]:
        owns = self._client is None
        client = self._client or httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True)
        try:
            try:
                async with client.stream("GET", url, params=query) as response:
                    self._check_status(response)
                    self._check_content_type(response)
                    body = await self._read_bounded(response)
            except httpx.TimeoutException:
                raise ProviderError(
                    f"{SERVICE} did not respond within {self.timeout_seconds:g}s; it is a free public "
                    f"endpoint with no availability guarantee - retry later"
                ) from None
            except httpx.HTTPError as exc:
                raise ProviderError(f"could not reach {SERVICE}: {type(exc).__name__}") from None
        finally:
            if owns:
                await client.aclose()

        fmt = detect_image_format(body)
        if fmt is None:
            raise ProviderError(
                f"{SERVICE} returned {len(body)} bytes that are not a PNG, JPEG or WebP image "
                f"(possibly a truncated transfer); nothing was written"
            )
        return body, fmt

    @staticmethod
    def _check_status(response: httpx.Response) -> None:
        code = response.status_code
        if code < 400:
            return
        if code in (401, 403):
            raise ProviderError(
                f"{SERVICE} returned HTTP {code}: the endpoint appears to require authentication now, "
                f"which it did not when this provider was written"
            )
        kind = "rejected the request" if code < 500 else "is failing"
        raise ProviderError(f"{SERVICE} {kind} (HTTP {code})")

    @staticmethod
    def _check_content_type(response: httpx.Response) -> None:
        media_type = response.headers.get("content-type", "").split(";")[0].strip().lower()
        if media_type not in ACCEPTED_CONTENT_TYPES:
            raise ProviderError(
                f"{SERVICE} returned Content-Type {media_type or '(none)'!r}, not an image - "
                f"usually an error page served with a success status; nothing was written"
            )

    async def _read_bounded(self, response: httpx.Response) -> bytes:
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > self.max_download_bytes:
                raise ProviderError(
                    f"{SERVICE} response exceeded the {self.max_download_bytes} byte download "
                    f"limit and was aborted; nothing was written"
                )
            chunks.append(chunk)
        if total == 0:
            raise ProviderError(f"{SERVICE} returned an empty body; nothing was written")
        return b"".join(chunks)

    def _write(self, job_id: str, body: bytes, fmt: str) -> Path:
        directory = self.output_dir or default_output_dir()
        directory.mkdir(parents=True, exist_ok=True)
        # Job ids are uuids, but this is the one place an id becomes a
        # filename, so it is reduced to a safe charset regardless of origin.
        safe = _SAFE_ID.sub("_", job_id)[:80] or "job"
        final = directory / f"{safe}.{fmt}"
        tmp = directory / f".{safe}.part"
        tmp.write_bytes(body)
        os.replace(tmp, final)
        return final
