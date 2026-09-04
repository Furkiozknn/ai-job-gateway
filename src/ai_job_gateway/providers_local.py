"""Local media operations as gateway capabilities, via mini-creative-toolkit.

Everything here runs on the gateway's own machine: resize, convert, strip
metadata, remove a background, upscale, optimise, inspect. No network, no
key. The heavy lifting lives in `mini-creative-toolkit`, which is an optional
extra (``pip install 'ai-job-gateway[media]'``); when it is not installed the
capabilities are simply not registered, and a provider constructed anyway
fails with an install hint rather than an ImportError from deep inside.

Params are passed to the toolkit function by keyword, so a caller uses the
toolkit's own argument names (``image_path``, ``width``, ``goal``...). Only
parameters the function actually declares are accepted; ``config`` is
reserved for the toolkit and refused. Path validation, size limits and the
``MCT_ALLOWED_ROOTS`` restriction are the toolkit's and apply to the gateway
process's environment - set them there.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import inspect
from typing import Any, Callable

from .providers import Provider, ProviderError

TOOLKIT_PACKAGE = "mini_creative_toolkit"
INSTALL_HINT = "install the 'media' extra: pip install 'ai-job-gateway[media]' (or uv sync --extra media)"

#: operation -> (module, function) inside mini-creative-toolkit.
OPERATIONS: dict[str, tuple[str, str]] = {
    "resize": ("mini_creative_toolkit.tools.image", "resize_image"),
    "convert": ("mini_creative_toolkit.tools.image", "convert_format"),
    "strip-metadata": ("mini_creative_toolkit.tools.image", "strip_metadata"),
    "watermark": ("mini_creative_toolkit.tools.image", "add_watermark"),
    "remove-background": ("mini_creative_toolkit.tools.background", "remove_background"),
    "upscale": ("mini_creative_toolkit.tools.upscale", "upscale_image_auto"),
    "optimize": ("mini_creative_toolkit.tools.optimize", "optimize_media"),
    "inspect": ("mini_creative_toolkit.tools.inspect", "inspect_media"),
    "thumbnail": ("mini_creative_toolkit.tools.video", "video_thumbnail"),
    "extract-audio": ("mini_creative_toolkit.tools.video", "extract_audio"),
}

_RESERVED_PARAMS = {"config"}


def toolkit_available() -> bool:
    return importlib.util.find_spec(TOOLKIT_PACKAGE) is not None


class LocalMediaProvider(Provider):
    """One toolkit operation exposed as one capability."""

    name = "local-media"

    def __init__(self, operation: str) -> None:
        if operation not in OPERATIONS:
            raise ValueError(f"unknown local media operation {operation!r}; known: {', '.join(sorted(OPERATIONS))}")
        self.operation = operation
        self.name = f"local-media:{operation}"
        self._func: Callable[..., Any] | None = None
        self._allowed: frozenset[str] = frozenset()
        self._toolkit_error: type[Exception] | None = None

    def _resolve(self) -> Callable[..., Any]:
        if self._func is not None:
            return self._func
        module_name, func_name = OPERATIONS[self.operation]
        try:
            module = importlib.import_module(module_name)
            errors = importlib.import_module("mini_creative_toolkit.errors")
        except ImportError as exc:
            raise ProviderError(f"mini-creative-toolkit is not installed ({exc}); {INSTALL_HINT}") from None
        func = getattr(module, func_name)
        self._allowed = frozenset(inspect.signature(func).parameters) - _RESERVED_PARAMS
        self._toolkit_error = errors.ToolkitError
        self._func = func
        return func

    async def run(self, job_id: str, params: dict) -> dict:
        func = self._resolve()
        unknown = sorted(set(params) - self._allowed)
        if unknown:
            raise ProviderError(
                f"{self.operation}: unknown parameter(s) {', '.join(unknown)}; "
                f"accepted: {', '.join(sorted(self._allowed))}"
            )
        assert self._toolkit_error is not None
        try:
            # The toolkit is synchronous (Pillow, ffmpeg, ONNX); a thread keeps
            # the event loop - and every other job's polling - responsive.
            result = await asyncio.to_thread(func, **params)
        except self._toolkit_error as exc:
            raise ProviderError(getattr(exc, "message", str(exc))) from None
        if isinstance(result, str):  # MCT_LEGACY_STRING_RESULTS=1
            result = {"output_path": result}
        return {**result, "provider": self.name, "operation": self.operation, "execution": "local"}


def local_media_registry(prefix: str = "media-") -> dict[str, Provider]:
    """``{"media-resize": ..., "media-upscale": ...}`` for every operation."""
    return {f"{prefix}{operation}": LocalMediaProvider(operation) for operation in OPERATIONS}
