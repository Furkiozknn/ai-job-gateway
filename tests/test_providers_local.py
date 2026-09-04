"""Local media capabilities. Real toolkit when installed; the install-hint
path is tested without it."""

from __future__ import annotations

import importlib

import pytest

from ai_job_gateway.providers import ProviderError
from ai_job_gateway.providers_local import (
    OPERATIONS,
    LocalMediaProvider,
    local_media_registry,
    toolkit_available,
)


def test_unknown_operation_is_rejected_at_construction():
    with pytest.raises(ValueError, match="unknown local media operation"):
        LocalMediaProvider("enhance")


def test_registry_covers_every_operation_with_the_prefix():
    registry = local_media_registry()
    assert set(registry) == {f"media-{op}" for op in OPERATIONS}
    assert registry["media-resize"].name == "local-media:resize"


async def test_missing_toolkit_gives_an_install_hint_not_an_import_error(monkeypatch):
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name.startswith("mini_creative_toolkit"):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    with pytest.raises(ProviderError, match="ai-job-gateway\\[media\\]"):
        await LocalMediaProvider("resize").run("job-1", {"image_path": "/x.png", "width": 1, "height": 1})


# --- with the real toolkit -------------------------------------------------

needs_toolkit = pytest.mark.skipif(not toolkit_available(), reason="mini-creative-toolkit is not installed (media extra)")


@pytest.fixture
def toolkit_env(tmp_path, monkeypatch):
    monkeypatch.setenv("MCT_OUTPUT_DIR", str(tmp_path / "mct-out"))
    from mini_creative_toolkit.config import reset_config

    reset_config()
    yield tmp_path
    reset_config()


@pytest.fixture
def png(toolkit_env):
    from PIL import Image

    path = toolkit_env / "in.png"
    Image.new("RGB", (120, 60), (10, 200, 90)).save(path)
    return path


@needs_toolkit
async def test_resize_runs_locally_and_returns_the_toolkits_structured_result(png, toolkit_env):
    result = await LocalMediaProvider("resize").run("job-1", {"image_path": str(png), "width": 40, "height": 40})
    assert result["execution"] == "local"
    assert result["operation"] == "resize"
    assert (result["actual_width"], result["actual_height"]) == (40, 20)
    assert result["output_path"].startswith(str(toolkit_env / "mct-out"))


@needs_toolkit
async def test_inspect_needs_no_output_and_reports_the_kind(png):
    result = await LocalMediaProvider("inspect").run("job-1", {"path": str(png)})
    assert result["kind"] == "image" and result["width"] == 120


@needs_toolkit
async def test_toolkit_errors_become_provider_errors_with_the_message(toolkit_env):
    with pytest.raises(ProviderError, match="No such file"):
        await LocalMediaProvider("resize").run("job-1", {"image_path": str(toolkit_env / "absent.png"), "width": 4, "height": 4})


@needs_toolkit
async def test_parameters_the_tool_does_not_declare_are_refused(png):
    with pytest.raises(ProviderError, match="unknown parameter"):
        await LocalMediaProvider("resize").run("job-1", {"image_path": str(png), "width": 4, "height": 4, "shell": "rm -rf /"})


@needs_toolkit
async def test_the_reserved_config_parameter_cannot_be_injected(png):
    with pytest.raises(ProviderError, match="unknown parameter"):
        await LocalMediaProvider("resize").run("job-1", {"image_path": str(png), "width": 4, "height": 4, "config": {}})


@needs_toolkit
async def test_a_local_job_runs_end_to_end_through_the_manager(png, toolkit_env):
    import asyncio

    from ai_job_gateway.manager import JobManager
    from ai_job_gateway.models import JobStatus
    from ai_job_gateway.store import InMemoryJobStore

    store = InMemoryJobStore()
    manager = JobManager(store, {"media-resize": LocalMediaProvider("resize")})
    record = await manager.submit("media-resize", {"image_path": str(png), "width": 30, "height": 30})
    for _ in range(100):
        record = await store.get(record.id)
        if record.status in (JobStatus.READY, JobStatus.ERROR):
            break
        await asyncio.sleep(0.05)
    assert record.status == JobStatus.READY, record.error
    assert record.result["actual_width"] == 30
