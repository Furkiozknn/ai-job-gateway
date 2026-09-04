from __future__ import annotations

from ai_job_gateway.providers import default_registry
from ai_job_gateway.providers_local import OPERATIONS, toolkit_available
from ai_job_gateway.providers_pollinations import PollinationsImageProvider


def test_default_registry_always_has_the_real_hosted_capability():
    registry = default_registry()
    assert isinstance(registry["generate-image"], PollinationsImageProvider)
    assert {"echo", "mock-generate"} <= set(registry)


def test_local_media_capabilities_appear_exactly_when_the_toolkit_is_installed():
    registry = default_registry()
    media = {k for k in registry if k.startswith("media-")}
    if toolkit_available():
        assert media == {f"media-{op}" for op in OPERATIONS}
    else:
        assert media == set()
