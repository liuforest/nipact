from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from nipact.errors import ValidationError
from nipact.hashing import sha256_digest
from nipact.source_authority import (
    LogicalSourceCoordinate,
    ObservedSourceAuthority,
    SourceDeclaration,
    SourceOccurrenceGuard,
    canonical_logical_source_coordinate_json,
    declared_source_extension,
    logical_source_coordinate_payload,
    observe_source_authority,
    registered_source_authority_from_facts,
)


def _entity_coordinate() -> LogicalSourceCoordinate:
    return LogicalSourceCoordinate(
        context="clms",
        scope="entity",
        source_name="t1_image",
        entity_id="aac_027_m00",
    )


def _global_coordinate() -> LogicalSourceCoordinate:
    return LogicalSourceCoordinate(
        context="clms",
        scope="global",
        source_name="atlas",
        entity_id=None,
    )


def _declaration(
    *,
    path: str = "data/aac_027_m00/t1w.nii.gz",
    extension: str = ".nii.gz",
) -> SourceDeclaration:
    return SourceDeclaration(
        coordinate=_entity_coordinate(),
        declared_path=path,
        declared_extension=extension,
    )


def _write_source(runtime_root: Path, content: bytes = b"source-v1") -> Path:
    path = runtime_root / "data/aac_027_m00/t1w.nii.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def _observe_new(runtime_root: Path) -> ObservedSourceAuthority:
    return observe_source_authority(
        runtime_root=runtime_root,
        declaration=_declaration(),
        registered=None,
    )


def test_logical_source_coordinates_validate_scope_and_identity() -> None:
    entity = _entity_coordinate()
    global_source = _global_coordinate()

    assert entity.entity_id == "aac_027_m00"
    assert global_source.entity_id is None
    assert len({entity, replace(entity), global_source}) == 2

    with pytest.raises(FrozenInstanceError):
        entity.source_name = "other"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"scope": "project"}, "scope"),
        ({"scope": []}, "scope"),
        ({"scope": "global", "entity_id": "aac_027_m00"}, "global"),
        ({"scope": "entity", "entity_id": None}, "entity"),
        ({"context": "bad/context"}, "context"),
        ({"source_name": "bad source"}, "source name"),
        ({"entity_id": "../bad"}, "entity_id"),
    ],
)
def test_logical_source_coordinate_rejects_invalid_fields(
    kwargs: dict[str, object],
    message: str,
) -> None:
    values: dict[str, object] = {
        "context": "clms",
        "scope": "entity",
        "source_name": "t1_image",
        "entity_id": "aac_027_m00",
    }
    values.update(kwargs)

    with pytest.raises(ValidationError, match=message):
        LogicalSourceCoordinate(**values)  # type: ignore[arg-type]


def test_source_name_is_independent_from_callable_input_role() -> None:
    coordinate = _entity_coordinate()
    consuming_input_role = "anatomical_image"

    assert coordinate.source_name == "t1_image"
    assert coordinate.source_name != consuming_input_role


def test_logical_source_coordinate_has_canonical_payload_and_json() -> None:
    entity_payload = {
        "context": "clms",
        "scope": "entity",
        "source_name": "t1_image",
        "entity_id": "aac_027_m00",
    }
    global_payload = {
        "context": "clms",
        "scope": "global",
        "source_name": "atlas",
        "entity_id": None,
    }

    assert logical_source_coordinate_payload(_entity_coordinate()) == entity_payload
    assert logical_source_coordinate_payload(_global_coordinate()) == global_payload
    assert canonical_logical_source_coordinate_json(_entity_coordinate()) == (
        '{"context":"clms","entity_id":"aac_027_m00","scope":"entity",'
        '"source_name":"t1_image"}'
    )
    assert json.loads(
        canonical_logical_source_coordinate_json(_global_coordinate())
    ) == global_payload


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("data/source.json", ".json"),
        ("data/table.csv", ".csv"),
        ("data/aac_027_m00/t1w.nii.gz", ".nii.gz"),
        ("data/archive.tar.gz", ".gz"),
    ],
)
def test_declared_source_extension_preserves_current_compound_rule(
    path: str,
    expected: str,
) -> None:
    assert declared_source_extension(path) == expected


@pytest.mark.parametrize(
    "path",
    [
        "/data/source.json",
        "data/../source.json",
        "data/./source.json",
        "data//source.json",
        "data\\source.json",
        "other/source.json",
        "data/*.json",
        "data/source",
    ],
)
def test_source_declaration_rejects_invalid_runtime_paths(path: str) -> None:
    with pytest.raises(ValidationError):
        SourceDeclaration(
            coordinate=_entity_coordinate(),
            declared_path=path,
            declared_extension=".json",
        )


@pytest.mark.parametrize("extension", ["nii.gz", ".", "..", ".bad/path"])
def test_source_declaration_rejects_invalid_extensions(extension: str) -> None:
    with pytest.raises(ValidationError, match="extension"):
        SourceDeclaration(
            coordinate=_entity_coordinate(),
            declared_path="data/source.nii.gz",
            declared_extension=extension,
        )


def test_source_declaration_requires_path_to_match_extension() -> None:
    with pytest.raises(ValidationError, match="end with declared extension"):
        SourceDeclaration(
            coordinate=_entity_coordinate(),
            declared_path="data/source.json",
            declared_extension=".csv",
        )


def test_source_occurrence_guard_validates_stat_facts() -> None:
    guard = SourceOccurrenceGuard(
        st_dev=1,
        st_ino=2,
        st_size=3,
        st_mtime_ns=4,
        st_ctime_ns=5,
    )
    assert guard.st_size == 3

    for field, value in (("st_dev", -1), ("st_ino", True), ("st_size", 1.5)):
        with pytest.raises(ValidationError, match=field):
            replace(guard, **{field: value})


def test_observed_authority_validates_persisted_facts() -> None:
    guard = SourceOccurrenceGuard(1, 2, 3, 4, 5)
    digest = sha256_digest(b"abc")
    authority = registered_source_authority_from_facts(
        coordinate=_entity_coordinate(),
        declared_path="data/source.json",
        declared_extension=".json",
        content_digest=digest,
        file_size=3,
        guard=guard,
    )

    assert authority.status == "unchanged"
    assert authority.content_digest == digest

    with pytest.raises(ValidationError, match="digest"):
        replace(authority, content_digest="bad")
    with pytest.raises(ValidationError, match="file_size"):
        replace(authority, file_size=4)
    with pytest.raises(ValidationError, match="status"):
        replace(authority, status="refreshed")


def test_new_source_is_hashed_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nipact import source_authority as source_authority_module

    source_path = _write_source(tmp_path)
    real_digest = source_authority_module._stream_sha256
    hashed_handles: list[str] = []

    def recording_digest(handle: object) -> str:
        hashed_handles.append(str(getattr(handle, "name")))
        return real_digest(handle)  # type: ignore[arg-type]

    monkeypatch.setattr(source_authority_module, "_stream_sha256", recording_digest)

    observed = _observe_new(tmp_path)

    assert observed.status == "new"
    assert observed.content_digest == sha256_digest(b"source-v1")
    assert observed.file_size == len(b"source-v1")
    assert hashed_handles == [str(source_path.resolve())]


def test_matching_guard_reuses_registered_digest_without_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nipact import source_authority as source_authority_module

    _write_source(tmp_path)
    registered = _observe_new(tmp_path)

    def unexpected_digest(_handle: object) -> str:
        raise AssertionError("matching source guard must not trigger content hashing")

    monkeypatch.setattr(source_authority_module, "_stream_sha256", unexpected_digest)

    observed = observe_source_authority(
        runtime_root=tmp_path,
        declaration=_declaration(),
        registered=registered,
    )

    assert observed == replace(registered, status="unchanged")


def test_changed_guard_with_equal_bytes_hashes_once_and_refreshes_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nipact import source_authority as source_authority_module

    _write_source(tmp_path)
    registered = _observe_new(tmp_path)
    stale_guard = replace(registered.guard, st_mtime_ns=registered.guard.st_mtime_ns + 1)
    registered = replace(registered, guard=stale_guard, status="unchanged")
    real_digest = source_authority_module._stream_sha256
    hash_count = 0

    def count_digest(handle: object) -> str:
        nonlocal hash_count
        hash_count += 1
        return real_digest(handle)  # type: ignore[arg-type]

    monkeypatch.setattr(source_authority_module, "_stream_sha256", count_digest)

    observed = observe_source_authority(
        runtime_root=tmp_path,
        declaration=_declaration(),
        registered=registered,
    )

    assert hash_count == 1
    assert observed.status == "unchanged"
    assert observed.content_digest == registered.content_digest
    assert observed.guard != stale_guard


def test_changed_source_is_hashed_once_and_classified_changed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nipact import source_authority as source_authority_module

    source_path = _write_source(tmp_path)
    registered = _observe_new(tmp_path)
    registered = replace(
        registered,
        guard=replace(
            registered.guard,
            st_mtime_ns=registered.guard.st_mtime_ns + 1,
        ),
        status="unchanged",
    )
    source_path.write_bytes(b"source-v2")
    real_digest = source_authority_module._stream_sha256
    hash_count = 0

    def count_digest(handle: object) -> str:
        nonlocal hash_count
        hash_count += 1
        return real_digest(handle)  # type: ignore[arg-type]

    monkeypatch.setattr(source_authority_module, "_stream_sha256", count_digest)

    observed = observe_source_authority(
        runtime_root=tmp_path,
        declaration=_declaration(),
        registered=registered,
    )

    assert hash_count == 1
    assert observed.status == "changed"
    assert observed.content_digest == sha256_digest(b"source-v2")
    assert observed.guard.st_size == len(b"source-v2")


def test_source_observation_rejects_missing_or_nonregular_occurrence(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="missing source occurrence"):
        _observe_new(tmp_path)

    source_path = tmp_path / "data/aac_027_m00/t1w.nii.gz"
    source_path.mkdir(parents=True)
    with pytest.raises(ValidationError, match="regular file"):
        _observe_new(tmp_path)


def test_source_observation_rejects_relocation_before_hashing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nipact import source_authority as source_authority_module

    _write_source(tmp_path)
    registered = _observe_new(tmp_path)
    relocated_path = tmp_path / "data/relocated/t1w.nii.gz"
    relocated_path.parent.mkdir(parents=True)
    relocated_path.write_bytes(b"source-v1")

    def unexpected_digest(_handle: object) -> str:
        raise AssertionError("unsupported relocation must fail before hashing")

    monkeypatch.setattr(source_authority_module, "_stream_sha256", unexpected_digest)

    with pytest.raises(ValidationError, match="relocation"):
        observe_source_authority(
            runtime_root=tmp_path,
            declaration=_declaration(path="data/relocated/t1w.nii.gz"),
            registered=registered,
        )


def test_source_observation_rejects_unstable_opened_occurrence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nipact import source_authority as source_authority_module

    _write_source(tmp_path)
    real_guard = source_authority_module._guard_for_open_file
    guard_calls = 0

    def changing_guard(handle: object) -> SourceOccurrenceGuard:
        nonlocal guard_calls
        guard_calls += 1
        guard = real_guard(handle)  # type: ignore[arg-type]
        if guard_calls == 2:
            return replace(guard, st_mtime_ns=guard.st_mtime_ns + 1)
        return guard

    monkeypatch.setattr(
        source_authority_module,
        "_guard_for_open_file",
        changing_guard,
    )

    with pytest.raises(ValidationError, match="changed while it was being hashed"):
        _observe_new(tmp_path)


def test_failed_observation_does_not_mutate_registered_authority(
    tmp_path: Path,
) -> None:
    _write_source(tmp_path)
    registered = _observe_new(tmp_path)
    original = replace(registered)
    (tmp_path / registered.declaration.declared_path).unlink()

    with pytest.raises(ValidationError):
        observe_source_authority(
            runtime_root=tmp_path,
            declaration=registered.declaration,
            registered=registered,
        )

    assert registered == original
