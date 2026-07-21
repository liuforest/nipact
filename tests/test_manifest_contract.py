from pathlib import Path

import pytest

from nipact.errors import ValidationError
from nipact.hashing import SHORT_HASH_LENGTH, is_valid_digest, sha256_digest, short_hash
from nipact.identity import validate_entity_id
from nipact.manifest import (
    MANIFEST_VALUE_SCHEMA,
    ManifestValue,
    ManifestValueReference,
    build_manifest,
    build_manifest_value,
    load_manifest,
    manifest_digest,
    parse_manifest,
)


ENTITY_SET_V1_BODY = "color_000\ncolor_001\ncolor_002"
ENTITY_SET_V1_DIGEST = "c553da47a8742c9f1546523a37879ff39cb8cc713f706791ae81307859f1a8ec"
LEGACY_MANIFEST_DIGEST = "bbf9943f3cf1af0acdab6fef252150a12f00f60473ae38e64f3116f80d952851"


def test_sha256_digest_and_short_hash_are_deterministic() -> None:
    digest = sha256_digest(b"abc")

    assert digest == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert is_valid_digest(digest)
    assert short_hash(digest) == "ba7816bf8f01cfea"
    assert len(short_hash(digest)) == SHORT_HASH_LENGTH


def test_manifest_identity_uses_sorted_membership_only() -> None:
    first = parse_manifest(
        {
            "description": "Original description",
            "entities": ["color_002", "color_000", "color_001"],
        }
    )
    second = parse_manifest(
        {
            "description": "Edited description",
            "entities": ["color_001", "color_002", "color_000"],
        }
    )

    assert first.entity_ids == ("color_000", "color_001", "color_002")
    assert first.manifest_body == "color_000\ncolor_001\ncolor_002"
    assert second.manifest_body == first.manifest_body
    assert second.manifest_digest == first.manifest_digest
    assert second.manifest_hash == first.manifest_hash


def test_build_manifest_exposes_derived_inspection_fields() -> None:
    manifest = build_manifest(
        description="Small test manifest",
        entities=["color_002", "color_000", "color_001"],
    )

    assert manifest.entity_count == 3
    assert manifest.first_entity_id == "color_000"
    assert manifest.last_entity_id == "color_002"
    assert manifest.manifest_digest == manifest_digest(manifest.entity_ids)
    assert manifest.manifest_hash == short_hash(manifest.manifest_digest)


def test_manifest_value_uses_frozen_entity_set_v1_contract() -> None:
    value = build_manifest_value(entities=["color_002", "color_000", "color_001"])

    assert value.value_schema == MANIFEST_VALUE_SCHEMA == "entity_set_v1"
    assert value.canonical_body == ENTITY_SET_V1_BODY
    assert value.manifest_digest == ENTITY_SET_V1_DIGEST
    assert value.entity_ids == ("color_000", "color_001", "color_002")
    assert value.entity_count == 3
    assert value.manifest_hash == "c553da47a8742c9f"
    assert value.reference == ManifestValueReference(
        value_schema="entity_set_v1",
        manifest_digest=ENTITY_SET_V1_DIGEST,
    )


def test_manifest_value_digest_is_domain_separated_from_legacy_digest() -> None:
    value = build_manifest_value(entities=["color_000", "color_001", "color_002"])

    assert manifest_digest(value.entity_ids) == LEGACY_MANIFEST_DIGEST
    assert value.manifest_digest != LEGACY_MANIFEST_DIGEST


def test_manifest_value_is_invariant_to_input_order() -> None:
    first = build_manifest_value(entities=["color_002", "color_000", "color_001"])
    second = build_manifest_value(entities=["color_001", "color_002", "color_000"])

    assert first == second


def test_manifest_value_is_invariant_to_declaration_representation(tmp_path: Path) -> None:
    first_path = tmp_path / "first-name.yaml"
    second_path = tmp_path / "renamed.yaml"
    first_path.write_text(
        "description: Original description\n"
        "entities:\n"
        "  - color_002\n"
        "  - color_000\n"
        "  - color_001\n",
        encoding="utf-8",
    )
    second_path.write_text(
        "# A differently formatted declaration with the same membership.\n"
        "entities: [color_001, color_002, color_000]\n"
        "description: Renamed and edited\n",
        encoding="utf-8",
    )

    first_manifest = load_manifest(first_path)
    second_manifest = load_manifest(second_path)
    first_value = build_manifest_value(entities=first_manifest.entity_ids)
    second_value = build_manifest_value(entities=second_manifest.entity_ids)

    assert first_value == second_value


def test_manifest_value_references_deduplicate_by_schema_and_digest() -> None:
    first = ManifestValueReference(
        value_schema=MANIFEST_VALUE_SCHEMA,
        manifest_digest=ENTITY_SET_V1_DIGEST,
    )
    second = ManifestValueReference(
        value_schema=MANIFEST_VALUE_SCHEMA,
        manifest_digest=ENTITY_SET_V1_DIGEST,
    )

    assert first == second
    assert {first, second} == {first}


@pytest.mark.parametrize(
    ("entities", "message"),
    [
        ([], "manifest entities cannot be empty"),
        (["color_000", "color_000"], "duplicate"),
        (["color/000"], "path separators"),
    ],
)
def test_build_manifest_value_rejects_invalid_membership(
    entities: list[object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        build_manifest_value(entities=entities)


def test_manifest_value_reference_rejects_unsupported_schema() -> None:
    with pytest.raises(ValidationError, match="unsupported manifest value schema"):
        ManifestValueReference(
            value_schema="entity_set_v2",
            manifest_digest=ENTITY_SET_V1_DIGEST,
        )


def test_manifest_value_reference_rejects_invalid_digest() -> None:
    with pytest.raises(ValidationError, match="lowercase 64-character"):
        ManifestValueReference(
            value_schema=MANIFEST_VALUE_SCHEMA,
            manifest_digest="not-a-digest",
        )


def test_manifest_value_rejects_digest_body_mismatch() -> None:
    with pytest.raises(ValidationError, match="does not match canonical body"):
        ManifestValue(
            value_schema=MANIFEST_VALUE_SCHEMA,
            manifest_digest="0" * 64,
            canonical_body=ENTITY_SET_V1_BODY,
        )


@pytest.mark.parametrize(
    "canonical_body",
    [
        "",
        "color_001\ncolor_000",
        "color_000\ncolor_000",
        "color_000\ncolor/001",
        "color_000\ncolor_001\n",
    ],
)
def test_manifest_value_rejects_noncanonical_stored_body(canonical_body: str) -> None:
    with pytest.raises(ValidationError):
        ManifestValue(
            value_schema=MANIFEST_VALUE_SCHEMA,
            manifest_digest=ENTITY_SET_V1_DIGEST,
            canonical_body=canonical_body,
        )


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"description": "demo", "entities": [],}, "manifest entities cannot be empty"),
        ({"description": "demo", "entities": ["color_000"], "count": 1}, "unknown field"),
        ({"description": "demo"}, "missing required field"),
        ({"entities": ["color_000"]}, "missing required field"),
        ({"description": 3, "entities": ["color_000"]}, "description must be a string"),
        ({"description": "demo", "entities": "color_000"}, "entities must be a list"),
        ({"description": "demo", "entities": ["color_000", "color_000"]}, "duplicate"),
        ({"description": "demo", "entities": [" color_000"]}, "leading or trailing whitespace"),
        ({"description": "demo", "entities": ["color/000"]}, "path separators"),
        ({"description": "demo", "entities": ["color..000"]}, "path traversal"),
        ({"description": "demo", "entities": [3]}, "entity_id must be a string"),
    ],
)
def test_parse_manifest_rejects_invalid_minimal_yaml(
    payload: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        parse_manifest(payload)


@pytest.mark.parametrize("entity_id", ["", " color_000", "color 000", "/color", ".", ".."])
def test_validate_entity_id_rejects_unsafe_tokens(entity_id: str) -> None:
    with pytest.raises(ValidationError):
        validate_entity_id(entity_id)
