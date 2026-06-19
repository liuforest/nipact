import pytest

from nipact.errors import ValidationError
from nipact.hashing import SHORT_HASH_LENGTH, is_valid_digest, sha256_digest, short_hash
from nipact.identity import validate_entity_id
from nipact.manifest import (
    build_manifest,
    manifest_digest,
    parse_manifest,
)


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
