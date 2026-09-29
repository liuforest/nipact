import hashlib
import json
import os
from pathlib import Path

import pytest

from nipact.errors import ValidationError
from nipact.hashing import directory_tree_digest, sha256_file_digest


@pytest.mark.parametrize(
    ("nested", "expected"),
    [
        (False, "ddab9dce7505e6ceaeb825c390ff9c2ee26c266d1e48f818971c99ff87f70ad9"),
        (True, "8051acc0267fb7bacc5e295022b2de5ff08adcd555a1903f4886a5247de4358b"),
    ],
)
def test_directory_tree_literal_vectors_and_metadata_independence(
    tmp_path: Path, nested: bool, expected: str
) -> None:
    for root_name, order in [
        ("first", ["empty", "maps"]),
        ("second", ["maps", "empty"]),
    ]:
        root = tmp_path / root_name
        root.mkdir()
        if nested:
            for name in order:
                (root / name).mkdir()
            member = root / "maps/a.txt"
            member.write_bytes(b"abc")
            os.utime(member, ns=(1, 1))
            member.chmod(0o400)
        os.utime(root, ns=(2, 2))
        assert directory_tree_digest(root) == (expected, 3 if nested else 0)


def test_directory_tree_preserves_exact_names_hidden_members_and_contents(
    tmp_path: Path,
) -> None:
    # Explicit records are the independent format oracle, not a filesystem traversal.
    names = [".snakemake_timestamp", "e\u0301", "é", 'quote"\\\n']
    empty_digest = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    for name in reversed(names):
        (tmp_path / name).write_bytes(b"")
    records = [
        ["file", name, 0, empty_digest]
        for name in sorted(names, key=lambda n: n.encode("utf-8"))
    ]
    manifest = json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    expected = hashlib.sha256(b"nipact.directory-tree.v1\0" + manifest).hexdigest()
    assert directory_tree_digest(tmp_path) == (expected, 0)
    (tmp_path / "é").rename(tmp_path / "renamed")
    renamed = directory_tree_digest(tmp_path)
    assert renamed[0] != expected
    (tmp_path / "renamed").write_bytes(b"abc")
    changed = directory_tree_digest(tmp_path)
    assert changed[0] != renamed[0] and changed[1] == 3
    (tmp_path / "empty").mkdir()
    assert directory_tree_digest(tmp_path)[0] != changed[0]
    (tmp_path / "empty").rmdir()
    assert directory_tree_digest(tmp_path) == changed
    (tmp_path / "renamed").write_bytes(b"abd")
    assert directory_tree_digest(tmp_path)[0] != changed[0]
    (tmp_path / "renamed").write_bytes(b"abc")
    assert (
        sha256_file_digest(tmp_path / "renamed")
        == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


@pytest.mark.parametrize(
    "invalid",
    ["file_root", "symlink_root", "symlink_member", "hardlink", "fifo", "undecodable"],
)
def test_directory_tree_rejects_unsupported_filesystem_objects(
    tmp_path: Path, invalid: str
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    external = tmp_path / "external"
    external.write_bytes(b"abc")
    if invalid == "file_root":
        root = external
    elif invalid == "symlink_root":
        root = tmp_path / "link"
        root.symlink_to(tmp_path / "root", target_is_directory=True)
    elif invalid == "symlink_member":
        (root / "link").symlink_to(external)
    elif invalid == "hardlink":
        os.link(external, root / "linked")
    elif invalid == "fifo":
        os.mkfifo(root / "pipe")
    else:
        (root / os.fsdecode(b"bad-\xff")).write_bytes(b"")
    with pytest.raises(ValidationError, match="directory.*(requires|UTF-8)"):
        directory_tree_digest(root)
