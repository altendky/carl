"""Atomic content-addressed storage for validated image files."""

import hashlib
import os
import secrets
import stat
from dataclasses import dataclass
from pathlib import Path

import anyio

_EXTENSIONS = {
    "image/avif": ".avif",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
}


@dataclass(frozen=True, slots=True)
class StoredImageFile:
    sha256: str
    size: int
    media_type: str
    locator: str
    path: Path


def _private_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("Image storage path is not an owned directory")
    if stat.S_IMODE(info.st_mode) & (stat.S_IRWXG | stat.S_IRWXO):
        os.chmod(path, 0o700)


def _verify_file(path: Path, *, sha256: str, size: int) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        digest = hashlib.sha256()
        observed_size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            observed_size += len(chunk)
    finally:
        os.close(descriptor)
    if observed_size != size or digest.hexdigest() != sha256:
        raise ValueError("Existing image file failed content verification")


def _publish(root: Path, content: bytes, media_type: str, sha256: str) -> StoredImageFile:
    if hashlib.sha256(content).hexdigest() != sha256:
        raise ValueError("Validated image digest does not match its content")
    try:
        extension = _EXTENSIONS[media_type]
    except KeyError:
        raise ValueError("Validated image has no supported filename extension") from None
    image_root = root / "images" / "sha256"
    bucket = image_root / sha256[:2]
    for directory in (root / "images", image_root, bucket):
        _private_directory(directory)
    destination = bucket / f"{sha256}{extension}"
    locator = destination.relative_to(root).as_posix()
    if destination.exists():
        _verify_file(destination, sha256=sha256, size=len(content))
        return StoredImageFile(sha256, len(content), media_type, locator, destination)

    temporary = bucket / f".{sha256}.{secrets.token_hex(16)}.tmp"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        offset = 0
        while offset < len(content):
            offset += os.write(descriptor, content[offset:])
        os.fsync(descriptor)
    except BaseException:
        os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise
    else:
        os.close(descriptor)
    try:
        try:
            os.link(temporary, destination, follow_symlinks=False)
        except FileExistsError:
            _verify_file(destination, sha256=sha256, size=len(content))
        directory_descriptor = os.open(bucket, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary.unlink(missing_ok=True)
    return StoredImageFile(sha256, len(content), media_type, locator, destination)


class ImageFileStore:
    def __init__(self, data_directory: Path):
        self.data_directory = data_directory.resolve()

    async def publish(self, *, content: bytes, media_type: str, sha256: str) -> StoredImageFile:
        return await anyio.to_thread.run_sync(
            _publish, self.data_directory, content, media_type, sha256
        )
