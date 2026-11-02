"""Content expansion is bounded during decoding, including saved-body reads."""

import gzip
import zlib
from collections.abc import Callable

import brotli
import pytest
import zstandard

from carl.core import content_encoding
from carl.core.content_encoding import ContentDecodingError, decode_content, decoded_stored_body

_ENCODERS: tuple[tuple[str, Callable[[bytes], bytes]], ...] = (
    ("gzip", gzip.compress),
    ("deflate", zlib.compress),
    ("deflate", lambda value: zlib.compress(value, wbits=-zlib.MAX_WBITS)),
    ("br", brotli.compress),
    ("zstd", zstandard.ZstdCompressor().compress),
    ("zstd", zstandard.ZstdCompressor(write_content_size=False).compress),
)


@pytest.mark.parametrize(("encoding", "compress"), _ENCODERS)
@pytest.mark.parametrize("size", (0, 1, 32_768, 100_000))
def test_valid_streams_round_trip_at_the_exact_byte_limit(
    monkeypatch: pytest.MonkeyPatch, encoding: str, compress: Callable[[bytes], bytes], size: int
) -> None:
    content = (bytes(range(256)) * ((size + 255) // 256))[:size]
    monkeypatch.setattr(content_encoding, "MAX_DECODED_BYTES", max(1, size))
    assert decode_content(compress(content), (encoding,)) == content


@pytest.mark.parametrize(("encoding", "compress"), _ENCODERS)
def test_expanding_bodies_are_rejected_before_unbounded_one_shot_decode(
    monkeypatch: pytest.MonkeyPatch, encoding: str, compress: Callable[[bytes], bytes]
) -> None:
    encoded = compress(b"x" * 1_000_000)
    monkeypatch.setattr(content_encoding, "MAX_DECODED_BYTES", 1_024)

    def forbidden(*_args: object, **_kwargs: object) -> bytes:
        pytest.fail("Unbounded whole-body decompression must not be used")

    monkeypatch.setattr(gzip, "decompress", forbidden)
    monkeypatch.setattr(zlib, "decompress", forbidden)
    monkeypatch.setattr(brotli, "decompress", forbidden)
    with pytest.raises(ContentDecodingError):
        decode_content(encoded, (encoding,))


def test_declared_zstd_size_is_checked_before_allocating_decoder_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = zstandard.ZstdCompressor().compress(b"x" * 1_000_000)
    monkeypatch.setattr(content_encoding, "MAX_DECODED_BYTES", 1_024)

    def forbidden(*_args: object, **_kwargs: object) -> None:
        pytest.fail("A frame exceeding the byte limit must not reach the decoder")

    monkeypatch.setattr(zstandard, "ZstdDecompressor", forbidden)
    with pytest.raises(ContentDecodingError, match="exceeds limit"):
        decode_content(encoded, ("zstd",))


@pytest.mark.parametrize(("encoding", "compress"), _ENCODERS)
def test_truncated_streams_remain_failures(
    encoding: str, compress: Callable[[bytes], bytes]
) -> None:
    with pytest.raises(ContentDecodingError):
        decode_content(compress(b"retained evidence" * 100)[:-1], (encoding,))


def test_concatenated_gzip_members_share_one_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    encoded = gzip.compress(b"a" * 600) + gzip.compress(b"b" * 600)
    assert decode_content(encoded, ("gzip",)) == b"a" * 600 + b"b" * 600
    monkeypatch.setattr(content_encoding, "MAX_DECODED_BYTES", 1_024)
    with pytest.raises(ContentDecodingError, match="exceeds limit"):
        decode_content(encoded, ("gzip",))


def test_each_stacked_encoding_has_a_separate_expansion_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    encoded = brotli.compress(gzip.compress(b"x" * 100_000))
    monkeypatch.setattr(content_encoding, "MAX_DECODED_BYTES", 1_024)
    with pytest.raises(ContentDecodingError, match="exceeds limit"):
        decode_content(encoded, ("gzip", "br"))


@pytest.mark.parametrize("encodings", ((), ("identity",), ("",)))
def test_identity_bodies_obey_the_same_limit(
    monkeypatch: pytest.MonkeyPatch, encodings: tuple[str, ...]
) -> None:
    monkeypatch.setattr(content_encoding, "MAX_DECODED_BYTES", 4)
    assert decode_content(b"1234", encodings) == b"1234"
    with pytest.raises(ContentDecodingError, match="exceeds limit"):
        decode_content(b"12345", encodings)


@pytest.mark.parametrize(
    "representation",
    (
        {"kind": "content_decoded_http_body", "content_decoded": True},
        {"kind": "decoded_image_file"},
        {"kind": "validated_http_image_body"},
    ),
)
def test_already_decoded_saved_bodies_cannot_bypass_limit(
    monkeypatch: pytest.MonkeyPatch, representation: dict[str, object]
) -> None:
    monkeypatch.setattr(content_encoding, "MAX_DECODED_BYTES", 4)
    with pytest.raises(ContentDecodingError, match="exceeds limit"):
        decoded_stored_body(b"12345", representation, ())
