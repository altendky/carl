"""Pure HTTP content-encoding decoding and stored-body interpretation."""

import gzip
import zlib
from io import BytesIO

import brotli
import zstandard

from carl.core.models import JsonValue

MAX_DECODED_BYTES = 100_000_000
_DECODE_CHUNK_BYTES = 32_768


class ContentDecodingError(ValueError):
    """HTTP content cannot be decoded as declared."""


def _append_decoded(output: bytearray, chunk: bytes, maximum_bytes: int) -> None:
    if len(output) + len(chunk) > maximum_bytes:
        raise ContentDecodingError("Decoded body exceeds limit")
    output.extend(chunk)


def _decode_gzip(content: bytes, maximum_bytes: int) -> bytes:
    output = bytearray()
    with gzip.GzipFile(fileobj=BytesIO(content)) as stream:
        while chunk := stream.read(min(_DECODE_CHUNK_BYTES, maximum_bytes - len(output) + 1)):
            _append_decoded(output, chunk, maximum_bytes)
    return bytes(output)


def _decode_deflate(content: bytes, window_bits: int, maximum_bytes: int) -> bytes:
    output = bytearray()
    decoder = zlib.decompressobj(window_bits)
    for offset in range(0, len(content), _DECODE_CHUNK_BYTES):
        pending = content[offset : offset + _DECODE_CHUNK_BYTES]
        while pending:
            chunk = decoder.decompress(
                pending, min(_DECODE_CHUNK_BYTES, maximum_bytes - len(output) + 1)
            )
            _append_decoded(output, chunk, maximum_bytes)
            pending = decoder.unconsumed_tail
        if decoder.eof:
            break
    if not decoder.eof:
        raise zlib.error("Incomplete deflate encoded content")
    return bytes(output)


def _decode_brotli(content: bytes, maximum_bytes: int) -> bytes:
    output = bytearray()
    decoder = brotli.Decompressor()
    for offset in range(0, len(content), _DECODE_CHUNK_BYTES):
        pending = content[offset : offset + _DECODE_CHUNK_BYTES]
        while True:
            # Brotli's output limit is a growth threshold, not an exact maximum.
            # One requests the smallest native buffer rather than permitting an
            # entire expanding body to be materialized before checking its size.
            chunk = decoder.process(pending, output_buffer_limit=1)
            _append_decoded(output, chunk, maximum_bytes)
            if decoder.is_finished() or (decoder.can_accept_more_data() and not chunk):
                break
            pending = b""
    if not decoder.is_finished():
        raise ContentDecodingError("Incomplete br encoded content")
    return bytes(output)


def _decode_zstd(content: bytes, maximum_bytes: int) -> bytes:
    declared_size = zstandard.frame_content_size(content)
    # max_output_size only limits frames whose size is absent from their header.
    # Check declared sizes before the decoder allocates its output buffer too.
    if declared_size != zstandard.CONTENTSIZE_UNKNOWN and declared_size > maximum_bytes:
        raise ContentDecodingError("Decoded body exceeds limit")
    return zstandard.ZstdDecompressor().decompress(content, max_output_size=max(1, maximum_bytes))


def decode_content(
    content: bytes, encodings: tuple[str, ...], *, maximum_bytes: int | None = None
) -> bytes:
    """Reverse HTTP Content-Encoding transforms without doing I/O."""

    maximum_bytes = MAX_DECODED_BYTES if maximum_bytes is None else maximum_bytes
    if maximum_bytes < 0:
        raise ValueError("Decoded body limit must be nonnegative")
    decoded = content
    for encoding in reversed(encodings):
        normalized = encoding.strip().lower()
        if normalized in {"", "identity"}:
            continue
        try:
            if normalized == "gzip":
                decoded = _decode_gzip(decoded, maximum_bytes)
            elif normalized == "deflate":
                try:
                    decoded = _decode_deflate(decoded, zlib.MAX_WBITS, maximum_bytes)
                except zlib.error:
                    decoded = _decode_deflate(decoded, -zlib.MAX_WBITS, maximum_bytes)
            elif normalized == "br":
                decoded = _decode_brotli(decoded, maximum_bytes)
            elif normalized == "zstd":
                decoded = _decode_zstd(decoded, maximum_bytes)
            else:
                raise ContentDecodingError(f"Unsupported content encoding: {normalized}")
        except (brotli.error, EOFError, OSError, zlib.error, zstandard.ZstdError) as error:
            raise ContentDecodingError(
                f"Invalid {normalized or 'identity'} encoded content"
            ) from error
        if len(decoded) > maximum_bytes:
            raise ContentDecodingError("Decoded body exceeds limit")
    if len(decoded) > maximum_bytes:
        raise ContentDecodingError("Decoded body exceeds limit")
    return decoded


def decoded_stored_body(
    content: bytes, representation: JsonValue, encodings: tuple[str, ...]
) -> bytes:
    """Read current decoded bodies and earlier HTTP message-content artifacts."""

    if not isinstance(representation, dict):
        raise ValueError("HTTP body representation is missing")
    kind = representation.get("kind")
    if kind == "content_decoded_http_body" and representation.get("content_decoded") is True:
        return decode_content(content, ())
    if kind in {"decoded_image_file", "validated_http_image_body"}:
        return decode_content(content, ())
    if kind == "http_message_content" and representation.get("content_decoded") is False:
        return decode_content(content, encodings)
    raise ValueError("Unsupported HTTP body representation")
