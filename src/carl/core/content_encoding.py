"""Pure HTTP content-encoding decoding and stored-body interpretation."""

import gzip
import zlib

import brotli
import zstandard

from carl.core.models import JsonValue

MAX_DECODED_BYTES = 100_000_000


class ContentDecodingError(ValueError):
    """HTTP content cannot be decoded as declared."""


def decode_content(content: bytes, encodings: tuple[str, ...]) -> bytes:
    """Reverse HTTP Content-Encoding transforms without doing I/O."""

    decoded = content
    for encoding in reversed(encodings):
        normalized = encoding.strip().lower()
        if normalized in {"", "identity"}:
            continue
        try:
            if normalized == "gzip":
                decoded = gzip.decompress(decoded)
            elif normalized == "deflate":
                try:
                    decoded = zlib.decompress(decoded)
                except zlib.error:
                    decoded = zlib.decompress(decoded, -zlib.MAX_WBITS)
            elif normalized == "br":
                decoded = brotli.decompress(decoded)
            elif normalized == "zstd":
                decoded = zstandard.ZstdDecompressor().decompress(
                    decoded, max_output_size=MAX_DECODED_BYTES
                )
            else:
                raise ContentDecodingError(f"Unsupported content encoding: {normalized}")
        except (brotli.error, EOFError, OSError, zlib.error, zstandard.ZstdError) as error:
            raise ContentDecodingError(
                f"Invalid {normalized or 'identity'} encoded content"
            ) from error
        if len(decoded) > MAX_DECODED_BYTES:
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
        return content
    if kind in {"decoded_image_file", "validated_http_image_body"}:
        return content
    if kind == "http_message_content" and representation.get("content_decoded") is False:
        return decode_content(content, encodings)
    raise ValueError("Unsupported HTTP body representation")
