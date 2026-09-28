"""Pure HTTP acquisition plans and redirect decisions."""

from email.message import Message
from urllib.parse import urljoin, urlsplit

from pydantic import Field, SecretStr, field_validator, model_validator

from carl.core.models import Header, JsonValue, StrictModel


class TimeoutPlan(StrictModel):
    connect_seconds: float = Field(default=10.0, gt=0)
    read_seconds: float = Field(default=30.0, gt=0)
    write_seconds: float = Field(default=30.0, gt=0)
    pool_seconds: float = Field(default=10.0, gt=0)

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class FormField(StrictModel):
    name: str = Field(min_length=1)
    value: SecretStr = Field(repr=False)
    protected: bool = False

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        if not value.isascii() or any(character in value for character in "\r\n=&"):
            raise ValueError("Form field names must be safe ASCII identifiers")
        return value

    def evidence(self) -> dict[str, JsonValue]:
        value = self.value.get_secret_value()
        return {
            "name": self.name,
            "value": (
                {
                    "state": "redacted",
                    "reason": "protected_session_or_credential_material",
                    "utf8_bytes": len(value.encode("utf-8")),
                }
                if self.protected
                else value
            ),
        }


class RequestPlan(StrictModel):
    url: str
    method: str = "GET"
    headers: tuple[Header, ...] = ()
    timeout: TimeoutPlan = Field(default_factory=TimeoutPlan)
    follow_redirects: bool = True
    max_redirects: int = Field(default=5, ge=0, le=10)
    max_body_bytes: int = Field(default=20_000_000, gt=0, le=100_000_000)
    routing: tuple[str, ...] = ("direct",)
    compression: tuple[str, ...] = ("gzip", "deflate", "br", "zstd")

    @model_validator(mode="after")
    def validate_request(self) -> "RequestPlan":
        parsed = urlsplit(self.url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Acquisition requires a credential-free HTTPS URL")
        if self.method not in {"GET", "POST"}:
            raise ValueError("Acquisition supports GET and POST only")
        if self.method == "POST" and self.follow_redirects:
            raise ValueError("POST acquisition requires explicit redirect handling")
        if not self.routing or any(not part for part in self.routing):
            raise ValueError("Routing identity parts must be nonempty")
        forbidden = {b"authorization", b"cookie", b"proxy-authorization"}
        for header in self.headers:
            if header.name.lower() in forbidden:
                raise ValueError("Anonymous acquisition cannot include credential headers")
            if b"\r" in header.name or b"\n" in header.name:
                raise ValueError("Invalid request header name")
            if b"\r" in header.value or b"\n" in header.value:
                raise ValueError("Invalid request header value")
        return self

    def as_json(
        self,
        *,
        protected_header_names: frozenset[bytes] = frozenset(),
    ) -> dict[str, JsonValue]:
        result = self.model_dump(mode="json", by_alias=True)
        result["headers"] = [
            (
                {
                    "name_latin1": header.name.decode("latin-1"),
                    "value": {
                        "state": "redacted",
                        "reason": "protected_session_or_credential_material",
                        "bytes": len(header.value),
                    },
                }
                if header.name.lower() in protected_header_names
                else header.as_json()
            )
            for header in self.headers
        ]
        return result


def header_values(headers: tuple[Header, ...], name: bytes) -> tuple[bytes, ...]:
    lowered = name.lower()
    return tuple(header.value for header in headers if header.name.lower() == lowered)


def stored_header_values(headers: JsonValue, name: str) -> tuple[str, ...]:
    if not isinstance(headers, list):
        raise ValueError("Stored headers are malformed")
    values = []
    for header in headers:
        if not isinstance(header, dict):
            raise ValueError("Stored header is malformed")
        header_name = header.get("name_latin1")
        value = header.get("value_latin1")
        if isinstance(header_name, str) and header_name.lower() == name and isinstance(value, str):
            values.append(value)
    return tuple(values)


def stored_content_encodings(headers: JsonValue) -> tuple[str, ...]:
    values = stored_header_values(headers, "content-encoding")
    return tuple(part.strip() for value in values for part in value.split(",") if part.strip())


def stored_response_charset(headers: JsonValue) -> tuple[str, str]:
    values = stored_header_values(headers, "content-type")
    if values:
        message = Message()
        message["Content-Type"] = values[-1]
        charset = message.get_content_charset()
        if charset is not None:
            return charset, "http_content_type"
    return "utf-8", "html_default"


def redirect_target(
    *, status_code: int, headers: tuple[Header, ...], response_url: str
) -> str | None:
    if status_code not in {301, 302, 303, 307, 308}:
        return None
    locations = header_values(headers, b"location")
    if not locations:
        return None
    target = urljoin(response_url, locations[-1].decode("latin-1"))
    parsed = urlsplit(target)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("Redirect target is not a credential-free HTTPS URL")
    return target
