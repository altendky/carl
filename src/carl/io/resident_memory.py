"""Conservative accounting for retained application session data, without logging it."""

import httpx


def httpx_retained_bytes(client: httpx.AsyncClient) -> int:
    # Include a fixed allowance per jar entry in addition to encoded data. This
    # bounds retained application state, not allocator or native connection RSS.
    cookies = sum(
        1024
        + sum(
            len(value.encode("utf-8"))
            for value in (cookie.name, cookie.value or "", cookie.domain, cookie.path)
        )
        for cookie in client.cookies.jar
    )
    return cookies + sum(len(name) + len(value) + 128 for name, value in client.headers.raw)
