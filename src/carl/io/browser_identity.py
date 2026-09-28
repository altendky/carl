"""Build navigation headers from the installed Brave version."""

import re
import subprocess
from pathlib import Path

from carl.core.models import Header


def brave_navigation_headers(
    executable: Path = Path("/usr/bin/brave-browser"),
) -> tuple[Header, ...]:
    completed = subprocess.run(
        [str(executable), "--version"],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    match = re.search(r"(\d+)\.", completed.stdout)
    if match is None:
        raise ValueError("Could not determine the installed Brave major version")
    major = match.group(1)
    values = (
        (
            "User-Agent",
            f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36",
        ),
        (
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        ),
        ("Accept-Language", "en-US,en;q=0.9"),
        ("Upgrade-Insecure-Requests", "1"),
        ("Sec-Fetch-Dest", "document"),
        ("Sec-Fetch-Mode", "navigate"),
        ("Sec-Fetch-Site", "none"),
        ("Sec-Fetch-User", "?1"),
        ("Sec-CH-UA", f'"Brave";v="{major}", "Chromium";v="{major}", "Not=A?Brand";v="24"'),
        ("Sec-CH-UA-Mobile", "?0"),
        ("Sec-CH-UA-Platform", '"Linux"'),
    )
    return tuple(
        Header(name=name.encode("ascii"), value=value.encode("ascii")) for name, value in values
    )
