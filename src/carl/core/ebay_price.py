"""Conservative money normalization for prices displayed by ebay.com.

The US site displays an unqualified dollar amount in USD. Explicit foreign
currency prefixes and an item's structured currency override that site default.
This convention is deliberately not applied to Facebook prices.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import cast

from carl.core.composed_projection import ScalarFieldValues
from carl.core.models import JsonValue

_MONEY = re.compile(
    r"^(?P<prefix>[A-Za-z]{3}|US\s*\$|CA\s*\$|C\s*\$|AU\s*\$|A\s*\$|[$£€])?"
    + r"\s*(?P<amount>(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)"
    + r"\s*(?P<suffix>[A-Za-z]{3})?$"
)
_CURRENCIES = {
    "US$": "USD",
    "CA$": "CAD",
    "C$": "CAD",
    "AU$": "AUD",
    "A$": "AUD",
    "£": "GBP",
    "€": "EUR",
}


def ebay_price_value(displayed: JsonValue, *, currency: JsonValue = None) -> JsonValue:
    """Structure one unambiguous amount; keep ranges and unknown formats verbatim."""

    if not isinstance(displayed, str):
        return displayed
    match = _MONEY.fullmatch(displayed.strip())
    if match is None:
        return displayed
    prefix = re.sub(r"\s+", "", match.group("prefix") or "").upper()
    suffix = (match.group("suffix") or "").upper()
    explicit = _CURRENCIES.get(prefix, prefix if len(prefix) == 3 else None)
    if explicit and suffix and explicit != suffix:
        return displayed
    explicit = explicit or suffix or None
    supplied = currency.strip().upper() if isinstance(currency, str) else None
    if supplied and re.fullmatch(r"[A-Z]{3}", supplied) is None:
        supplied = None
    if explicit and supplied and explicit != supplied:
        return displayed
    amount = Decimal(match.group("amount").replace(",", ""))
    result: dict[str, JsonValue] = {
        "amount_decimal": format(amount, "f"),
        "formatted_amount": displayed,
    }
    selected_currency = explicit or supplied or ("USD" if prefix == "$" else None)
    if selected_currency is not None:
        result["currency"] = selected_currency
    return result


def normalize_ebay_scalar_fields(values: ScalarFieldValues) -> ScalarFieldValues:
    """Upgrade retained eBay snapshots without changing their review facts."""

    updates: dict[str, JsonValue] = {"price": ebay_price_value(values.price)}
    if isinstance(values.last_sale, dict):
        sale = dict(cast(dict[str, JsonValue], values.last_sale))
        price = ebay_price_value(sale.get("sold_price"))
        if isinstance(price, dict):
            sale["sold_price_value"] = price
        updates["last_sale"] = sale
    return values.model_copy(update=updates)
