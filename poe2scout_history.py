
import json
import logging
import urllib.request
import urllib.parse
from datetime import datetime, timezone

BASE_URL = "https://api.poe2scout.com/poe2/Leagues"

CATEGORIES = [
    "currency", "fragments", "runes", "essences",
    "ultimatum", "expedition", "ritual", "vaultkeys",
    "breach", "abyss", "uncutgems", "lineagesupportgems",
    "delirium", "incursion", "idol", "verisium", "vaal"
]

DIVINE_ITEM_ID = 291


def fetch_json(url):
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "poe2-trade-collector/0.2",
            "Accept": "application/json"
        }
    )
    with urllib.request.urlopen(request, timeout=45) as response:
        return json.load(response)


def get_currencies(league):
    currencies = {}

    for category in CATEGORIES:
        page = 1

        while True:
            params = urllib.parse.urlencode({
                "category": category,
                "page": page,
                "perPage": 25,
                "dataPoints": 7,
                "frequencyHours": 24
            })

            url = (
                f"{BASE_URL}/{league}/Currencies/ByCategory"
                f"?{params}"
            )

            logging.info("Requesting POE2 Scout category %s: %s", category, url)
            data = fetch_json(url)

            for item in data.get("Items", []):
                item_id = item.get("ItemId")

                if item_id is not None:
                    currencies[item_id] = {
                        "item_id": item_id,
                        "name": item.get("Text"),
                        "api_id": item.get("ApiId"),
                        "category": category
                    }

            if page >= data.get("Pages", 1):
                break

            page += 1

    return list(currencies.values())


def get_daily_history(league, item_id):
    url = (
        f"{BASE_URL}/{league}/Items/{item_id}"
        f"/DailyStatsHistory?dayCount=1000"
    )

    return fetch_json(url)


def collect_history(league, blob_service_client, container_name):
    currencies = get_currencies(league)

    divine_history = get_daily_history(league, DIVINE_ITEM_ID)

    divine_prices = {
        row["Time"]: row["Average"]
        for row in divine_history.get("DailyStats", [])
        if row.get("Average", 0) > 0
    }

    if not divine_prices:
        raise ValueError("No Divine Orb history available")

    container = blob_service_client.get_container_client(
        container_name
    )

    collected = 0
    failures = []

    for currency in currencies:
        item_id = currency["item_id"]

        try:
            history = get_daily_history(league, item_id)

            if history.get("BaseCurrencyApiId") != "exalted":
                raise ValueError(
                    f"Unexpected base currency for {item_id}"
                )

            daily_prices = []

            for row in history.get("DailyStats", []):
                date = row["Time"]
                divine_price = divine_prices.get(date)

                daily_prices.append({
                    **row,
                    "AverageDivines": (
                        row["Average"] / divine_price
                        if divine_price and row.get("Average") is not None
                        else None
                    )
                })

            result = {
                "league": league,
                "currency": currency,
                "retrieved_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "base_currency": "exalted",
                "normalized_currency": "divine",
                "has_more": history.get("HasMore", False),
                "daily_prices": daily_prices
            }

            blob_name = (
                f"poe2scout/{league}/daily/{item_id}.json"
            )

            container.upload_blob(
                name=blob_name,
                data=json.dumps(result),
                overwrite=True
            )

            collected += 1

        except Exception as exc:
            logging.exception(
                "Failed to collect item %s: %s",
                item_id,
                exc
            )
            failures.append(item_id)

    logging.info(
        "POE2 Scout history collected: %s currencies",
        collected
    )

    if failures:
        raise RuntimeError(
            f"Failed to collect {len(failures)} currencies: "
            f"{failures}"
        )

    return collected
