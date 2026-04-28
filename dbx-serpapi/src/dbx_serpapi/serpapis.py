from functools import cache
from typing import Any

import serpapi
from dbx_tools import configs
from serpapi.models import json

"""SerpAPI helpers for Google Maps search requests."""

_DEFAULT_GOOLE_LANGUAGE = "en"


def client(api_key: str | None = None) -> serpapi.Client:
    return _client(api_key) if api_key else _client_default()


def _client(api_key: str) -> serpapi.Client:
    return serpapi.Client(api_key=api_key)


@cache
def _client_default() -> serpapi.Client:
    api_key = configs.value("SERPAPI_API_KEY")
    if not isinstance(api_key, str) or not api_key:
        raise ValueError("SERPAPI_API_KEY required")
    return _client(api_key)


def _ll(lat: float, long: float, zoom: int) -> str:
    """Build SerpAPI `ll` value in `@lat,long,zoomz` format."""
    return f"@{lat},{long},{zoom}z"


def google_maps(
    q: str,
    lat: float,
    long: float,
    serpapi_client: serpapi.Client | None = None,
    **kwargs: Any,
) -> serpapi.SerpResults:
    """Run a Google Maps search through SerpAPI.

    Args:
        api_key: SerpAPI API key.
        search_term: Search query text for Google Maps.
        lat: Latitude for center point.
        long: Longitude for center point.
        google_domain: Google domain to query.
        hl: Response language code.
        zoom: Google Maps zoom level.

    Returns:
        Parsed SerpAPI response payload.
    """
    if not serpapi_client:
        serpapi_client = client()

    params = {
        "engine": "google_maps",
        "q": q,
        **kwargs,
        # "engine": "google_maps",
        # "type": "search",
        # "google_domain": google_domain,
        # "q": search_term,
        # "ll": _ll(lat, long, zoom),
        # "hl": hl,
    }
    zoom = params.pop("zoom", 14)
    ll = _ll(lat, long, zoom)
    params["ll"] = ll
    params.setdefault("google_domain", "google.com")
    params.setdefault("type", "search")
    params.setdefault("hl", _DEFAULT_GOOLE_LANGUAGE)
    return serpapi_client.search(params)


def google_maps_reviews(
    place_id: str,
    serpapi_client: serpapi.Client | None = None,
    **kwargs: Any,
) -> serpapi.SerpResults:
    """Fetch Google Maps reviews for a place through SerpAPI.

    Args:
        place_id: Google Maps place identifier.
        serpapi_client: Optional SerpAPI client to use.
        **kwargs: Additional SerpAPI parameters merged into the request.

    Returns:
        SerpAPI reviews response payload.
    """
    if not serpapi_client:
        serpapi_client = client()
    params = {
        "engine": "google_maps_reviews",
        "place_id": place_id,
        **kwargs,
    }
    params.setdefault("hl", _DEFAULT_GOOLE_LANGUAGE)
    return serpapi_client.search(params)


if __name__ == "__main__":
    serpapi_client = client()
    results = google_maps(
        q="racetrac",
        lat=34.238148,
        long=-84.227853,
        type="search",
        serpapi_client=serpapi_client,
    )
    print(json.dumps(results.as_dict(), indent=2))
