"""
Client for interacting with the Meraki Dashboard API.

This module provides a client interface for requesting camera snapshots from
Meraki devices via the Meraki Dashboard API. It handles authentication, API
requests, and image retrieval.

Environment Variables:
    MERAKI_API_KEY: Meraki API token for authentication (optional if provided to constructor)
    MERAKI_CAMERA_SERIAL: Default camera device serial number (optional, used in __main__)

Example:
    >>> client = MerakiClient(api_token="your_api_token")
    >>> image_bytes = client.request_snapshot(device_serial="Q2XX-XXXX-XXXX")
"""

import os
import time
import base64
import requests
from typing import Optional


class MerakiClient:
    """
    Client for interacting with the Meraki Dashboard API.

    This client provides methods to request camera snapshots from Meraki devices.
    It manages authentication headers and HTTP session handling for API requests.

    Attributes:
        BASE_URL: Base URL for the Meraki API v1 endpoint
        api_token: Meraki API token used for authentication
        session: Requests session object with pre-configured authentication headers

    Example:
        >>> client = MerakiClient(api_token="your_api_token")
        >>> snapshot = client.request_snapshot(device_serial="Q2XX-XXXX-XXXX")
    """

    BASE_URL = "https://api.meraki.com/api/v1"

    def __init__(self, api_token: str):
        """
        Initialize the Meraki API client.

        Args:
            api_token: Meraki Dashboard API token for authentication

        Raises:
            ValueError: If api_token is empty or None

        Example:
            >>> client = MerakiClient(api_token="your_api_token")
        """
        if not api_token:
            raise ValueError("Missing Meraki API token")
        self.api_token = api_token

        # Create a persistent session with authentication headers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_token}",
                "ContentType": "application/json",
            }
        )

    def create_url(self, endpoint: str) -> str:
        """
        Construct a full API URL from an endpoint path.

        Args:
            endpoint: API endpoint path (e.g., "devices/Q2XX-XXXX-XXXX/camera/generateSnapshot")

        Returns:
            Full URL string combining BASE_URL with the endpoint

        Example:
            >>> client = MerakiClient(api_token="token")
            >>> url = client.create_url("devices/123/camera/generateSnapshot")
            >>> print(url)
            https://api.meraki.com/api/v1/devices/123/camera/generateSnapshot
        """
        return f"{self.BASE_URL}/{endpoint}"

    def request_snapshot(
        self,
        device_serial: str,
        timestamp: Optional[str] = None,
        fullframe: bool = True,
    ) -> bytes:
        """
        Request a camera snapshot from a Meraki device.

        This method requests a snapshot generation from the Meraki API, then
        retrieves the actual image data from the returned URL. The snapshot
        generation is asynchronous, so the API returns a URL that can be used
        to fetch the image once it's ready.

        Args:
            device_serial: Serial number of the Meraki camera device (e.g., "Q2XX-XXXX-XXXX")
            timestamp: Optional ISO 8601 timestamp string for requesting a snapshot at a specific time.
                      If None, requests a snapshot at the current time.
            fullframe: Whether to request a full-frame snapshot (default: True).
                      If False, may return a cropped or optimized version.

        Returns:
            Raw image bytes (typically JPEG format) from the camera snapshot

        Raises:
            requests.HTTPError: If the API request fails or returns an error status code
            requests.RequestException: If there's a network error or connection issue

        Example:
            >>> client = MerakiClient(api_token="your_api_token")
            >>> # Request current snapshot
            >>> image_bytes = client.request_snapshot(device_serial="Q2XX-XXXX-XXXX")
            >>> # Request snapshot at specific time
            >>> image_bytes = client.request_snapshot(
            ...     device_serial="Q2XX-XXXX-XXXX",
            ...     timestamp="2024-01-01T12:00:00Z"
            ... )
        """
        # Build request payload with optional parameters
        payload = {}
        if timestamp is not None:
            payload["timestamp"] = timestamp
        payload["fullframe"] = fullframe

        # Request snapshot generation from Meraki API
        endpoint = f"devices/{device_serial}/camera/generateSnapshot"
        url = self.create_url(endpoint)

        resp = self.session.post(url, json=payload)
        resp.raise_for_status()

        # Extract the image URL from the snapshot generation response
        snapshot_info = resp.json()
        image_url = snapshot_info["url"]

        # Fetch the actual image data from the provided URL
        img_resp = self.session.get(image_url)
        img_resp.raise_for_status()
        return img_resp.content


if __name__ == "__main__":
    """
    Example script that requests camera snapshots and saves the last successful
    snapshot as an HTML file with embedded base64 image.

    This script will:
    1. Prompt for API token and device serial if not provided via environment variables
    2. Request 10 snapshots (one per second)
    3. Save the last successful snapshot as snapshot.html
    """

    def _read_input(prompt: str) -> str:
        """Read user input from stdin and strip whitespace."""
        val = input(prompt).strip()
        return val

    # Get API credentials from environment or prompt user
    api_token = os.getenv("MERAKI_API_KEY")
    if not api_token:
        api_token = _read_input("Enter Meraki API key: ")

    device_serial = os.getenv("MERAKI_CAMERA_SERIAL")
    if not device_serial:
        device_serial = _read_input("Enter camera serial: ")

    svc = MerakiClient(api_token=api_token)

    # Request multiple snapshots and keep the last successful one
    last_img = None

    for _ in range(10):
        time.sleep(1)
        try:
            last_img = svc.request_snapshot(device_serial=device_serial)
        except Exception as exc:
            print("Snapshot error:", exc)

    # Save the last successful snapshot as an HTML file with embedded image
    if last_img is not None:
        b64 = base64.b64encode(last_img).decode("utf8")
        html = f"""
        <html>
        <body>
            <img src="data:image/jpeg;base64,{b64}">
        </body>
        </html>
        """

        with open("snapshot.html", "w") as f:
            f.write(html)
