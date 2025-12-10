"""
Client for interacting with the Meraki Dashboard API.

Provides methods to request camera snapshots from Meraki devices.
Handles authentication, API requests, and image retrieval.

Environment Variables:
    MERAKI_API_KEY: Meraki API token (optional if provided to constructor)
    MERAKI_CAMERA_SERIAL: Default camera device serial (optional, used in __main__)
"""

import base64
import os
import pathlib
import sys
import tempfile
import time
from datetime import datetime, timezone
from typing import Optional

import requests


class MerakiClient:
    """
    Client for interacting with the Meraki Dashboard API.

    Manages authentication and HTTP session for requesting camera snapshots.

    Attributes:
        BASE_URL: Base URL for the Meraki API v1 endpoint
        api_token: Meraki API token for authentication
        session: Requests session with pre-configured auth headers
    """

    BASE_URL = "https://api.meraki.com/api/v1"

    def __init__(self, api_token: str):
        """
        Initialize the Meraki API client.

        Args:
            api_token: Meraki Dashboard API token for authentication

        Raises:
            ValueError: If api_token is empty or None
        """
        if not api_token:
            raise ValueError("Missing Meraki API token")
        self.api_token = api_token

        # Create a persistent session with authentication headers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

    def create_url(self, endpoint: str) -> str:
        """
        Construct a full API URL from an endpoint path.

        Args:
            endpoint: API endpoint path (e.g., "devices/Q2XX-XXXX-XXXX/camera/generateSnapshot")

        Returns:
            Full URL string combining BASE_URL with the endpoint
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

        Requests snapshot generation from the Meraki API, then polls the returned URL
        to retrieve the image data. Snapshot generation is asynchronous.

        Args:
            device_serial: Serial number of the Meraki camera device (e.g., "Q2XX-XXXX-XXXX")
            timestamp: Optional ISO 8601 timestamp string. If None, requests current snapshot.
            fullframe: Whether to request a full-frame snapshot (default: True).

        Returns:
            Raw image bytes (typically JPEG format) from the camera snapshot

        Raises:
            requests.HTTPError: If the API request fails or returns an error status code
            requests.RequestException: If there's a network error or connection issue
            ValueError: If the API response doesn't contain a valid image URL
        """
        # Build request payload with optional parameters
        payload = {}
        if timestamp is not None:
            payload["timestamp"] = timestamp
        payload["fullframe"] = fullframe

        # Request snapshot generation from Meraki API
        endpoint = f"devices/{device_serial}/camera/generateSnapshot"
        url = self.create_url(endpoint)

        # Post with a timeout to avoid hangs and capture response body on error
        resp = self.session.post(url, json=payload, timeout=10)

        # If there's a 4xx/5xx, show body to help diagnose
        if resp.status_code >= 400:
            # try to show JSON error, fall back to text
            try:
                err = resp.json()
            except ValueError:
                err = resp.text
            raise requests.HTTPError(
                f"Snapshot generation failed: {resp.status_code} - {err}", response=resp
            )

        # Extract the image URL from the snapshot generation response
        snapshot_info = resp.json()
        image_url = snapshot_info.get("url")
        if not image_url:
            raise ValueError("API response missing 'url' field")

        expiry = snapshot_info.get("expiry")

        # Parse expiry if present so we don't poll past it
        exp_dt = None
        if expiry:
            try:
                # Convert "2025-12-08T15:26:54-08:00" into aware datetime
                exp_dt = datetime.fromisoformat(expiry)
                if exp_dt.tzinfo is None:
                    exp_dt = exp_dt.replace(tzinfo=timezone.utc)
            except Exception:
                exp_dt = None

        # Try polling the returned URL a few times (stop early if expiry passed)
        img_resp = None
        for _ in range(16):  # increase attempts if needed
            if exp_dt and datetime.now(tz=exp_dt.tzinfo) >= exp_dt:
                break
            try:
                img_resp = requests.get(image_url, timeout=15)
            except requests.RequestException:
                img_resp = None
            if img_resp is not None and img_resp.status_code == 200:
                return img_resp.content
            time.sleep(0.5)

        # If plain GET fails, try using the session as a fallback (some hosts may require auth)
        try:
            img_resp = self.session.get(image_url, timeout=15)
        except requests.RequestException as exc:
            raise requests.HTTPError(f"Failed to fetch snapshot URL: {exc}") from exc

        # Raise with body to aid debugging
        if img_resp.status_code >= 400:
            try:
                body = img_resp.json()
            except ValueError:
                body = img_resp.text
            raise requests.HTTPError(
                f"Fetching snapshot failed: {img_resp.status_code} - {body}",
                response=img_resp,
            )

        return img_resp.content


if __name__ == "__main__":
    """
    Example script that requests camera snapshots and saves the last successful
    snapshot as an HTML file with embedded base64 image.

    Prompts for API token and device serial if not provided via environment variables,
    requests 10 snapshots (one per second), and saves the last successful snapshot.
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

        # Ensure we write to a writable location (use temp or user home)
        out_dir = pathlib.Path(tempfile.gettempdir())
        out_file = out_dir / "snapshot.html"
        print("Writing snapshot to:", out_file, "cwd:", pathlib.Path.cwd())

        try:
            out_file.write_text(html, encoding="utf8")
        except PermissionError:
            # fallback: try user home
            home_file = pathlib.Path.home() / "snapshot.html"
            print("Permission denied; trying home:", home_file, file=sys.stderr)
            home_file.write_text(html, encoding="utf8")
