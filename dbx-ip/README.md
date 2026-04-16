# dbx-ip

IP metadata lookup helpers backed by `ipwho.is`.

## What It Provides

- `IpInfo`: dataclass for normalized IP metadata fields.
- `info(address=None, **kwargs)`: fetches and parses metadata for an IP address, hostname, or caller IP.

## Usage

```python
from dbx_ip.ips import info

result = info("8.8.8.8")
```
