import time
from typing import List, Dict, Any
import requests


def fetch_network_flows_json(api_url: str, offset: int, limit: int = 5000, max_retries: int = 3) -> List[Dict[str, Any]]:
    """
    Fetches one batch of network flows as a JSON payload from the Source REST API.
    Conforms to PRD Section 3.2.
    """
    params = {"offset": offset, "limit": limit}
    last_exception = None

    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(api_url, params=params, timeout=30)
            response.raise_for_status()
            # Parses the response directly into a Python list of dictionaries
            data = response.json()
            if isinstance(data, list):
                return data
            raise ValueError(f"Expected JSON list from {api_url}, got {type(data)}")
        except (requests.RequestException, ValueError) as e:
            last_exception = e
            if attempt < max_retries:
                time.sleep(1.0 * attempt)
            else:
                raise last_exception
