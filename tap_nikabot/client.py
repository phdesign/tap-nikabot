from typing import Any, cast, Callable, Iterator, List
import requests
from tap_nikabot.typing import JsonResult

BASE_URL = "https://api.nikabot.com"
MAX_API_PAGES = 10000


def fetch_swagger_definition() -> Any:
    response = requests.get(f"{BASE_URL}/v2/api-docs?group=public")
    response.raise_for_status()
    swagger = response.json()
    return swagger


def fetch_one_page(session: requests.Session, page_size: str, page_number: int, url: str) -> JsonResult:
    params = {"limit": page_size, "page": page_number}
    response = session.get(BASE_URL + url, params=params)
    response.raise_for_status()
    return cast(JsonResult, response.json())


def fetch_all_pages(fetch: Callable[[int, str], JsonResult], url: str) -> Iterator[List[JsonResult]]:
    for page_number in range(MAX_API_PAGES):
        result = fetch(page_number, url)
        if len(result["result"]) == 0:
            break
        yield result["result"]
