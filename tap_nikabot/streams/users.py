from typing import Any, Callable, Iterator, List
from singer import CatalogEntry, Schema
from ..typing import JsonResult, MakeCatalogEntry


def get_catalog_entry(make_catalog_entry: MakeCatalogEntry, swagger: Any) -> CatalogEntry:
    schema = Schema.from_dict(swagger["definitions"]["UserDTO"])
    return make_catalog_entry(schema, "users", ["id"], "updated_at")


def get_records(fetch: Callable[[str], Iterator[List[JsonResult]]]) -> Iterator[List[JsonResult]]:
    for page in fetch("/api/v1/users"):
        yield page
