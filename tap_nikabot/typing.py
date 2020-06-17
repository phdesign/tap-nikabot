from typing import Dict, Any, List, Optional, Protocol
from singer import Schema, CatalogEntry

JsonResult = Dict[str, Any]


class MakeCatalogEntry(Protocol):
    def __call__(
        self, schema: Schema, stream_id: str, key_properties: List[str], replication_key: Optional[str] = None
    ) -> CatalogEntry:
        ...
