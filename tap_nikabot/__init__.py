#!/usr/bin/env python3
from functools import partial
from typing import Dict, Any, List, Iterator
import requests
import singer
from singer import utils, Schema, metadata
from singer.catalog import Catalog, CatalogEntry

LOGGER = singer.get_logger()
DEFAULT_CONFIG = {"page_size": 1000}
REQUIRED_CONFIG_KEYS = ["access_token"]
BASE_URL = "https://api.nikabot.com"
MAX_API_PAGES = 10000


def fetch_swagger_definition() -> Any:
    response = requests.get(f"{BASE_URL}/v2/api-docs?group=public")
    response.raise_for_status()
    swagger = response.json()
    return swagger


def get_catalog_entry(schema: Schema, stream_id: str, key_properties: List[str], replication_key: str = None) -> CatalogEntry:
    stream_metadata = metadata.get_standard_metadata(
        schema.to_dict(),
        stream_id,
        key_properties,
        valid_replication_keys=[replication_key] if replication_key else None,
    )
    # Default to selected
    stream_metadata = metadata.to_list(metadata.write(metadata.to_map(stream_metadata), (), "selected", True))
    catalog_entry = CatalogEntry(
        tap_stream_id=stream_id,
        stream=stream_id,
        schema=schema,
        key_properties=key_properties,
        metadata=stream_metadata,
        replication_key=replication_key,
    )
    return catalog_entry


def get_user_catalog_entry(swagger: Any) -> CatalogEntry:
    schema = Schema.from_dict(swagger["definitions"]["UserDTO"])
    return get_catalog_entry(schema, "users", ["id"], "updated_at")


def get_role_catalog_entry(swagger: Any) -> CatalogEntry:
    schema = Schema.from_dict(swagger["definitions"]["RoleDTO"])
    return get_catalog_entry(schema, "roles", ["id"])


def discover() -> Catalog:
    swagger = fetch_swagger_definition()
    schemas = [get_user_catalog_entry(swagger), get_role_catalog_entry(swagger)]
    return Catalog(schemas)


def _fetch(session: requests.Session, page_size: str, url: str, page_number: int) -> Any:
    params = {"limit": page_size, "page": page_number}
    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()


def fetch_all(fetch) -> Iterator[List[Dict[str, Any]]]:
    for page_number in range(MAX_API_PAGES):
        result = fetch(page_number)
        if len(result["result"]) == 0:
            break
        yield result["result"]


def get_user_records(fetch) -> Iterator[List[Dict[str, Any]]]:
    for page in fetch_all(partial(fetch, f"{BASE_URL}/api/v1/users")):
        yield page


def get_role_records(fetch) -> Iterator[List[Dict[str, Any]]]:
    for page in fetch_all(partial(fetch, f"{BASE_URL}/api/v1/roles")):
        yield page


def sync(config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog) -> None:
    """ Sync data from tap source """
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {config['access_token']}"})
    fetch = partial(_fetch, session, config["page_size"])
    streams = {
        "users": get_user_records,
        "roles": get_role_records,
    }

    # Loop over selected streams in catalog
    for selected_stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream: %s", selected_stream.tap_stream_id)

        bookmark_column = selected_stream.replication_key
        is_sorted = False

        singer.write_schema(
            stream_name=selected_stream.tap_stream_id,
            schema=selected_stream.schema.to_dict(),
            key_properties=selected_stream.key_properties,
        )

        max_bookmark = ""
        stream = streams[selected_stream.tap_stream_id]
        for rows in stream(fetch):
            # write one or more rows to the stream:
            singer.write_records(selected_stream.tap_stream_id, rows)
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({selected_stream.tap_stream_id: rows[-1][bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, max([row[bookmark_column] for row in rows]))
        if bookmark_column and not is_sorted:
            singer.write_state({selected_stream.tap_stream_id: max_bookmark})


@utils.handle_top_exception(LOGGER)
def main() -> None:
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = dict(DEFAULT_CONFIG, **args.config)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(config, args.state, catalog)


if __name__ == "__main__":
    main()
