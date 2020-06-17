#!/usr/bin/env python3
from functools import partial
from typing import Dict, Any, List, Optional
import requests
import singer
from singer import utils, Schema, metadata
from singer.catalog import Catalog, CatalogEntry
from . import client
from .streams import groups, roles, users, teams
from .typing import JsonResult

LOGGER = singer.get_logger()
DEFAULT_CONFIG = {"page_size": 1000}
REQUIRED_CONFIG_KEYS = ["access_token"]


def make_catalog_entry(
    schema: Schema, stream_id: str, key_properties: List[str], replication_key: Optional[str] = None
) -> CatalogEntry:
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


def discover() -> Catalog:
    swagger = client.fetch_swagger_definition()
    schemas = [
        users.get_catalog_entry(make_catalog_entry, swagger),
        roles.get_catalog_entry(make_catalog_entry, swagger),
        groups.get_catalog_entry(make_catalog_entry, swagger),
        teams.get_catalog_entry(make_catalog_entry, swagger),
    ]
    return Catalog(schemas)


def sync(config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog) -> None:
    """ Sync data from tap source """
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {config['access_token']}"})
    fetch_with_auth = partial(client.fetch_one_page, session, config["page_size"])
    fetch_with_paging = partial(client.fetch_all_pages, fetch_with_auth)
    streams = {
        "users": users.get_records,
        "roles": roles.get_records,
        "groups": groups.get_records,
        "teams": teams.get_records,
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
        for rows in stream(fetch_with_paging):
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
