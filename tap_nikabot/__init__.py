#!/usr/bin/env python3
import requests
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema


LOGGER = singer.get_logger()
BASE_URL = "https://api.nikabot.com"
MAX_API_PAGES = 10000
DEFAULT_CONFIG = {
    "page_size": 1000
}
REQUIRED_CONFIG_KEYS = ["access_token"]


def user_schema():
    """ Load user schema from swagger definition """
    response = requests.get(f"{BASE_URL}/v2/api-docs?group=public")
    response.raise_for_status()
    swagger = response.json()
    schema = Schema.from_dict(swagger["definitions"]["UserDTO"])

    stream_id = "users"
    key_properties = ["id"]
    stream_metadata = metadata.get_standard_metadata(
        schema.to_dict(),
        stream_id,
        key_properties,
        valid_replication_keys=["updated_at"],
    )
    catalog_entry = CatalogEntry(
            tap_stream_id=stream_id,
            stream=stream_id,
            schema=schema,
            key_properties=key_properties,
            metadata=stream_metadata,
            replication_key="updated_at",
            replication_method=None,
        )
    return catalog_entry


def discover():
    streams = [user_schema()]
    return Catalog(streams)


def user_data(access_token, page_size):
    for page in range(MAX_API_PAGES):
        params = {"limit": page_size, "page": page}
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(f"{BASE_URL}/api/v1/users", params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        if len(data["result"]) == 0:
            break
        yield data["result"]


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = False

        singer.write_schema(
            stream_name=stream.tap_stream_id, schema=stream.schema.to_dict(), key_properties=stream.key_properties,
        )

        max_bookmark = ""
        for rows in user_data(config["access_token"], config["page_size"]):
            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, rows)
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: rows[-1][bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, max([row[bookmark_column] for row in rows]))
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
    return


@utils.handle_top_exception(LOGGER)
def main():
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
