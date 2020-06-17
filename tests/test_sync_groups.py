import json
import logging
from unittest.mock import call
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from tap_nikabot import sync

LOGGER = logging.getLogger()
EMPTY_RESPONSE = '{"ok":true,"result":[]}'
GROUPS_RESPONSE = '{"ok":true,"result":[{"id":"f1b4b37cc2658672770b789f","team_id":"T034F9NPW","name":"TA Squad 5"},{"id":"3176700ac4f2203b825fae6c","team_id":"T034F9NPW","name":"Platform Toolkit"}]}'


def test_should_output_records(mock_stdout, requests_mock):
    requests_mock.get("https://api.nikabot.com/api/v1/groups?limit=1000&page=0", json=json.loads(GROUPS_RESPONSE))
    requests_mock.get("https://api.nikabot.com/api/v1/groups?limit=1000&page=1", json=json.loads(EMPTY_RESPONSE))
    config = {"access_token": "my-access-token", "page_size": 1000}
    state = {}
    catalog = Catalog(
        streams=[
            CatalogEntry(
                tap_stream_id="groups",
                stream="groups",
                schema=Schema.from_dict({}),
                key_properties=["id"],
                metadata=[{"breadcrumb": [], "metadata": {"selected": True}}],
            )
        ]
    )
    sync(config, state, catalog)
    mock_stdout.assert_has_calls(
        [
            call('{"type": "SCHEMA", "stream": "groups", "schema": {}, "key_properties": ["id"]}\n'),
            call(
                '{"type": "RECORD", "stream": "groups", "record": {"id": "f1b4b37cc2658672770b789f", "team_id": "T034F9NPW", "name": "TA Squad 5"}}\n'
            ),
            call(
                '{"type": "RECORD", "stream": "groups", "record": {"id": "3176700ac4f2203b825fae6c", "team_id": "T034F9NPW", "name": "Platform Toolkit"}}\n'
            ),
        ]
    )
    LOGGER.info.assert_called_once_with("Syncing stream: %s", "groups")
