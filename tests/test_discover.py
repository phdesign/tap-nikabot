import json
from tap_nikabot import discover

SWAGGER_DEFINITION = '{"swagger":"2.0","definitions":{"RoleDTO":{"type":"object","required":["id","name","team_id"],"properties":{"id":{"type":"string"},"name":{"type":"string"},"team_id":{"type":"string"}},"title":"RoleDTO"},"UserDTO":{"type":"object","required":["deleted","id","is_admin","is_checkin_excluded","is_nikabot_admin","name","presence","team_id","tz_offset","user_id"],"properties":{"create_date":{"type":"string","format":"date-time"},"created_at":{"type":"string","format":"date-time"},"custom_ref":{"type":"string"},"deleted":{"type":"boolean"},"groups":{"type":"array","items":{"type":"string"}},"id":{"type":"string"},"is_admin":{"type":"boolean"},"is_checkin_excluded":{"type":"boolean"},"is_nikabot_admin":{"type":"boolean"},"is_restricted":{"type":"boolean"},"is_ultra_restricted":{"type":"boolean"},"last_eom_reminder":{"type":"string","format":"date-time"},"last_feedback":{"type":"string","format":"date-time"},"name":{"type":"string"},"presence":{"type":"string"},"role":{"type":"string"},"team_id":{"type":"string"},"tz":{"type":"string"},"tz_label":{"type":"string"},"tz_offset":{"type":"integer","format":"int32"},"updated_at":{"type":"string","format":"date-time"},"user_id":{"type":"string"}},"title":"UserDTO"}}}'  # noqa


def should_return_catalog(requests_mock):
    requests_mock.get("https://api.nikabot.com/v2/api-docs?group=public", json=json.loads(SWAGGER_DEFINITION))
    catalog = discover()
    assert len(catalog.streams) == 2
    assert catalog.streams[0].stream == "users"
    assert catalog.streams[0].tap_stream_id == "users"
    assert catalog.streams[0].schema.properties["name"] is not None
