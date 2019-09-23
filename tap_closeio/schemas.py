#!/usr/bin/env python3
import os
import singer
from singer import utils


class IDS():
    CUSTOM_FIELDS = "custom_fields"
    LEADS = "leads"
    ACTIVITIES = "activities"
    TASKS = "tasks"
    USERS = "users"
    EVENT_LOG = "event_log"

PK_FIELDS = {
    IDS.CUSTOM_FIELDS: ["id"],
    IDS.LEADS: ["id"],
    IDS.ACTIVITIES: ["id"],
    IDS.TASKS: ["id"],
    IDS.USERS: ["id"],
    IDS.EVENT_LOG: ["id"],
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(ctx, tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(ctx, sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema


def load_and_write_schema(ctx, tap_stream_id):
    schema = load_schema(ctx, tap_stream_id)
    singer.write_schema(tap_stream_id, schema, PK_FIELDS[tap_stream_id])
