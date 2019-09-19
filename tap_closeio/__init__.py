#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from . import streams as streams_
from .context import Context
from . import schemas

REQUIRED_CONFIG_KEYS = ["start_date", "api_key"]
LOGGER = singer.get_logger()


def has_access_to_event_log(ctx):
    request = streams_.create_request(schemas.IDS.EVENT_LOG)
    resp = ctx.client.prepare_and_send(request)
    if resp.status_code == 400:
        return False
    resp.raise_for_status()
    return True


def discover(ctx):
    LOGGER.info("Running discover")
    use_event_log = has_access_to_event_log(ctx)
    catalog = Catalog([])
    for tap_stream_id in streams_.stream_ids:
        if not use_event_log and tap_stream_id == schemas.IDS.EVENT_LOG:
            continue
        schema_dict = schemas.load_schema(ctx, tap_stream_id)
        schema = Schema.from_dict(schema_dict)
        mdata = metadata.get_standard_metadata(schema_dict,
                                               key_properties=schemas.PK_FIELDS[tap_stream_id])
        mdata = metadata.to_map(mdata)

        for field_name in schema_dict['properties'].keys():
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')

        catalog.streams.append(CatalogEntry(
            stream=tap_stream_id,
            tap_stream_id=tap_stream_id,
            key_properties=schemas.PK_FIELDS[tap_stream_id],
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))
    return catalog


def sync(ctx):
    LOGGER.info("Running sync")
    currently_syncing = ctx.state.get("currently_syncing")
    start_idx = streams_.stream_ids.index(currently_syncing) \
        if currently_syncing else 0
    streams = [s for s in streams_.streams[start_idx:]
               if s.tap_stream_id in ctx.selected_stream_ids]
    for stream in streams:
        ctx.state["currently_syncing"] = stream.tap_stream_id
        ctx.write_state()
        schemas.load_and_write_schema(ctx, stream.tap_stream_id)
        stream.sync_fn(ctx)
    ctx.state["currently_syncing"] = None
    ctx.write_state()


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    ctx = Context(args.config, args.state)
    if args.discover:
        discover(ctx).dump()
        print()
    else:
        ctx.catalog = args.catalog \
            if args.catalog else discover(ctx)
        sync(ctx)


def main():
    try:
        main_impl()
    except Exception as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        raise

if __name__ == "__main__":
    main()
