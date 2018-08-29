import requests
import json
from datetime import datetime, timedelta, timezone
from collections import namedtuple
from functools import partial
import pendulum
import singer
from singer.utils import strftime
from .http import paginate, create_get_request
from .transform import transform_dts, format_leads
from .schemas import IDS

LOGGER = singer.get_logger()

PATHS = {
    IDS.CUSTOM_FIELDS: "/custom_fields/lead/",
    IDS.LEADS: "/lead/",
    IDS.ACTIVITIES: "/activity/",
    IDS.TASKS: "/task/",
    IDS.USERS: "/user/",
    IDS.EVENT_LOG: "/event/",
}

Stream = namedtuple("Stream", ["tap_stream_id", "sync_fn"])

BOOK_KEYS = {
    IDS.CUSTOM_FIELDS: "date_updated",
    IDS.LEADS: "date_updated",
    IDS.ACTIVITIES: "date_created",
    IDS.TASKS: "date_updated",
    IDS.USERS: "date_updated",
    IDS.EVENT_LOG: "date_updated",
}

FORMATTERS = {
    IDS.LEADS: format_leads,
}


def bookmark(tap_stream_id):
    return [tap_stream_id, BOOK_KEYS[tap_stream_id]]


def new_max_bookmark(max_bookmark, records, key):
    for record in records:
        if record[key] > max_bookmark:
            max_bookmark = record[key]
    return max_bookmark


def format_dts(tap_stream_id, ctx, records):
    schema = [
        stream for stream
        in ctx.catalog.streams
        if stream.tap_stream_id == tap_stream_id
    ][0].schema
    paths = ctx.schema_dt_paths[tap_stream_id]
    return transform_dts(records, paths, schema)


def metrics(tap_stream_id, page):
    with singer.metrics.record_counter(tap_stream_id) as counter:
        counter.increment(len(page))


def write_records(tap_stream_id, page):
    singer.write_records(tap_stream_id, page)
    metrics(tap_stream_id, page)


def create_leads_request(ctx):
    start_date = ctx.update_start_date_bookmark(bookmark(IDS.LEADS))
    # date_updated>= has precision to the minute
    formatted_start = pendulum.parse(start_date).strftime("%Y-%m-%dT%H:%M")
    query = 'date_updated>="{}" sort:date_updated'.format(formatted_start)
    return create_request(IDS.LEADS, params={"query": query})


def paginated_sync(tap_stream_id, ctx, request, start_date):
    _request = request
    bookmark_key = BOOK_KEYS[tap_stream_id]
    offset = [tap_stream_id, "skip"]
    skip = ctx.get_offset(offset) or 0
    max_bookmark = start_date
    formatter = FORMATTERS.get(tap_stream_id, (lambda x: x))
    while True:
        try:
            for page in paginate(ctx.client, tap_stream_id, _request, skip=skip):
                records = formatter(format_dts(tap_stream_id, ctx, page.records))
                to_write = [rec for rec in records if rec[bookmark_key] >= start_date]
                max_bookmark = new_max_bookmark(max_bookmark, records, bookmark_key)
                write_records(tap_stream_id, to_write)
                ctx.set_offset(offset, page.next_skip)
                LOGGER.info("Current Bookmark and Offset: `{}`, `{}`".format(
                    ctx.get_bookmark(bookmark(tap_stream_id)),
                    page.next_skip))
                LOGGER.info("Current Max Bookmark: `{}`".format(
                    max_bookmark))
                ctx.write_state()
            break
        except requests.Timeout as e:
            LOGGER.info("Request timed out after 5 seconds: stream={}, skip={}".format(
                tap_stream_id, ctx.get_offset(offset)))
            LOGGER.info("Setting bookmark to `{}` and restarting pagination.".format(
                max_bookmark))
            skip = 0
            ctx.clear_offsets(tap_stream_id)
            ctx.set_bookmark(bookmark(tap_stream_id), max_bookmark)
            if IDS.LEADS != tap_stream_id:
                _request = create_request(tap_stream_id)
            else:
                _request = create_leads_request(ctx)
            ctx.write_state()
        except Exception as e:
            # There may be streams other than `leads` that will run into
            # `max_skip` errors but YAGNI. We can make the tap more
            # complicated once we have an extant need for it.
            if 'max_skip = ' in str(e) and tap_stream_id == IDS.LEADS:
                LOGGER.info("Hit max_skip error. Setting bookmark to `{}` and restarting pagination.".format(
                    max_bookmark))
                skip = 0
                ctx.clear_offsets(tap_stream_id)
                ctx.set_bookmark(bookmark(tap_stream_id), max_bookmark)
                _request = create_leads_request(ctx)
                ctx.write_state()
            else:
                raise
    ctx.clear_offsets(tap_stream_id)
    ctx.set_bookmark(bookmark(tap_stream_id), max_bookmark)
    ctx.write_state()


def create_request(tap_stream_id, params=None):
    params = params or {}
    return create_get_request(PATHS[tap_stream_id], params=params)


def fetch_all(tap_stream_id, ctx):
    """Does a basic, paginated request to this stream's path and returns
    all records from all pages."""
    request = create_request(tap_stream_id)
    records = []
    for page in paginate(ctx.client, tap_stream_id, request):
        records += page.records
    return records


def basic_paginator(tap_stream_id, ctx):
    request = create_request(tap_stream_id)
    start_date = ctx.update_start_date_bookmark(bookmark(tap_stream_id))
    paginated_sync(tap_stream_id, ctx, request, start_date)


def sync_leads(ctx):
    request = create_leads_request(ctx)
    start_date = ctx.update_start_date_bookmark(bookmark(IDS.LEADS))
    paginated_sync(IDS.LEADS, ctx, request, start_date)


def sync_activities(ctx):
    start_date_str = ctx.update_start_date_bookmark(bookmark(IDS.ACTIVITIES))
    start_date = pendulum.parse(start_date_str)
    # 24 hours of activities essentially allows us to capture updates to
    # calls that are in progress _while_ an extraction is happening, no
    # matter the replication frequency or call duration.
    offset_secs = ctx.config.get("activities_window_seconds", (60 * 60 * 24))
    LOGGER.info("Using offset seconds {}".format(offset_secs))
    start_date -= timedelta(seconds=offset_secs)
    # date_created__gt has precision to the second
    formatted_start = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    params = {"date_created__gt": formatted_start}
    request = create_request(IDS.ACTIVITIES, params=params)
    paginated_sync(IDS.ACTIVITIES, ctx, request, formatted_start)


def sync_event_log(ctx):
    start_date = ctx.update_start_date_bookmark(bookmark(IDS.EVENT_LOG))
    # date_updated__gt has sub-second precision, but truncate to the second
    # just to be sure
    formatted_start = pendulum.parse(start_date).strftime("%Y-%m-%dT%H:%M:%S")
    max_bookmark = start_date
    cursor_next = None
    while True:
        params = {"date_updated__gte": formatted_start}
        if cursor_next:
            params["_cursor"] = cursor_next
        request = create_request(IDS.EVENT_LOG, params)
        response = ctx.client.request_with_handling(IDS.EVENT_LOG, request)
        if not response["data"]:
            break
        events = format_dts(IDS.EVENT_LOG, ctx, response["data"])
        for event in events:
            # According to the API docs 'data' and 'previous_data' are
            # only available to 'organization admins' so this needs to be
            # conditional. https://developer.close.io/#event-log
            if 'data' in event:
                event["data"] = json.dumps(event["data"])
                event["previous_data"] = json.dumps(event["previous_data"])
        write_records(IDS.EVENT_LOG, events)
        max_bookmark = new_max_bookmark(max_bookmark, events, "date_updated")
        cursor_next = response["cursor_next"]
        if not cursor_next:
            break
    # The Close.io docs indicate:
    # > To avoid missing recent events when paginating, we recommend to
    # > scan the latest five minutes of events.
    # Therefore we do not set the bookmark to any value that is in the last
    # five minutes.
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    five_minutes_ago = strftime(now - timedelta(minutes=5))
    max_bookmark = min(five_minutes_ago, max_bookmark)
    ctx.set_bookmark(bookmark(IDS.EVENT_LOG), max_bookmark)


def mk_basic_paginator(tap_stream_id):
    return Stream(tap_stream_id, partial(basic_paginator, tap_stream_id))

streams = [
    mk_basic_paginator(IDS.CUSTOM_FIELDS),
    Stream(IDS.LEADS, sync_leads),
    Stream(IDS.ACTIVITIES, sync_activities),
    mk_basic_paginator(IDS.TASKS),
    mk_basic_paginator(IDS.USERS),
    Stream(IDS.EVENT_LOG, sync_event_log),
]
stream_ids = [s.tap_stream_id for s in streams]
