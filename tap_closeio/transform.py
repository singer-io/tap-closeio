import pendulum
import singer
from singer.utils import strftime

LOGGER = singer.get_logger()


class _PathItem():
    def iterate(self, item):
        raise NotImplementedError()


class DictKey(_PathItem):
    expected_type = dict
    def __init__(self, key):
        self.key = key
    def __repr__(self):
        return "<DictKey({})>".format(self.key)
    def __eq__(self, other):
        return self.key == other.key
    def iterate(self, item):
        yield self.key, item.get(self.key)


class _ListItems(_PathItem):
    expected_type = list
    def __repr__(self):
        return "<ListItems>"
    def iterate(self, item):
        for i, v in enumerate(item):
            yield i, v

ListItems = _ListItems()


def find_dt_paths(schema, path=None):
    """Given a schema, recursively finds all keys with a date-time format.

    Returns a list of lists, where each inner list represents the path in a
    record where a date-time would be found. For example, if the path were

        [DictKey("foo"), ListItems, DictKey("bar")]

    Then for a record matching this schema, a date-time value could be found
    with:

        record["foo"][0]["bar"]

    Note that ListItems is used to indicate there will be a list, rather than a
    dict. Hence if the path were

        [ListItems]

    This means that all items inside the list are date-times."""
    path = path or []
    found = []
    if schema.anyOf:
        if len([type
                for type
                in schema.anyOf
                if type.get('format')
                and type['format'] == 'date-time']):
            found.append(path)
    if schema.format == "date-time":
        found.append(path)
    elif schema.properties:
        for k, v in schema.properties.items():
            found += find_dt_paths(v, path + [DictKey(k)])
    elif schema.items:
        found += find_dt_paths(schema.items, path + [ListItems])
    return found


class TransformationException(Exception):
    def __init__(self, item, path, path_idx):
        super().__init__(
            "Unexpected type found. path {}; path_idx {}; actual type {};"
            .format(path, path_idx, type(item))
        )

def _check_type(item, path, path_idx):
    path_item = path[path_idx]
    if not isinstance(item, path_item.expected_type):
        raise TransformationException(item, path, path_idx)


def _is_any_of_path(path, schema):
    if len(path) == 1:
        try:
            return schema[path[0].key].anyOf
        except:
            try:
                return schema.properties[path[0].key].anyOf
            except:
                LOGGER.error("Failed to detect anyOf path")
                raise
    elif isinstance(path[0], _ListItems):
        if schema.properties:
            return _is_any_of_path(path[1:], schema.properties)
        return _is_any_of_path(path[1:], schema.items)
    elif isinstance(path[0], DictKey):
        return _is_any_of_path(path[1:], schema[path[0].key])

def _transform_impl(item, path, schema, path_idx=0):
    if not item:
        return item
    if path_idx == len(path):
        if _is_any_of_path(path, schema):
            LOGGER.debug("Found anyOf path `{}`".format(path))
            try:
                dt = pendulum.parse(item).in_timezone("UTC")
                LOGGER.debug(
                    "Successfully parsed anyOf path `{}`".format(
                        path))
                return strftime(dt)
            except:
                LOGGER.debug(
                    "Failed to parse anyOf path `{}`. Returning as string.".format(
                        path))
                LOGGER.debug("value `{}`".format(item))
                return item
        else:
            try:
                dt = pendulum.parse(item).in_timezone("UTC")
            except:
                LOGGER.error(
                    "Failed to parse non anyOf path `{}`".format(path))
                LOGGER.debug("value `{}`".format(item))
                raise
            return strftime(dt)
    _check_type(item, path, path_idx)
    path_item = path[path_idx]
    for k, v in path_item.iterate(item):
        if not v:
            continue
        item[k] = _transform_impl(v, path, schema, path_idx + 1)
    return item


def transform_dts(records, paths, schema):
    """Accepts a list of records and a list of paths and re-formats all
    date-times to RFC3339.

    `paths` is a list as output by the `find_dt_paths` function."""
    for path in paths:
        _transform_impl(records, [ListItems] + path, schema)
    return records


def format_leads(leads):
    """For a list of leads, removes the "custom" key from each lead and
    transforms all "custom.* fields into a new "custom_fields" list."""
    new_leads = []
    for lead in leads:
        custom_fields = []
        new_lead = {"custom_fields": custom_fields}
        for k, v in lead.items():
            if k.startswith("custom."):
                custom_id = k.split(".")[1]
                custom_fields.append({"id": custom_id, "value": v})
            elif k != "custom":
                new_lead[k] = v
        new_leads.append(new_lead)
    return new_leads
