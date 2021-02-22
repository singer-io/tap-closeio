import time
from collections import namedtuple
import requests
from requests.auth import HTTPBasicAuth
from singer import metrics, utils
import singer

BASE_URL = "https://app.close.io/api/v1"
PER_PAGE = 100
LOGGER = singer.get_logger()


class RateLimitException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


def url(path):
    return _join(BASE_URL, path)


def create_get_request(path, **kwargs):
    return requests.Request(method="GET", url=url(path), **kwargs)


class Client():
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()
        self.auth = HTTPBasicAuth(config["api_key"], "")

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent
        request.auth = self.auth
        # This timeout was increased from 10 to 30 after receiving errors
        # that were being retried erroneously: requests.exceptions.ReadTimeout
        return self.session.send(request.prepare(), timeout=30.0)

    @utils.backoff((requests.exceptions.RequestException), utils.exception_is_4xx)
    def request_with_handling(self, tap_stream_id, request):
        with metrics.http_request_timer(tap_stream_id) as timer:
            resp = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
        # We're only interested in triggering the custom backoff code if
        # it's a 429. Everything else should die immediately or be retried
        # by the utility code.
        if resp.status_code != 429:
            if resp.status_code == 400:
                try:
                    message = "400 Response: {}".format(resp.json()['error'])
                except:
                    message = "400 Response. Unable to determine cause."
                raise Exception(message)
            resp.raise_for_status()
        json = resp.json()
        # if we're hitting the rate limit cap, sleep until the limit resets
        if resp.headers.get('X-Rate-Limit-Remaining') == "0":
            # close.io can return a fraction of a second sleep time which will
            # both fail to parse and sleep for 0 seconds. So we will sleep for
            # at least 1 second here after parsing the limit reset value
            limit_reset_time = int(float(resp.headers['X-Rate-Limit-Reset'])) or 1
            time.sleep(limit_reset_time)

        # if we're already over the limit, we'll get a 429
        # sleep for the rate_reset seconds and then retry
        if resp.status_code == 429:
            time.sleep(json["rate_reset"])
            return self.request_with_handling(tap_stream_id, request)
        resp.raise_for_status()
        return json

Page = namedtuple("Page", ("records", "skip", "next_skip"))


def paginate(client, tap_stream_id, request, *, skip=0):
    request.params = request.params or {}
    request.params["_limit"] = PER_PAGE
    while True:
        request.params["_skip"] = skip
        response = client.request_with_handling(tap_stream_id, request)
        next_skip = skip + len(response["data"])
        if 'total_results' in response:
            LOGGER.info("Retrieved page from offset `{}` of total_results `{}`.".format(
                skip, response['total_results']))
        yield Page(response["data"], skip, next_skip)
        if not response.get("has_more"):
            break
        skip = next_skip
