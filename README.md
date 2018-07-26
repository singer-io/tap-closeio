# tap-close-io

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from Close.io's [REST API](https://developer.close.io/)
- Extracts the following resources from Close.io:
  - [Activities](https://developer.close.io/#activities)
  - [Leads](https://developer.close.io/#leads)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Quick start

1. Install

    ```bash
    > pip install tap-closeio
    ```

2. Get your Close.io API Key

    Login to your Close.io account, navigate to your account settings and "Your API Keys". Generate a New API Key, you'll need it for the next step.

3. Create the config file

    Create a JSON file called `config.json` containing the api key you just
    generated 

    ```json
    {
        "start_date": "2010-01-01",
        "api_key": "your-api-token"
    }
    ```

    The `start_date` is the date at which the tap will begin syncing data. Ie.
    if there is data in your Close.io account older than `start_date`, it will
    not be synced.

4. Run the tap in discovery mode

    ```bash
    tap-closeio --config config.json --discover
    ```

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

5. Run the tap in sync mode

    ```bash
    tap-closeio --config config.json --properties catalog.json
    ```

## Event Logs

The event log endpoint returns two fields that are troublesome in terms of
describing them with a JSON schema and fitting them into tabular structures,
like PostgreSQL or Redshift. They are the `data` and `previous_data` fields.
These fields vary depending on the type of the event, meaning an event for a
lead will have a vastly different structure than an event for a task. Due to
the varying nature of these fields, the tap JSON-encodes the fields during
sync.

## Activities

The activities endpoint does not provide a way to filter data based on when an
activity was updated. Because of this, there is no way to have the tap sync
changes to previously-synced activities without syncing the entire data set
during every run. As an alternative, your configuration file can contain the
key `activities_window_seconds`. When provided, any activity which was created
`activities_window_seconds` seconds before the bookmark in the `state.json`
file will be synced. For example, if your configuration file includes

```json
{
    ...,
    "activities_window_seconds": 3600
}
```

and the previous run of tap synced activities up until 10am today, the next
sync will start syncing activities that were created at 9am today.

---

Copyright &copy; 2017 Stitch
