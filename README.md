# `tap-datadog`
This tap datadog was created by Degreed as a datadog to be used for extracting data via Meltano into defined targets


## tap-datadog

These are the steps required for using this repo as a 'datadog' for a Meltano extractor. Note: we will use tap-datadog as the example throughout the process.

1.  Being aware of case sensitivity, replace the following throughout the repo:

* `tap-datadog` >`tap-datadog` 
* `tap_datadog` > `tap_datadog`
* `TapDatadogStream` > `TapDatadogStream` (inside `streams.py`)

2. Update the following folders/files to:
* `tap_datadog` > `tap_datadog`
* `tap-datadog.sh` > `tap-datadog.sh`

3. Inside `streams.py` update TapDatadogStream with the authentication used for the tap-datadog api calls.  Note: all streams in streams.py work as a heirarchy further down. i.e. you can replace the http headers in another stream

4. Using `Events(TapDatadogStream)` as an example, build your first stream to be synced. There are comments to help identify what values to use 

For setting the `records_jsonpath` value in the stream, you can use a tool likle postman to make a sample call and view the response json.  After identifying what keys and values you need to extract, you will need to narrow down the json path. This is a helpful site that you can paste the response text in and help locate the correct path to use.  In this example, we want to only extract the `id` and `type` values inside `data`:

```json
{
    "meta": {
        "page": {
            "after": "293048209rudjkfjdsf"
        }
    },
    "data": [
        {
            "type": "error",
            "id": "234234324324234"
        },
        {
            "type": "log",
            "id": "2342123123"
        },
        {
            "type": "log",
            "id": "09823044ugkdf"
        }
    ],
    "links": {
        "next": "https://api.datadoghq.com/api/v2/logs/events?..."
    }
}
```

Using the link above and entering the value $.data[*], the correct fields are now displaying, confirming that is the correct path:

```json
[
  {
    "type": "error",
    "id": "234234324324234"
  },
  {
    "type": "log",
    "id": "2342123123"
  },
  {
    "type": "log",
    "id": "09823044ugkdf"
  }
]
```

For the schema, you can create the .json file and place it in the schemas/ folder, or you can create the schema on the fly using the eample in the Events stream

- **Option 1:** Adding the `events.json` file to the schemas/ folder:
```json
{
        "type": "object",
        "properties": {
                "id": {
                        "type": "string"
                },
                "type": {
                        "type": "string"
                }
        }
}
```

- **Option 2:** Defining schema using hte PropertiesList in the stream `class`: 
```python
schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
    ).to_dict()
```

5. In `tap.py` add each stream added in `streams.py` to `STREAM_TYPES` and define the configuration required:

```python
    config_jsonschema = th.PropertiesList(
        th.Property("api_token", th.StringType, required=False, description="api token for Basic auth"),
        th.Property("start_date", th.StringType, required=False, description="start date for sync"),
    ).to_dict()
```

6. After updating those components and confirming all references to `datadog` or `Datadog` have been updated, you can test the tap locally.

## Testing locally

To test locally, pipx poetry
```bash
pipx install poetry
```

Install poetry for the package
```bash
poetry install
```

To confirm everything is setup properly, run the following: 
```bash
poetry run tap-datadog --help
```

To run the tap locally outside of Meltano and view the response in a text file, run the following: 
```bash
poetry run tap-datadog > output.txt 
```

A full list of supported settings and capabilities is available by running: `tap-datadog --about`

## Config Guide

To test locally, create a `config.json` with required config values in your tap_datadog folder (i.e. `tap_datadog/config.json`)

```json
{
  "api_key": "$DD_API_KEY",
  "app_key": "$DD_APP_KEY",
  "start_date": "2022-10-05T00:00:00Z"
}
```

**note**: It is critical that you delete the config.json before pushing to github.  You do not want to expose an api key or token 
### Add to Meltano 

The provided `meltano.yml` provides the correct setup for the tap to be installed in the data-houston repo.  

At this point you should move all your updated tap files into its own tap-datadog github repo. You also want to make sure you update in the `setup.py` the `url` of the repo for you tap.

Update the following in meltano within the data-houston repo with the new tap-datadog credentials/configuration.

```yml
plugins:
  extractors:
  - name: tap-datadog
    namespace: tap_datadog
    pip_url: git+https://github.com/degreed-data-engineering/tap-datadog
    capabilities:
    - state
    - catalog
    - discover
    config:
      api_key: $DD_API_KEY
      app_key: $DD_APP_KEY
      start_date: '2022-10-05T00:00:00Z'
 ```

To test in data-houston, run the following:
1. `make meltano` - spins up meltano
2. `meltano install extractor tap-datadog` - installs the tap
3. `meltano invoke tap-datadog --discover > catalog.json` - tests the catalog/discovery
3. `meltano invoke tap-datadog > output.txt` - runs tap with .txt output in `meltano/degreed/`

That should be it! Feel free to contribute to the tap to help add functionality for any future sources
## Singer SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/index.html) for more instructions on how to use the Singer SDK to 
develop your own taps and targets.