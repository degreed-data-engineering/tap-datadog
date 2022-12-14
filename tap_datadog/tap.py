"""datadog tap class."""

from pathlib import Path
from typing import List
import logging
import click
from singer_sdk import Tap, Stream
from singer_sdk import typing as th


from tap_datadog.streams import (
    AggregateLogs,
    Metric_Response_Time,
    SLO_History_US_Prod,
    SLO_History_EU_Prod,
    SLO_History_CA_Prod,
)

PLUGIN_NAME = "tap-datadog"

STREAM_TYPES = [ 
    AggregateLogs,
    Metric_Response_Time,
    SLO_History_US_Prod,
    SLO_History_EU_Prod,
    SLO_History_CA_Prod,
]

class TapDatadog(Tap):
    """datadog tap class."""

    name = "tap-datadog"
    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True, description="DD-API-KEY"),
        th.Property("app_key", th.StringType, required=True, description="DD-APP-KEY"),
        th.Property("start_date", th.StringType, required=True, description="start date to sync from"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams =  [stream_class(tap=self) for stream_class in STREAM_TYPES]

        return streams

# CLI Execution:
cli = TapDatadog.cli