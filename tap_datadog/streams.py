"""Stream class for tap-datadog."""
from doctest import FAIL_FAST
import logging
import sys

import base64
import calendar
import json
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing
from functools import cached_property
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator


from singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    get_starting_replication_value,
    get_state_partitions_list,
    get_writeable_state_dict,
    increment_state,
    log_sort_error,
    reset_state_progress_markers,
    write_replication_key_signpost,
    write_starting_replication_value,
)

from singer_sdk import Tap, Stream

import requests

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class TapDatadogStream(RESTStream):
    """Datadog stream class."""
    
    _LOG_REQUEST_METRIC_URLS: bool = True
    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return f"https://api.datadoghq.com"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        headers["Accept"] = "application/json"
        return headers

    @property
    def authenticator(self):
        http_headers = {}

        # If only api_token is provided, use "Basic xyzabcdlmnopqrszyzabcdefgh" authentication
        http_headers["DD-API-KEY"] = self.config.get("api_key")
        http_headers["DD-APPLICATION-KEY"] = self.config.get("app_key")

        return SimpleAuthenticator(stream=self, auth_headers=http_headers)

class AggregateLogs(TapDatadogStream):
    name = "aggregate_logs" # Stream name 
    path = "/api/v2/logs/analytics/aggregate" # API endpoint after base_url 
    rest_method = "POST"
    #primary_keys = ["id"]

    
    records_jsonpath = "$.data.buckets.[*]" # https://jsonpath.com Use requests response json to identify the json path 
    replication_key = None
    schema_filepath = SCHEMAS_DIR / "aggregate_logs.json"  # Optional: use schema_filepath with .json inside schemas/ 

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        """Define request parameters to return"""

        todayUTC = datetime.now(timezone.utc).date()
        yesterdayUTC = todayUTC - timedelta(1)
        
        from_date = f"{yesterdayUTC}T00:00:00+03:00"
        to_date = f"{yesterdayUTC}T23:12:59+03:00"

        payload = {"compute": [{"aggregation": "count", "type": "total" }, { "aggregation": "sum", "type": "total", "metric": "@Properties.Elapsed" } ], "filter": { "query": "source:degreed.api @MessageTemplate:\"HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0000} ms\" host: api.degreed.com OR api.eu.degreed.com OR api.ca.degreed.com", "from": from_date, "to": to_date, "indexes": [ "main" ] }, "group_by": [ { "facet": "status" }, { "facet": "host" }, { "facet": "@http.status_code" }, { "facet": "@Properties.OrganizationId" } ] }
        return payload



# def blarg():
#     return 1,2
# hello,bye = blarg()
# print(hello,bye)
class SLO_History(TapDatadogStream):        
# weekly (not daily)
# monday - sunday
# start at August 
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

        self.slo_id = 'e96fa5aa00dc57af8718c8e7044b0f51' # prod-us

        self.full_sync_date = self.config["start_date"]
        self.current_epoch = int(time.time())


        self.full_sync_next_page = 1662619320

        self.full_sync_from_ts = 0
        self.full_sync_to_ts = 0

        self.next_page_token_epoch = 0
        self.end_of_month_limit_epoch = 0

        self.logger.info(f'FULL_SYNC_DATA')
        self.logger.info(self.full_sync_date)
        self.logger.info(f'CURRENT_TIME')
        self.logger.info(self.current_epoch)
    


        self.first_of_month_epoch, self.to_time_epoch = self._get_epoch_date_values()
        #self.replication_value_test = self.get_starting_replication_key_value(TapDatadogStream)


        self.first_run = True
        self.slo_date_overreach = False
        
    # config_start_date = TapDatadogStream.config.get("start_date")    

    name = "slo_history" # Stream name 
    
    rest_method = "GET"
    
    def _get_epoch_date_values(self):
        today = datetime.today()

        if today.day - 1 < 1:
            if today.month == 1:
                year = today.year - 1 
                month = 12
            else: 
                year = today.year
                month = today.month - 1

            first_of_month_date = datetime(year, month, 1, 0, 0, 0)
            first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

            last_day_month = calendar.monthrange(year, month)[1]
            to_time_date = datetime(year, month, last_day_month, 23, 59, 59)
            to_time_epoch = calendar.timegm(to_time_date.timetuple())
        
        else: 
            first_of_month_date = datetime(today.year, today.month, 1, 0, 0, 0)
            first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

            to_time_date = datetime(today.year, today.month, today.day - 1, 23, 59, 59)
            to_time_epoch = calendar.timegm(to_time_date.timetuple())

            return first_of_month_epoch, to_time_epoch

    slo_id = 'e96fa5aa00dc57af8718c8e7044b0f51' # prod-us
    

    #path = f"/api/v1/slo/{slo_id}/history?from_ts={first_of_month_epoch}&to_ts={to_time_epoch}" # API endpoint after base_url 
    path = f"/api/v1/slo/{slo_id}/history"
    records_jsonpath = "$.data" # https://jsonpath.com Use requests response json to identify the json path 
    #next_page_token_jsonpath = "$.data.next_page_key"  # Or override `get_next_page_token`.
    #records_jsonpath = "$[*]" # https://jsonpath.com Use requests response json to identify the json path 
    
    next_page_token = 0

    #primary_keys = ["type_id"]
    replication_key = "to_ts"
    schema_filepath = SCHEMAS_DIR / "slo_history.json"  # Optional: use schema_filepath with .json inside schemas/ 
    


    def _get_first_of_month_epoch(self, epoch):

        date_time = datetime.fromtimestamp(epoch)  
        date_split = str(date_time).split(' ') 
        year, month, day = date_split[0].split('-')

        first_day_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
        first_day_of_month_date_epoch  = calendar.timegm(first_day_of_month_date.timetuple())

        return first_day_of_month_date_epoch


    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        # params["from_ts"] = self.first_of_month_epoch
        # params["to_ts"] = self.to_time_epoch


        if "to_ts" not in self.stream_state:
            if self.first_run:
                self.logger.info("FULL STREAM")
                # Get Start date for initial full sync
                year, month, day = self.full_sync_date.split('-')

                # get last day of the start_date month
                last_day_month = calendar.monthrange(int(year), int(month))[1]

                # get epoch for last day of the month
                last_day_month_date = datetime(int(year), int(month), last_day_month, 0, 0, 0)
                self.end_of_month_limit_epoch  = calendar.timegm(last_day_month_date.timetuple())
                ####

                first_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
                first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())


                config_start_date = datetime(int(year), int(month), int(day), 0, 0, 0)
                self.full_sync_from_ts = calendar.timegm(config_start_date.timetuple())
                
                self.full_sync_to_ts = self.full_sync_from_ts  + 86400
                
                params["from_ts"] = first_of_month_epoch
                params["to_ts"] = self.full_sync_to_ts

                self.first_run = False
            else: 
                
                self.full_sync_to_ts = self.full_sync_to_ts + 86400

                first_of_month_epoch = self._get_first_of_month_epoch(self.full_sync_to_ts)
                
                self.logger.info("******NON FULL STREAM******")
                self.logger.info(f"first of month: {first_of_month_epoch}")
                self.logger.info(f"full sync to ts: {self.full_sync_to_ts}")
                params["from_ts"] = first_of_month_epoch
                params["to_ts"] = self.full_sync_to_ts
                
                if self.full_sync_to_ts + 86400 >= self.current_epoch:
                    self.slo_date_overreach = True





        self.logger.info("NEXTPAGE_TOKEN")
        self.logger.info(self.full_sync_next_page)
        # next_page_token = self.full_sync_next_page


        # self.logger.info("replication_key_value****")
        # self.logger.info(self.get_starting_replication_key_value(TapDatadogStream))


        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:


        row["slo_id"] = self.slo_id
       # row["last_sync_epoch"] = 1666483188
        row["next_page_key"] = self.full_sync_next_page
        self.next_page_token = self.full_sync_next_page
        

        if "to_ts" in self.stream_state:
            self.logger.info("Found to_ts state value")
            
        self.logger.info(row)
                
        return row


    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.



        if self.slo_date_overreach == True:
            self.logger.info("No more next page token")

            return None

        self.logger.info("Next page token generated")
        next_page_token = self.full_sync_to_ts

        self.logger.info(next_page_token)
        return next_page_token
 
   
 #{"type": "STATE", "value": {"bookmarks": {"slo_history": {"last_record": "2017-07-07T10:20:00Z"}}}}
    # schema = th.PropertiesList(
    #     th.Property("to_ts", th.NumberType),
    #     th.Property("type_id", th.NumberType),
    #     th.Property(
    #         "thresholds", 
    #         th.ObjectType(
    #             th.Property(
    #                 "30d",
    #                 th.ObjectType(
    #                     th.Property("target", th.NumberType),
    #                     th.Property("target_display", th.StringType),
    #                     th.Property("timeframe", th.StringType)
    #                 )
    #             )
    #         )
    #     ),
    #     th.Property(
    #         "overall",
    #         th.ObjectType(
    #             th.Property("name", th.StringType),
    #             th.Property("sli_value", th.NumberType),
    #             th.Property(
    #                 "precision",
    #                 th.ObjectType(
    #                     th.Property("30d", th.NumberType),
    #                     th.Property("custom", th.NumberType)
    #                 )
    #             ),
    #         th.Property("monitor_modified", th.NumberType),
    #         th.Property("span_precision", th.NumberType),
    #         th.Property("monitor_type", th.StringType)
    #         )
    #     ),
    #     th.Property("from_ts", th.NumberType),
    #     th.Property(
    #         "slo",
    #         th.ObjectType(
    #         th.Property("description", th.StringType),
    #         th.Property(
    #             "creator",
    #             th.ObjectType(
    #                 th.Property("handle", th.StringType),
    #                 th.Property("name", th.StringType),
    #                 th.Property("email", th.StringType)

    #             )
    #         ),
    #         th.Property(
    #             "thresholds",
    #             th.ObjectType(
    #                 th.Property("target", th.NumberType),
    #                 th.Property("target_display", th.StringType),
    #                 th.Property("timeframe", th.StringType)

    #             )
    #         ),
    #         th.Property("type_id", th.NumberType),
    #         th.Property("id", th.StringType),
    #         th.Property("name", th.StringType),
    #         th.Property("created_at", th.NumberType),
    #         th.Property("tags", th.ArrayType(th.StringType)),
    #         th.Property("modified_at", th.NumberType),
    #         th.Property("type", th.StringType)
    #     )),
    #     th.Property("type", th.StringType)
    # ).to_dict()





    # For passing url parameters: 
    # def get_url_params(
    #     self, context: Optional[dict], next_page_token: Optional[Any]
    # ) -> Dict[str, Any]:






### Datadog to use for new stream 
# class DatadogStream(RESTStream):
#     """Datadog stream class."""

#     # TODO: Set the API's base URL here:
#     url_base = "https://api.mysample.com"

#     # OR use a dynamic url_base:
#     # @property
#     # def url_base(self) -> str:
#     #     """Return the API URL root, configurable via tap settings."""
#     #     return self.config["api_url"]

#     records_jsonpath = "$[*]"  # Or override `parse_response`.
#     next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.

#     @property
#     def authenticator(self) -> BasicAuthenticator:
#         """Return a new authenticator object."""
#         return BasicAuthenticator.create_for_stream(
#             self,
#             username=self.config.get("username"),
#             password=self.config.get("password"),
#         )

#     @property
#     def http_headers(self) -> dict:
#         """Return the http headers needed."""
#         headers = {}
#         if "user_agent" in self.config:
#             headers["User-Agent"] = self.config.get("user_agent")
#         # If not using an authenticator, you may also provide inline auth headers:
#         # headers["Private-Token"] = self.config.get("auth_token")
#         return headers

#     def get_next_page_token(
#         self, response: requests.Response, previous_token: Optional[Any]
#     ) -> Optional[Any]:
#         """Return a token for identifying next page or None if no more pages."""
#         # TODO: If pagination is required, return a token which can be used to get the
#         #       next page. If this is the final page, return "None" to end the
#         #       pagination loop.
#         if self.next_page_token_jsonpath:
#             all_matches = extract_jsonpath(
#                 self.next_page_token_jsonpath, response.json()
#             )
#             first_match = next(iter(all_matches), None)
#             next_page_token = first_match
#         else:
#             next_page_token = response.headers.get("X-Next-Page", None)

#         return next_page_token

#     def get_url_params(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> Dict[str, Any]:
#         """Return a dictionary of values to be used in URL parameterization."""
#         params: dict = {}
#         if next_page_token:
#             params["page"] = next_page_token
#         if self.replication_key:
#             params["sort"] = "asc"
#             params["order_by"] = self.replication_key
#         return params

#     def prepare_request_payload(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> Optional[dict]:
#         """Prepare the data payload for the REST API request.

#         By default, no payload will be sent (return None).
#         """
#         # TODO: Delete this method if no payload is required. (Most REST APIs.)
#         return None

#     def parse_response(self, response: requests.Response) -> Iterable[dict]:
#         """Parse the response and return an iterator of result records."""
#         # TODO: Parse response body and return a set of records.
#         yield from extract_jsonpath(self.records_jsonpath, input=response.json())

#     def post_process(self, row: dict, context: Optional[dict]) -> dict:
#         """As needed, append or transform raw data to match expected structure."""
#         # TODO: Delete this method if not needed.
#         return row
