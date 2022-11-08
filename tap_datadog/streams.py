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

    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

        self.current_host = 0 
        self.host = [
            "api.degreed.com",
            "api.eu.degreed.com",
            "api.ca.degreed.com"
        ]

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

        payload = {"compute": [{"aggregation": "count", "type": "total" }, { "aggregation": "sum", "type": "total", "metric": "@Properties.Elapsed" } ], "filter": { "query": "source:degreed.api @MessageTemplate:\"HTTP {RequestMethod} {RequestPath} responded {StatusCode} in {Elapsed:0.0000} ms\" host: " + self.host[self.current_host], "from": from_date, "to": to_date, "indexes": [ "main" ] }, "group_by": [ { "facet": "@http.status_code" }, { "facet": "@Properties.OrganizationId" }, { "facet": "@Properties.PathTemplate" }, { "facet": "@Properties.RequestMethod" } ] }
        print(payload)
        return payload

    def post_process(self, row: dict, context: Optional[dict]) -> dict:

        row["by"]["host_name"] = self.host[self.current_host]

        self.logger.info(row)
                
        return row


    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        if self.current_host != len(self.host) - 1:
            self.current_host += 1
            return self.current_host
        else:
            return None
        # if self.get_next_page_token_epoch + 86400 >= int(time.time()):
        #     self.logger.info("No more next page token")
        #     self.logger.info(f"the previous token WAS: {self.get_next_page_token_epoch}")

        #     return None
        # else: 
        #     self.logger.info(f"get_next_page_token: {self.get_next_page_token_epoch}")
        #     return self.get_next_page_token_epoch
 



class Metric_Response_Time(TapDatadogStream):        
 
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

        self.configuration_start_date = self.config["start_date"]
        self.get_next_page_token_epoch = 0
        self.end_of_month_limit_epoch = 0
        self.replication_key_value = 0
        self.first_run = True
        self.slo_date_overreach = False
        
    name = "metric_response_time" # Stream name 
    path = f"/api/v1/query"
    rest_method = "GET"
    
    #records_jsonpath = "$." 
    next_page_token = 0
    schema = th.PropertiesList(
        th.Property("schema", th.StringType),
        th.Property("resp_version", th.NumberType),
        th.Property(
            "series",
            th.ArrayType(
                th.ObjectType(
                        th.Property("metric", th.StringType),
                        th.Property("scope", th.StringType),
                        th.Property("pointlist", th.ArrayType(th.ArrayType(th.NumberType)),
                        th.Property("expression", th.StringType),
                    )
                )
            ),
            th.Property("scope", th.StringType),
        ),
        th.Property("to_date", th.NumberType),
        th.Property("query", th.StringType),
        th.Property("from_date", th.NumberType),

    ).to_dict()

    #primary_keys = ["type_id"]
    replication_key = "to_date"
    schema_filepath = SCHEMAS_DIR / "metric_response_time.json"  # Optional: use schema_filepath with .json inside schemas/ 
    

    def _get_first_of_month_epoch(self, epoch):

        date_time = datetime.fromtimestamp(epoch)  
        date_split = str(date_time).split(' ') 
        year, month, day = date_split[0].split('-')

        first_day_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
        first_day_of_month_date_epoch  = calendar.timegm(first_day_of_month_date.timetuple())

        self.logger.info(f"_get_first_of_month_epoch:  {first_day_of_month_date_epoch}")
        return first_day_of_month_date_epoch


    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["query"] = 'trace.aspnet.request.duration{env:production}'
        try:
            if self.replication_key_value == 0:
                self.replication_key_value = self.stream_state["replication_key_value"]
                self.logger.info("oh no...")
                self.get_next_page_token_epoch = self.replication_key_value + 86400
            else:
                self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
            
            first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
            

            params["from"] = first_of_month_epoch
            params["to"] = self.get_next_page_token_epoch

            if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                self.slo_date_overreach = True
            

        except:
            if "to_date" not in self.stream_state:
                if self.first_run:

                    # Get Start date for initial full sync
                    year, month, day = self.configuration_start_date.split('-')

                    first_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
                    first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

                    self.get_next_page_token_epoch = first_of_month_epoch + 86400

                    self.logger.info("FULL STREAM")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"To ts date: {self.get_next_page_token_epoch}")

                    params["from"] = first_of_month_epoch
                    params["to"] = self.get_next_page_token_epoch

                    
                    self.first_run = False
                    
                else: 
                    first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
                    self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
                    
                    self.logger.info("******NON FULL STREAM******")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"Next page token:  {self.get_next_page_token_epoch}")

                    params["from"] = first_of_month_epoch
                    params["to"] = self.get_next_page_token_epoch


                    if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                        self.slo_date_overreach = True
                

        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:

        self.logger.info('BLARGFEST')
        self.logger.info(row)
        
        if "to_date" in self.stream_state:
            self.logger.info("Found to_ts state value")
            
        self.logger.info(row)
                
        return row


    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        if self.get_next_page_token_epoch + 86400 >= int(time.time()):
            self.logger.info("No more next page token")
            self.logger.info(f"the previous token WAS: {self.get_next_page_token_epoch}")

            return None
        else: 
            self.logger.info(f"get_next_page_token: {self.get_next_page_token_epoch}")
            return self.get_next_page_token_epoch
 

############################################################################
class SLO_History_US_Prod(TapDatadogStream):        
 
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

        self.slo_id = 'e96fa5aa00dc57af8718c8e7044b0f51' # prod-us

        self.configuration_start_date = self.config["start_date"]
        self.get_next_page_token_epoch = 0
        self.end_of_month_limit_epoch = 0
        self.replication_key_value = 0
        self.first_run = True
        self.slo_date_overreach = False
        
    name = "slo_history_us_prod" # Stream name 
    rest_method = "GET"
    slo_id = 'e96fa5aa00dc57af8718c8e7044b0f51' # prod-us
    path = f"/api/v1/slo/{slo_id}/history"
    records_jsonpath = "$.data" # https://jsonpath.com Use requests response json to identify the json path 
    next_page_token = 0

    #primary_keys = ["type_id"]
    replication_key = "to_ts"
    schema_filepath = SCHEMAS_DIR / "slo_history_us_prod.json"  # Optional: use schema_filepath with .json inside schemas/ 
    

    def _get_first_of_month_epoch(self, epoch):

        date_time = datetime.fromtimestamp(epoch)  
        date_split = str(date_time).split(' ') 
        year, month, day = date_split[0].split('-')

        first_day_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
        first_day_of_month_date_epoch  = calendar.timegm(first_day_of_month_date.timetuple())

        self.logger.info(f"_get_first_of_month_epoch:  {first_day_of_month_date_epoch}")
        return first_day_of_month_date_epoch


    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        try:
            if self.replication_key_value == 0:
                self.replication_key_value = self.stream_state["replication_key_value"]
                self.get_next_page_token_epoch = self.replication_key_value + 86400
            else:
                self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
            
            first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
            

            params["from_ts"] = first_of_month_epoch
            params["to_ts"] = self.get_next_page_token_epoch

            if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                self.slo_date_overreach = True
            

        except:
            if "to_ts" not in self.stream_state:
                if self.first_run:

                    # Get Start date for initial full sync
                    year, month, day = self.configuration_start_date.split('-')

                    first_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
                    first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

                    self.get_next_page_token_epoch = first_of_month_epoch + 86400

                    self.logger.info("FULL STREAM")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"To ts date::  {self.get_next_page_token_epoch}")

                    params["from_ts"] = first_of_month_epoch
                    params["to_ts"] = self.get_next_page_token_epoch

                    
                    self.first_run = False
                    
                else: 
                    first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
                    self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
                    
                    self.logger.info("******NON FULL STREAM******")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"Next page token:  {self.get_next_page_token_epoch}")

                    params["from_ts"] = first_of_month_epoch
                    params["to_ts"] = self.get_next_page_token_epoch


                    if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                        self.slo_date_overreach = True
                

        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:

        row["slo_id"] = self.slo_id
        if "to_ts" in self.stream_state:
            self.logger.info("Found to_ts state value")
            
        self.logger.info(row)
                
        return row


    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        if self.get_next_page_token_epoch + 86400 >= int(time.time()):
            self.logger.info("No more next page token")
            self.logger.info(f"the previous token WAS: {self.get_next_page_token_epoch}")

            return None
        else: 
            self.logger.info(f"get_next_page_token: {self.get_next_page_token_epoch}")
            return self.get_next_page_token_epoch
 
class SLO_History_EU_Prod(TapDatadogStream):        
 
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

        self.slo_id = '12b1e51cb1bd57928ce126502a9a7e01' # prod-us

        self.configuration_start_date = self.config["start_date"]
        self.get_next_page_token_epoch = 0
        self.end_of_month_limit_epoch = 0
        self.replication_key_value = 0
        self.first_run = True
        self.slo_date_overreach = False
        
    name = "slo_history_eu_prod" # Stream name 
    rest_method = "GET"
    slo_id = '12b1e51cb1bd57928ce126502a9a7e01' # prod-us
    path = f"/api/v1/slo/{slo_id}/history"
    records_jsonpath = "$.data" # https://jsonpath.com Use requests response json to identify the json path 
    next_page_token = 0

    #primary_keys = ["type_id"]
    replication_key = "to_ts"
    schema_filepath = SCHEMAS_DIR / "slo_history_eu_prod.json"  # Optional: use schema_filepath with .json inside schemas/ 
    

    def _get_first_of_month_epoch(self, epoch):

        date_time = datetime.fromtimestamp(epoch)  
        date_split = str(date_time).split(' ') 
        year, month, day = date_split[0].split('-')

        first_day_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
        first_day_of_month_date_epoch  = calendar.timegm(first_day_of_month_date.timetuple())

        self.logger.info(f"_get_first_of_month_epoch:  {first_day_of_month_date_epoch}")
        return first_day_of_month_date_epoch


    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        try:
            if self.replication_key_value == 0:
                self.replication_key_value = self.stream_state["replication_key_value"]
                self.get_next_page_token_epoch = self.replication_key_value + 86400
            else:
                self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
            
            first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
            

            params["from_ts"] = first_of_month_epoch
            params["to_ts"] = self.get_next_page_token_epoch

            if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                self.slo_date_overreach = True
            

        except:
            if "to_ts" not in self.stream_state:
                if self.first_run:

                    # Get Start date for initial full sync
                    year, month, day = self.configuration_start_date.split('-')

                    first_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
                    first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

                    self.get_next_page_token_epoch = first_of_month_epoch + 86400

                    self.logger.info("FULL STREAM")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"To ts date::  {self.get_next_page_token_epoch}")

                    params["from_ts"] = first_of_month_epoch
                    params["to_ts"] = self.get_next_page_token_epoch

                    
                    self.first_run = False
                    
                else: 
                    first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
                    self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
                    
                    self.logger.info("******NON FULL STREAM******")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"Next page token:  {self.get_next_page_token_epoch}")

                    params["from_ts"] = first_of_month_epoch
                    params["to_ts"] = self.get_next_page_token_epoch


                    if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                        self.slo_date_overreach = True
                

        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:

        row["slo_id"] = self.slo_id
        if "to_ts" in self.stream_state:
            self.logger.info("Found to_ts state value")
            
        self.logger.info(row)
                
        return row


    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        if self.get_next_page_token_epoch + 86400 >= int(time.time()):
            self.logger.info("No more next page token")
            self.logger.info(f"the previous token WAS: {self.get_next_page_token_epoch}")

            return None
        else: 
            self.logger.info(f"get_next_page_token: {self.get_next_page_token_epoch}")
            return self.get_next_page_token_epoch
 

class SLO_History_CA_Prod(TapDatadogStream):        
 
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.logger = logging.getLogger(__name__)

        self.slo_id = '36ecf33bedb25fedb64d9ef843780c8b' # prod-us

        self.configuration_start_date = self.config["start_date"]
        self.get_next_page_token_epoch = 0
        self.end_of_month_limit_epoch = 0
        self.replication_key_value = 0
        self.first_run = True
        self.slo_date_overreach = False
        
    name = "slo_history_ca_prod" # Stream name 
    rest_method = "GET"
    slo_id = '36ecf33bedb25fedb64d9ef843780c8b' # prod-us
    path = f"/api/v1/slo/{slo_id}/history"
    records_jsonpath = "$.data" # https://jsonpath.com Use requests response json to identify the json path 
    next_page_token = 0

    #primary_keys = ["type_id"]
    replication_key = "to_ts"
    schema_filepath = SCHEMAS_DIR / "slo_history_ca_prod.json"  # Optional: use schema_filepath with .json inside schemas/ 
    

    def _get_first_of_month_epoch(self, epoch):

        date_time = datetime.fromtimestamp(epoch)  
        date_split = str(date_time).split(' ') 
        year, month, day = date_split[0].split('-')

        first_day_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
        first_day_of_month_date_epoch  = calendar.timegm(first_day_of_month_date.timetuple())

        self.logger.info(f"_get_first_of_month_epoch:  {first_day_of_month_date_epoch}")
        return first_day_of_month_date_epoch


    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        try:
            if self.replication_key_value == 0:
                self.replication_key_value = self.stream_state["replication_key_value"]
                self.get_next_page_token_epoch = self.replication_key_value + 86400
            else:
                self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
            
            first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
            

            params["from_ts"] = first_of_month_epoch
            params["to_ts"] = self.get_next_page_token_epoch

            if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                self.slo_date_overreach = True
            

        except:
            if "to_ts" not in self.stream_state:
                if self.first_run:

                    # Get Start date for initial full sync
                    year, month, day = self.configuration_start_date.split('-')

                    first_of_month_date = datetime(int(year), int(month), 1, 0, 0, 0)
                    first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

                    self.get_next_page_token_epoch = first_of_month_epoch + 86400

                    self.logger.info("FULL STREAM")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"To ts date::  {self.get_next_page_token_epoch}")

                    params["from_ts"] = first_of_month_epoch
                    params["to_ts"] = self.get_next_page_token_epoch

                    
                    self.first_run = False
                    
                else: 
                    first_of_month_epoch = self._get_first_of_month_epoch(self.get_next_page_token_epoch)
                    self.get_next_page_token_epoch = self.get_next_page_token_epoch + 86400
                    
                    self.logger.info("******NON FULL STREAM******")
                    self.logger.info(f"first of month: {first_of_month_epoch}")
                    self.logger.info(f"Next page token:  {self.get_next_page_token_epoch}")

                    params["from_ts"] = first_of_month_epoch
                    params["to_ts"] = self.get_next_page_token_epoch


                    if self.get_next_page_token_epoch + 86400 >= int(time.time()):
                        self.slo_date_overreach = True
                

        return params

    def post_process(self, row: dict, context: Optional[dict]) -> dict:

        row["slo_id"] = self.slo_id
        if "to_ts" in self.stream_state:
            self.logger.info("Found to_ts state value")
            
        self.logger.info(row)
                
        return row


    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""

        if self.get_next_page_token_epoch + 86400 >= int(time.time()):
            self.logger.info("No more next page token")
            self.logger.info(f"the previous token WAS: {self.get_next_page_token_epoch}")

            return None
        else: 
            self.logger.info(f"get_next_page_token: {self.get_next_page_token_epoch}")
            return self.get_next_page_token_epoch
 
    
    











    # def _get_epoch_date_values(self):
    #     today = datetime.today()

    #     if today.day - 1 < 1:
    #         if today.month == 1:
    #             year = today.year - 1 
    #             month = 12
    #         else: 
    #             year = today.year
    #             month = today.month - 1

    #         first_of_month_date = datetime(year, month, 1, 0, 0, 0)
    #         first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

    #         last_day_month = calendar.monthrange(year, month)[1]
    #         to_time_date = datetime(year, month, last_day_month, 23, 59, 59)
    #         to_time_epoch = calendar.timegm(to_time_date.timetuple())
        
    #     else: 
    #         first_of_month_date = datetime(today.year, today.month, 1, 0, 0, 0)
    #         first_of_month_epoch = calendar.timegm(first_of_month_date.timetuple())

    #         to_time_date = datetime(today.year, today.month, today.day - 1, 23, 59, 59)
    #         to_time_epoch = calendar.timegm(to_time_date.timetuple())

    #         return first_of_month_epoch, to_time_epoch
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
