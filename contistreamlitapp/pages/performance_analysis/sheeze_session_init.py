import os
import time
from datetime import datetime, timedelta
from time import mktime
from typing import Dict, Optional

import pytz
import requests
# from jose import jwt
from msal import ConfidentialClientApplication
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# from opentelemetry.instrumentation.requests import RequestsInstrumentor
from urllib3 import Retry

# from sheeze.tracing import tracer_provider
# from sheeze.util import get_project_name

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

# RequestsInstrumentor().instrument(tracer_provider=tracer_provider)

JSON_CONTENT_TYPE = "application/json"
XML_CONTENT_TYPE = "text/xml"
EXCEL_CONTENT_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def get_access_token() -> str:
    scope = ["api://aaf1f5ec-730c-4f0a-aa1f-a9e2d7e6b53a/.default"]
    app = ConfidentialClientApplication(
        client_id=os.environ["AZURE_CLIENT_ID"],
        authority=f"https://login.microsoftonline.com/{os.environ['AZURE_TENANT_ID']}",
        client_credential=os.environ["AZURE_CLIENT_SECRET"],
    )
    result = app.acquire_token_silent(scope, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=scope)
        return result["access_token"]
    # else:
    #     claims = jwt.get_unverified_claims(result["access_token"])
    #     iat_date = datetime.fromtimestamp(mktime(time.gmtime(claims["iat"])))
    #     now = datetime.now(tz=pytz.utc)
    #     if iat_date + timedelta(seconds=result["expires_in"]) >= now:
    #         result = app.acquire_token_for_client(scopes=scope)
    #     return result["access_token"]


def _build_base_session(
    max_retries=10, content_type: str = JSON_CONTENT_TYPE, retry_strategy: Retry = None
) -> requests.Session:
    session = requests.Session()

    if retry_strategy is None:
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            allowed_methods=["GET"],  # HTTP methods to trigger retry
        )

    http_adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    https_adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", http_adapter)
    session.mount("https://", https_adapter)
    # session.headers["User-Agent"] = f"EnergeTech-{get_project_name()}"
    session.headers["Accept"] = content_type
    return session


def build_azure_session(max_retries=10, retry_strategy: Retry = None) -> requests.Session:
    session = _build_base_session(max_retries, retry_strategy=retry_strategy)
    session.trust_env = False
    session.verify = False
    return session


def build_xml_azure_session(max_retries=10, retry_strategy: Retry = None) -> requests.Session:
    session = _build_base_session(max_retries, content_type=XML_CONTENT_TYPE, retry_strategy=retry_strategy)
    session.trust_env = False
    session.verify = False
    return session


def build_session(max_retries=10, http_headers: Optional[Dict[str, str]] = None, retry_strategy: Retry = None) -> requests.Session:
    if http_headers is None:
        http_headers = {}
    session = _build_base_session(max_retries, retry_strategy=retry_strategy)
    session.trust_env = False
    session.verify = False
    for k, v in http_headers.items():
        session.headers[k] = v
    return session


def build_seer_session(max_retries=10, retry_strategy: Retry = None) -> requests.Session:
    session = build_azure_session(max_retries, retry_strategy=retry_strategy)
    session.headers["Authorization"] = f"Bearer {get_access_token()}"
    return session
