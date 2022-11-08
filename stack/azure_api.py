import adal
import requests
import os
import json
import logging

from enum import Enum
from typing import Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

"""
az
account
create
--enrollment-account-object-id
'/providers/Microsoft.Billing/billingAccounts/75463289/enrollmentAccounts/302404'
--offer-type
'{MS-AZR-0017P,MS-AZR-0041P}'
--subscription
$PROJECT
--display-name
$PROJECT
--owner-object-id
'/providers/Microsoft.Management/managementGroups/centre_for_population_genomics'


# CPG Tenant?
{
  "appId": "b7ceac2b-1b0a-4368-9664-2a85970b3f76",
  "displayName": "azure-cli-2022-11-02-03-01-08",
  "password": "6lRdqcb8oZqOJMJ6C-Lf3eRozKb4I0QRKA",
  "tenant": "a744336e-0ec4-40f1-891f-6c8ccaf8e267"
}

"""


CLIENT_ID = os.getenv('CLIENTID') or "b7ceac2b-1b0a-4368-9664-2a85970b3f76"
CLIENT_SECRET = os.getenv('CLIENTSECRET') or "6lRdqcb8oZqOJMJ6C-Lf3eRozKb4I0QRKA"
TENANT = os.getenv('TENANT') or "a744336e-0ec4-40f1-891f-6c8ccaf8e267"
RESOURCE = 'https://management.azure.com/'
AUTHORITY_URL = 'https://login.microsoftonline.com/' + TENANT
API_VERSION = {'api-version': '2020-06-01'}

class RequestType(Enum):
    GET = 'get'
    POST = 'post'
    PUT = 'put'
    DELETE = 'delete'

def azure_oauth():
    context = adal.AuthenticationContext(AUTHORITY_URL)
    token = context.acquire_token_with_client_credentials(
        RESOURCE, CLIENT_ID, CLIENT_SECRET
    )
    return token

def azure_api_request(
    token: dict, 
    endpoint: str, 
    request_type: RequestType, 
    request_body: Optional[dict] = None
):
    headers = {
        'Authorization': 'Bearer ' + token['accessToken'],
        'Content-Type': 'application/json'
    }
    url = RESOURCE + endpoint

    request = getattr(requests, request_type.value, None)

    if not request:
        logger.error(f'Not a valid request type: {request_type}')
        return {}

    if request_type == RequestType.GET:
        result = request(url, headers=headers, params=API_VERSION)
        return result.json()
    
    result = request(url, headers=headers, params=API_VERSION, body=request_body)
    return result.json()


token = azure_oauth()
r = azure_api_request(token, 'subscriptions', RequestType.GET)
print(json.dumps(r, indent=2))
