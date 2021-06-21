import requests
import re

from .login import SalesforceLogin
from .exceptions import SalesforceGeneralError, SalesforceError, SalesforceRefusedRequest, SalesforceResourceNotFound, \
    SalesforceExpiredSession, SalesforceMalformedRequest, SalesforceMoreThanOneRecord, SalesforceAuthenticationFailed
from collections import namedtuple


Usage = namedtuple('Usage', 'used total')
PerAppUsage = namedtuple('PerAppUsage', 'used total name')


class Transport(object):

    @property
    def headers(self):
        return {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self.session_id,
            'X-PrettyPrint': '1'
        }

    @property
    def base_url(self):
        return 'https://{instance}/services/data/v{version}/'.format(instance=self.sf_instance, version=self.api_version)

    @property
    def apex_url(self):
        return 'https://{instance}/services/apexrest/'.format(instance=self.sf_instance)

    @property
    def bulk_url(self):
        return 'https://{instance}/services/async/{version}/'.format(instance=self.sf_instance, version=self.api_version)

    @property
    def metadata_url(self):
        return 'https://{instance}/services/Soap/m/{version}/'.format(instance=self.sf_instance, version=self.api_version)

    @property
    def tooling_url(self):
        return '{base_url}tooling/'.format(base_url=self.base_url)

    def __init__(self, username=None, password=None, security_token=None, organizationId=None, version=None,
                 client_id=None, domain=None, consumer_key=None, privatekey_file=None, privatekey=None):

        self.session = requests.Session()
        self.session_id = None
        self.sf_instance = None
        self._exp = 0
        self.api_usage = {}
        self.api_version = version

        # Determine if the user wants to use our username/password auth or pass
        # in their own information
        if all(arg is not None for arg in (username, password, security_token)):
            self.auth_type = "password"
            self.auth_kwargs = {
                'session': self.session,
                'username': username,
                'password': password,
                'security_token': security_token,
                'sf_version': version,
                'client_id': client_id,
                'domain': domain,
            }

        elif all(arg is not None for arg in (username, password, organizationId)):
            self.auth_type = 'ipfilter'
            self.auth_kwargs = {
                'session': self.session,
                'username': username,
                'password': password,
                'organizationId': organizationId,
                'sf_version': version,
                'client_id': client_id,
                'domain': domain,
            }

        elif all(arg is not None for arg in (username, consumer_key, privatekey_file or privatekey)):
            self.auth_type = "jwt-bearer"
            self.auth_kwargs = {
                'session': self.session,
                'username': username,
                'consumer_key': consumer_key,
                'privatekey_file': privatekey_file,
                'privatekey': privatekey,
                'domain': domain,
            }

        else:
            raise TypeError(
                'You must provide login information or an instance and token'
            )

        # We get the initial SFDC Session
        self.refresh_session()

    def refresh_session(self):
        self.session_id, self.sf_instance = SalesforceLogin(
            **self.auth_kwargs
        )

    @staticmethod
    def parse_api_usage(sforce_limit_info):
        """parse API usage and limits out of the Sforce-Limit-Info header

        Arguments:

        * sforce_limit_info: The value of response header 'Sforce-Limit-Info'
            Example 1: 'api-usage=18/5000'
            Example 2: 'api-usage=25/5000;
                per-app-api-usage=17/250(appName=sample-connected-app)'
        """
        result = {}

        api_usage = re.match(r'[^-]?api-usage=(?P<used>\d+)/(?P<tot>\d+)',
                             sforce_limit_info)
        pau = r'.+per-app-api-usage=(?P<u>\d+)/(?P<t>\d+)\(appName=(?P<n>.+)\)'
        per_app_api_usage = re.match(pau, sforce_limit_info)

        if api_usage and api_usage.groups():
            groups = api_usage.groups()
            result['api-usage'] = Usage(used=int(groups[0]),
                                        total=int(groups[1]))
        if per_app_api_usage and per_app_api_usage.groups():
            groups = per_app_api_usage.groups()
            result['per-app-api-usage'] = PerAppUsage(used=int(groups[0]),
                                                      total=int(groups[1]),
                                                      name=groups[2])

        return result

    def call(self, method, endpoint, api='base', **kwargs):
        """Utility method for performing HTTP call to Salesforce.

        Returns a `requests.result` object.
        """
        api_base = getattr(self, api + '_url')
        url = api_base + endpoint

        return self._api_call(method, url, **kwargs)

    def _api_call(self, method, url, **kwargs):
        headers = self.headers.copy()
        additional_headers = kwargs.pop('headers', dict())
        headers.update(additional_headers or dict())
        result = self.session.request(method, url, headers=headers, **kwargs)

        try:
            if result.status_code >= 300:
                self.exception_handler(result)
        except SalesforceExpiredSession as e:
            self.refresh_session()
            result = self.session.request(method, url, headers=headers, **kwargs)

            if result.status_code >= 300:
                self.exception_handler(result)

        sforce_limit_info = result.headers.get('Sforce-Limit-Info')
        if sforce_limit_info:
            self.api_usage = Transport.parse_api_usage(sforce_limit_info)

        return result

    def exception_handler(self, result, name=""):
        """Exception router. Determines which error to raise for bad results"""
        try:
            response_content = result.json()
        # pylint: disable=broad-except
        except Exception:
            response_content = result.text

        exc_map = {
            300: SalesforceMoreThanOneRecord,
            400: SalesforceMalformedRequest,
            401: SalesforceExpiredSession,
            403: SalesforceRefusedRequest,
            404: SalesforceResourceNotFound,
        }
        exc_cls = exc_map.get(result.status_code, SalesforceGeneralError)

        raise exc_cls(result.url, result.status_code, name, response_content)
