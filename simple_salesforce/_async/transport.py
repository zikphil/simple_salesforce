import aiohttp

from datetime import datetime, timedelta
from .login import AsyncSalesforceLogin
from ..exceptions import SalesforceGeneralError, SalesforceError, SalesforceRefusedRequest, SalesforceResourceNotFound, \
    SalesforceExpiredSession, SalesforceMalformedRequest, SalesforceMoreThanOneRecord, SalesforceAuthenticationFailed
from collections import namedtuple
from ..transport import Transport


Usage = namedtuple('Usage', 'used total')
PerAppUsage = namedtuple('PerAppUsage', 'used total name')


class AsyncTransport(Transport):

    def __init__(self, username=None, password=None, security_token=None, organizationId=None, version=None,
                 client_id=None, domain=None, consumer_key=None, privatekey_file=None, privatekey=None):

        self.session = aiohttp.ClientSession()
        self.session_id = None
        self.sf_instance = None
        self.exp = datetime.utcnow()
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

    async def refresh_session(self):
        del self.auth_kwargs['session']
        self.session_id, self.sf_instance, session_duration = await AsyncSalesforceLogin(
            **self.auth_kwargs,
            session=self.session
        )
        self.exp = datetime.utcnow() + timedelta(seconds=session_duration)

    async def call(self, method, endpoint, api='base', **kwargs):
        """Utility method for performing HTTP call to Salesforce.

        Returns a `requests.result` object.
        """
        # Making sure the session has not expired
        if datetime.utcnow() >= self.exp:
            await self.refresh_session()

        api_base = getattr(self, api + '_url')
        url = api_base + endpoint

        return await self._api_call(method, url, **kwargs)

    async def _api_call(self, method, url, **kwargs):

        # Making sure the session has not expired
        if datetime.utcnow() >= self.exp:
            await self.refresh_session()

        headers = self.headers.copy()
        additional_headers = kwargs.pop('headers', dict())
        headers.update(additional_headers or dict())
        result = await self.session.request(method, url, headers=headers, **kwargs)

        if result.status >= 300:
            await self.exception_handler(result)

        sforce_limit_info = result.headers.get('Sforce-Limit-Info')
        if sforce_limit_info:
            self.api_usage = AsyncTransport.parse_api_usage(sforce_limit_info)

        return result

    async def exception_handler(self, result, name=""):
        """Exception router. Determines which error to raise for bad results"""
        try:
            response_content = await result.json()
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
        exc_cls = exc_map.get(result.status, SalesforceGeneralError)

        raise exc_cls(result.url, result.status, name, response_content)
