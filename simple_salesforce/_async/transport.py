import aiohttp

from .login import AsyncSalesforceLogin
from ..exceptions import SalesforceGeneralError, SalesforceError, SalesforceRefusedRequest, SalesforceResourceNotFound, \
    SalesforceExpiredSession, SalesforceMalformedRequest, SalesforceMoreThanOneRecord, SalesforceAuthenticationFailed
from collections import namedtuple
from ..transport import Transport


Usage = namedtuple('Usage', 'used total')
PerAppUsage = namedtuple('PerAppUsage', 'used total name')


class AsyncTransport(Transport):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.session = aiohttp.ClientSession()

    async def refresh_session(self):
        self.session_id, self.sf_instance = await AsyncSalesforceLogin(
            **self.auth_kwargs
        )

    async def call(self, method, endpoint, api='base', **kwargs):
        """Utility method for performing HTTP call to Salesforce.

        Returns a `requests.result` object.
        """
        api_base = getattr(self, api + '_url')
        url = api_base + endpoint

        return await self._api_call(method, url, **kwargs)

    async def _api_call(self, method, url, **kwargs):
        headers = self.headers.copy()
        additional_headers = kwargs.pop('headers', dict())
        headers.update(additional_headers or dict())
        result = await self.session.request(method, url, headers=headers, **kwargs)

        try:
            if result.status >= 300:
                await self.exception_handler(result)
        except SalesforceExpiredSession as e:
            await self.refresh_session()
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
