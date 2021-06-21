"""Simple-Salesforce Package"""
# flake8: noqa
import sys

from .api import Salesforce, SFType
from .bulk import SFBulkHandler
from .exceptions import (SalesforceAuthenticationFailed, SalesforceError,
                         SalesforceExpiredSession, SalesforceGeneralError,
                         SalesforceMalformedRequest,
                         SalesforceMoreThanOneRecord, SalesforceRefusedRequest,
                         SalesforceResourceNotFound)
from .login import SalesforceLogin
from .format import format_soql, format_external_id

__all__ = [
    "Salesforce",
    "SFBulkHandler",
    "SFType",
    "SalesforceAuthenticationFailed",
    "SalesforceError",
    "SalesforceExpiredSession",
    "SalesforceGeneralError",
    "SalesforceMalformedRequest",
    "SalesforceMoreThanOneRecord",
    "SalesforceRefusedRequest",
    "SalesforceResourceNotFound",
    "SalesforceLogin",
    "format_soql",
    "format_external_id",
]

try:
    # Asyncio only supported on Python 3.6+
    if sys.version_info < (3, 6):
        raise ImportError

    from ._async.api import AsyncSalesforce, AsyncSFType
    from ._async.bulk import AsyncSFBulkHandler
    from ._async.login import AsyncSalesforceLogin

    __all__ += [
        "AsyncSalesforce",
        "AsyncSFType",
        "AsyncTransport",
        "AsyncSFBulkHandler",
        "AsyncSalesforceLogin",
    ]
except (ImportError, SyntaxError):
    pass
