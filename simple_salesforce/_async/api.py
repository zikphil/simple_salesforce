"""Core classes and exceptions for Simple-Salesforce"""

import base64

# has to be defined prior to login import
DEFAULT_API_VERSION = '42.0'

import json
import logging
from collections import OrderedDict, namedtuple
from urllib.parse import urljoin, urlparse

from .bulk import AsyncSFBulkHandler
from ..exceptions import SalesforceGeneralError
from ..util import date_to_iso8601
from .metadata import SfdcMetadataApi
from .transport import AsyncTransport

# pylint: disable=invalid-name
logger = logging.getLogger(__name__)

Usage = namedtuple('Usage', 'used total')
PerAppUsage = namedtuple('PerAppUsage', 'used total name')


# pylint: disable=too-many-instance-attributes
class AsyncSalesforce(object):
    """Salesforce Instance

    An instance of Salesforce is a handy way to wrap a Salesforce session
    for easy use of the Salesforce REST API.
    """

    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    def __init__(
        self,
        username=None,
        password=None,
        security_token=None,
        organizationId=None,
        version=DEFAULT_API_VERSION,
        client_id=None,
        domain=None,
        consumer_key=None,
        privatekey_file=None,
        privatekey=None
    ):

        """Initialize the instance with the given parameters.

        Available kwargs

        Password Authentication:

        * username -- the Salesforce username to use for authentication
        * password -- the password for the username
        * security_token -- the security token for the username
        * domain -- The domain to using for connecting to Salesforce. Use
                    common domains, such as 'login' or 'test', or
                    Salesforce My domain. If not used, will default to
                    'login'.

        OAuth 2.0 JWT Bearer Token Authentication:

        * consumer_key -- the consumer key generated for the user

        Then either
        * privatekey_file -- the path to the private key file used
                             for signing the JWT token
        OR
        * privatekey -- the private key to use
                         for signing the JWT token

        Direct Session and Instance Access:

        * session_id -- Access token for this session

        Then either
        * instance -- Domain of your Salesforce instance, i.e.
          `na1.salesforce.com`
        OR
        * instance_url -- Full URL of your instance i.e.
          `https://na1.salesforce.com

        Universal Kwargs:
        * version -- the version of the Salesforce API to use, for example
                     `29.0`
        * proxies -- the optional map of scheme to proxy server
        * session -- Custom requests session, created in calling code. This
                     enables the use of requests Session features not otherwise
                     exposed by simple_salesforce.

        """

        if domain is None:
            domain = 'login'

        self.transport = AsyncTransport(
            username=username,
            password=password,
            security_token=security_token,
            organizationId=organizationId,
            client_id=client_id,
            domain=domain,
            privatekey_file=privatekey_file,
            privatekey=privatekey,
            version=version,
            consumer_key=consumer_key,
        )

        self._mdapi = None

    @property
    def mdapi(self, sandbox=False):
        if not self._mdapi:
            self._mdapi = SfdcMetadataApi(sandbox=sandbox, transport_instance=self.transport)
        return self._mdapi

    async def describe(self, **kwargs):
        """Describes all available objects

        Arguments:

        * keyword arguments supported by requests.request (e.g. json, timeout)
        """
        result = await self.transport.call(
            'GET',
            endpoint="sobjects",
            **kwargs
        )

        json_result = await result.json()
        if len(json_result) == 0:
            return None

        return json_result

    async def is_sandbox(self):
        """After connection returns is the organization is a sandbox"""
        is_sandbox = None
        if self.transport.session_id:
            is_sandbox = await self.query_all("SELECT IsSandbox FROM Organization LIMIT 1")
            is_sandbox = is_sandbox.get('records', [{'IsSandbox': None}])[0].get('IsSandbox')
        return is_sandbox

    # SObject Handler
    def __getattr__(self, name):
        """Returns an `SFType` instance for the given Salesforce object type
        (given in `name`).

        The magic part of the SalesforceAPI, this function translates
        calls such as `salesforce_api_instance.Lead.metadata()` into fully
        constituted `SFType` instances to make a nice Python API wrapper
        for the REST API.

        Arguments:

        * name -- the name of a Salesforce object type, e.g. Lead or Contact
        """

        # fix to enable serialization
        # (https://github.com/heroku/simple-salesforce/issues/60)
        if name.startswith('__'):
            return super().__getattr__(name)

        if name == 'bulk':
            # Deal with bulk API functions
            return AsyncSFBulkHandler(self.transport)

        return AsyncSFType(name, self.transport)

    # User utility methods
    async def set_password(self, user, password):
        """Sets the password of a user

        salesforce dev documentation link:
        https://www.salesforce.com/us/developer/docs/api_rest/Content
        /dome_sobject_user_password.htm

        Arguments:

        * user: the userID of the user to set
        * password: the new password
        """

        endpoint = 'sobjects/User/%s/password' % user
        params = {'NewPassword': password}

        result = await self.transport.call(
            'POST',
            endpoint=endpoint,
            data=json.dumps(params)
        )

        # salesforce return 204 No Content when the request is successful
        if result.status != 200 and result.status != 204:
            raise SalesforceGeneralError(endpoint, result.status_code, 'User', result.content)

        json_result = await result.json()

        if len(json_result) == 0:
            return None

        return json_result

    # Search Functions
    async def search(self, search):
        """Returns the result of a Salesforce search as a dict decoded from
        the Salesforce response JSON payload.

        Arguments:

        * search -- the fully formatted SOSL search string, e.g.
                    `FIND {Waldo}`
        """
        endpoint = 'search/'

        # `requests` will correctly encode the query string passed as `params`
        params = {'q': search}

        result = await self.transport.call(
            'POST',
            endpoint=endpoint,
            params=params
        )

        json_result = await result.json()

        if len(json_result) == 0:
            return None

        return json_result

    async def quick_search(self, search):
        """Returns the result of a Salesforce search as a dict decoded from
        the Salesforce response JSON payload.

        Arguments:

        * search -- the non-SOSL search string, e.g. `Waldo`. This search
                    string will be wrapped to read `FIND {Waldo}` before being
                    sent to Salesforce
        """
        search_string = 'FIND {{{search_string}}}'.format(search_string=search)
        return await self.search(search_string)

    async def limits(self, **kwargs):
        """Return the result of a Salesforce request to list Organization
        limits.
        """
        endpoint = 'limits/'
        result = await self.transport.call(
            'GET',
            endpoint=endpoint,
            **kwargs
        )

        if result.status != 200:
            self.transport.exception_handler(result)

        return await result.json()

    # Query Handler
    async def query(self, query, include_deleted=False, **kwargs):
        """Return the result of a Salesforce SOQL query as a dict decoded from
        the Salesforce response JSON payload.

        Arguments:

        * query -- the SOQL query to send to Salesforce, e.g.
                   SELECT Id FROM Lead WHERE Email = "waldo@somewhere.com"
        * include_deleted -- True if deleted records should be included
        """
        endpoint = 'queryAll/' if include_deleted else 'query/'
        params = {'q': query}

        # `requests` will correctly encode the query string passed as `params`
        result = await self.transport.call(
            'GET',
            endpoint=endpoint,
            params=params,
            **kwargs
        )

        return await result.json()

    async def query_more(self, next_records_identifier, identifier_is_url=False, include_deleted=False, **kwargs):
        """Retrieves more results from a query that returned more results
        than the batch maximum. Returns a dict decoded from the Salesforce
        response JSON payload.

        Arguments:

        * next_records_identifier -- either the Id of the next Salesforce
                                     object in the result, or a URL to the
                                     next record in the result.
        * identifier_is_url -- True if `next_records_identifier` should be
                               treated as a URL, False if
                               `next_records_identifier` should be treated as
                               an Id.
        * include_deleted -- True if the `next_records_identifier` refers to a
                             query that includes deleted records. Only used if
                             `identifier_is_url` is False
        """
        if identifier_is_url:
            # Don't use `self.base_url` here because the full URI is provided
            url = ('https://{instance}{next_record_url}'
                   .format(instance=self.transport.sf_instance,
                           next_record_url=next_records_identifier))

            result = await self.transport._api_call('GET', url,  **kwargs)

        else:
            endpoint = '{}/{}'.format('queryAll' if include_deleted else 'query', next_records_identifier)

            result = await self.transport.call('GET', endpoint=endpoint, **kwargs)

        return await result.json()

    async def query_all_iter(self, query, include_deleted=False, **kwargs):
        """This is a lazy alternative to `query_all` - it does not construct
        the whole result set into one container, but returns objects from each
        page it retrieves from the API.

        Since `query_all` has always been eagerly executed, we reimplemented it
        using `query_all_iter`, only materializing the returned iterator to
        maintain backwards compatibility.

        The one big difference from `query_all` (apart from being lazy) is that
        we don't return a dictionary with `totalSize` and `done` here,
        we only return the records in an iterator.

        Arguments

        * query -- the SOQL query to send to Salesforce, e.g.
                   SELECT Id FROM Lead WHERE Email = "waldo@somewhere.com"
        * include_deleted -- True if the query should include deleted records.
        """

        result = await self.query(query, include_deleted=include_deleted, **kwargs)
        while True:
            for record in result['records']:
                yield record
            # fetch next batch if we're not done else break out of loop
            if not result['done']:
                result = await self.query_more(result['nextRecordsUrl'], identifier_is_url=True)
            else:
                return

    async def query_all(self, query, include_deleted=False, **kwargs):
        """Returns the full set of results for the `query`. This is a
        convenience
        wrapper around `query(...)` and `query_more(...)`.

        The returned dict is the decoded JSON payload from the final call to
        Salesforce, but with the `totalSize` field representing the full
        number of results retrieved and the `records` list representing the
        full list of records retrieved.

        Arguments

        * query -- the SOQL query to send to Salesforce, e.g.
                   SELECT Id FROM Lead WHERE Email = "waldo@somewhere.com"
        * include_deleted -- True if the query should include deleted records.
        """

        records = self.query_all_iter(query, include_deleted=include_deleted, **kwargs)
        all_records = [x async for x in records]
        return {
            'records': all_records,
            'totalSize': len(all_records),
            'done': True,
        }

    async def toolingexecute(self, action, method='GET', data=None, **kwargs):
        """Makes an HTTP request to an TOOLING REST endpoint

        Arguments:

        * action -- The REST endpoint for the request.
        * method -- HTTP method for the request (default GET)
        * data -- A dict of parameters to send in a POST / PUT request
        * kwargs -- Additional kwargs to pass to `requests.request`
        """
        # If data is None, we should send an empty body, not "null", which is
        # None in json.
        json_data = json.dumps(data) if data is not None else None
        result = await self.transport.call(
            method,
            api='tooling',
            endpoint=action,
            data=json_data, **kwargs
        )
        try:
            response_content = await result.json()
        # pylint: disable=broad-except
        except Exception:
            response_content = result.text

        return response_content

    async def apexecute(self, action, method='GET', data=None, **kwargs):
        """Makes an HTTP request to an APEX REST endpoint

        Arguments:

        * action -- The REST endpoint for the request.
        * method -- HTTP method for the request (default GET)
        * data -- A dict of parameters to send in a POST / PUT request
        * kwargs -- Additional kwargs to pass to `requests.request`
        """
        # If data is None, we should send an empty body, not "null", which is
        # None in json.
        json_data = json.dumps(data) if data is not None else None
        result = await self.transport.call(
            method,
            api='apex',
            endpoint=action,
            data=json_data, **kwargs
        )
        try:
            response_content = await result.json()
        # pylint: disable=broad-except
        except Exception:
            response_content = result.text

        return response_content

    # file-based deployment function
    def deploy(self, zipfile, sandbox, **kwargs):

        """Deploy using the Salesforce Metadata API. Wrapper for
        SfdcMetaDataApi.deploy(...).

        Arguments:

        * zipfile: a .zip archive to deploy to an org, given as (
        "path/to/zipfile.zip")
        * options: salesforce DeployOptions in .json format.
            (https://developer.salesforce.com/docs/atlas.en-us.api_meta.meta
            /api_meta/meta_deploy.htm)

        Returns a process id and state for this deployment.
        """
        asyncId, state = self._mdapi(sandbox=sandbox).deploy(zipfile, **kwargs)
        result = {'asyncId': asyncId, 'state': state}
        return result

    # check on a file-based deployment
    def checkDeployStatus(self, asyncId, **kwargs):
        """Check on the progress of a file-based deployment via Salesforce
        Metadata API.
        Wrapper for SfdcMetaDataApi.check_deploy_status(...).

        Arguments:

        * asyncId: deployment async process ID, returned by Salesforce.deploy()

        Returns status of the deployment the asyncId given.
        """
        state, state_detail, deployment_detail, unit_test_detail = \
            self._mdapi.check_deploy_status(asyncId, **kwargs)
        results = {
            'state': state,
            'state_detail': state_detail,
            'deployment_detail': deployment_detail,
            'unit_test_detail': unit_test_detail
        }

        return results


class AsyncSFType(object):
    """An interface to a specific type of SObject"""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        object_name,
        transport_instance,
    ):
        """Initialize the instance with the given parameters.

        Arguments:

        * object_name -- the name of the type of SObject this represents,
                         e.g. `Lead` or `Contact`
        * transport_instance -- Transport instance to use for API communication
        """
        self.name = object_name
        self.transport = transport_instance

        self.base_endpoint = 'sobjects/{object_name}/'.format(object_name=object_name)

    async def metadata(self, headers=None):
        """Returns the result of a GET to `.../{object_name}/` as a dict
        decoded from the JSON payload returned by Salesforce.

        Arguments:

        * headers -- a dict with additional request headers.
        """
        result = await self.transport.call(
            'GET',
            endpoint=self.base_endpoint,
            headers=headers
        )
        return await result.json()

    async def describe(self, headers=None):
        """Returns the result of a GET to `.../{object_name}/describe` as a
        dict decoded from the JSON payload returned by Salesforce.

        Arguments:

        * headers -- a dict with additional request headers.
        """
        result = await self.transport.call(
            method='GET',
            endpoint=urljoin(self.base_endpoint, 'describe'),
            headers=headers
        )
        return await result.json()

    async def describe_layout(self, record_id, headers=None):
        """Returns the layout of the object

        Returns the result of a GET to
        `.../{object_name}/describe/layouts/<recordid>` as a dict decoded from
        the JSON payload returned by Salesforce.

        Arguments:

        * record_id -- the Id of the SObject to get
        * headers -- a dict with additional request headers.
        """
        custom_url_part = 'describe/layouts/{record_id}'.format(
            record_id=record_id
        )
        result = await self.transport.call(
            method='GET',
            endpoint=urljoin(self.base_endpoint, custom_url_part),
            headers=headers
        )
        return await result.json()

    async def get(self, record_id, headers=None):
        """Returns the result of a GET to `.../{object_name}/{record_id}` as a
        dict decoded from the JSON payload returned by Salesforce.

        Arguments:

        * record_id -- the Id of the SObject to get
        * headers -- a dict with additional request headers.
        """
        result = await self.transport.call(
            method='GET',
            endpoint=urljoin(self.base_endpoint, record_id),
            headers=headers
        )
        return await result.json()

    async def get_by_custom_id(self, custom_id_field, custom_id, headers=None):
        """Return an ``SFType`` by custom ID

        Returns the result of a GET to
        `.../{object_name}/{custom_id_field}/{custom_id}` as a dict decoded
        from the JSON payload returned by Salesforce.

        Arguments:

        * custom_id_field -- the API name of a custom field that was defined
                             as an External ID
        * custom_id - the External ID value of the SObject to get
        * headers -- a dict with additional request headers.
        """
        custom_url = urljoin(
            self.base_endpoint, '{custom_id_field}/{custom_id}'.format(
                custom_id_field=custom_id_field, custom_id=custom_id
            )
        )
        result = await self.transport.call(
            method='GET',
            endpoint=custom_url,
            headers=headers
        )
        return await result.json()

    async def create(self, data, headers=None):
        """Creates a new SObject using a POST to `.../{object_name}/`.

        Returns a dict decoded from the JSON payload returned by Salesforce.

        Arguments:

        * data -- a dict of the data to create the SObject from. It will be
                  JSON-encoded before being transmitted.
        * headers -- a dict with additional request headers.
        """
        result = await self.transport.call(
            method='POST',
            endpoint=self.base_endpoint,
            data=json.dumps(data),
            headers=headers
        )
        return await result.json()

    async def upsert(self, record_id, data, raw_response=False, headers=None):
        """Creates or updates an SObject using a PATCH to
        `.../{object_name}/{record_id}`.

        If `raw_response` is false (the default), returns the status code
        returned by Salesforce. Otherwise, return the `requests.Response`
        object.

        Arguments:

        * record_id -- an identifier for the SObject as described in the
                       Salesforce documentation
        * data -- a dict of the data to create or update the SObject from. It
                  will be JSON-encoded before being transmitted.
        * raw_response -- a boolean indicating whether to return the response
                          directly, instead of the status code.
        * headers -- a dict with additional request headers.
        """
        result = await self.transport.call(
            method='PATCH',
            endpoint=urljoin(self.base_endpoint, record_id),
            data=json.dumps(data),
            headers=headers
        )
        return self._raw_response(result, raw_response)

    async def update(self, record_id, data, raw_response=False, headers=None):
        """Updates an SObject using a PATCH to
        `.../{object_name}/{record_id}`.

        If `raw_response` is false (the default), returns the status code
        returned by Salesforce. Otherwise, return the `requests.Response`
        object.

        Arguments:

        * record_id -- the Id of the SObject to update
        * data -- a dict of the data to update the SObject from. It will be
                  JSON-encoded before being transmitted.
        * raw_response -- a boolean indicating whether to return the response
                          directly, instead of the status code.
        * headers -- a dict with additional request headers.
        """
        result = await self.transport.call(
            method='PATCH',
            endpoint=urljoin(self.base_endpoint, record_id),
            data=json.dumps(data),
            headers=headers
        )
        return self._raw_response(result, raw_response)

    async def delete(self, record_id, raw_response=False, headers=None):
        """Deletes an SObject using a DELETE to
        `.../{object_name}/{record_id}`.

        If `raw_response` is false (the default), returns the status code
        returned by Salesforce. Otherwise, return the `requests.Response`
        object.

        Arguments:

        * record_id -- the Id of the SObject to delete
        * raw_response -- a boolean indicating whether to return the response
                          directly, instead of the status code.
        * headers -- a dict with additional request headers.
        """
        result = await self.transport.call(
            method='DELETE',
            endpoint=urljoin(self.base_endpoint, record_id),
            headers=headers
        )
        return self._raw_response(result, raw_response)

    async def deleted(self, start, end, headers=None):
        # pylint: disable=line-too-long
        """Gets a list of deleted records

        Use the SObject Get Deleted resource to get a list of deleted records
        for the specified object.
        .../deleted/?start=2013-05-05T00:00:00+00:00&end=2013-05-10T00:00:00
        +00:00

        * start -- start datetime object
        * end -- end datetime object
        * headers -- a dict with additional request headers.
        """
        url = urljoin(
            self.base_endpoint, 'deleted/?start={start}&end={end}'.format(
                start=date_to_iso8601(start), end=date_to_iso8601(end)
            )
        )
        result = await self.transport.call(method='GET', endpoint=url, headers=headers)
        return await result.json()

    async def updated(self, start, end, headers=None):
        # pylint: disable=line-too-long
        """Gets a list of updated records

        Use the SObject Get Updated resource to get a list of updated
        (modified or added) records for the specified object.

         .../updated/?start=2014-03-20T00:00:00+00:00&end=2014-03-22T00:00:00
         +00:00

        * start -- start datetime object
        * end -- end datetime object
        * headers -- a dict with additional request headers.
        """
        url = urljoin(
            self.base_endpoint, 'updated/?start={start}&end={end}'.format(
                start=date_to_iso8601(start), end=date_to_iso8601(end)
            )
        )
        result = await self.transport.call(method='GET', endpoint=url, headers=headers)
        return await result.json()

    # pylint: disable=no-self-use
    def _raw_response(self, response, body_flag):
        """Utility method for processing the response and returning either the
        status code or the response object.

        Returns either an `int` or a `requests.Response` object.
        """
        if not body_flag:
            return response.status

        return response

    async def upload_base64(self, file_path, base64_field='Body', data={}, headers=None, **kwargs):
        with open(file_path, "rb") as f:
            body = base64.b64encode(f.read()).decode('utf-8')
        data[base64_field] = body
        result = await self.transport.call(
            method='POST',
            endpoint=self.base_endpoint,
            headers=headers,
            json=data,
            **kwargs
        )

        return result

    async def update_base64(self, record_id, file_path, base64_field='Body', data={}, headers=None, raw_response=False,
                            **kwargs):
        with open(file_path, "rb") as f:
            body = base64.b64encode(f.read()).decode('utf-8')
        data[base64_field] = body
        result = await self.transport.call(
            method='PATCH',
            endpoint=urljoin(self.base_endpoint, record_id),
            json=data,
            headers=headers,
            **kwargs
        )

        return self._raw_response(result, raw_response)

    async def get_base64(self, record_id, base64_field='Body', data=None, headers=None, **kwargs):
        """Returns binary stream of base64 object at specific path.
        Arguments:
        * path: The path of the request
            Example: sobjects/Attachment/ABC123/Body
                     sobjects/ContentVersion/ABC123/VersionData
        """
        result = await self.transport.call(
            method='GET',
            endpoint=urljoin(self.base_endpoint, f"{record_id}/{base64_field}"),
            data=data,
            headers=headers,
            **kwargs
        )

        return result.content
