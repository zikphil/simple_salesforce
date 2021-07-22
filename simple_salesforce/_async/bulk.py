""" Classes for interacting with Salesforce Bulk API """

import json
from collections import OrderedDict
from time import sleep
import concurrent.futures
from functools import partial

from ..util import list_from_generator


class AsyncSFBulkHandler(object):
    """ Bulk API request handler
    Intermediate class which allows us to use commands,
     such as 'sf.bulk.Contacts.create(...)'
    This is really just a middle layer, whose sole purpose is
    to allow the above syntax
    """

    def __init__(self, transport_instance):
        """Initialize the instance with the given parameters.

        Arguments:

        * transport_instance -- Transport instance to use for API communication
        """
        self.transport = transport_instance

    def __getattr__(self, name):
        return AsyncSFBulkType(object_name=name, transport_instance=self.transport_instance)


class AsyncSFBulkType(object):
    """ Interface to Bulk/Async API functions"""

    def __init__(self, object_name, transport_instance):
        """Initialize the instance with the given parameters.

        Arguments:

        * object_name -- the name of the type of SObject this represents,
                         e.g. `Lead` or `Contact`
        * transport_instance -- Transport instance to use for API communication
        """
        self.object_name = object_name
        self.transport = transport_instance

    async def _create_job(self, operation, use_serial, external_id_field=None):
        """ Create a bulk job

        Arguments:

        * operation -- Bulk operation to be performed by job
        * use_serial -- Process batches in order
        * external_id_field -- unique identifier field for upsert operations
        """

        if use_serial:
            use_serial = 1
        else:
            use_serial = 0
        payload = {
            'operation': operation,
            'object': self.object_name,
            'concurrencyMode': use_serial,
            'contentType': 'JSON'
            }

        if operation == 'upsert':
            payload['externalIdFieldName'] = external_id_field

        result = await self.transport.call(
            method='POST',
            endpoint='job',
            api='bulk',
            data=json.dumps(payload, allow_nan=False)
        )
        return await result.json()

    async def _close_job(self, job_id):
        """ Close a bulk job """
        payload = {
            'state': 'Closed'
            }

        endpoint = "{}{}".format('job/', job_id)

        result = await self.transport.call(
            method='POST',
            endpoint=endpoint,
            api='bulk',
            data=json.dumps(payload, allow_nan=False)
        )

        return await result.json()

    async def _get_job(self, job_id):
        """ Get an existing job to check the status """
        endpoint = "{}{}".format('job/', job_id)

        result = await self.transport.call(
            method='GET',
            endpoint=endpoint,
            api='bulk'
        )
        return await result.json()

    async def _add_batch(self, job_id, data, operation):
        """ Add a set of data as a batch to an existing job
        Separating this out in case of later
        implementations involving multiple batches
        """

        endpoint = "{}{}{}".format('job/', job_id, '/batch')

        if operation not in ('query', 'queryAll'):
            data = json.dumps(data, allow_nan=False)

        result = await self.transport.call(
            method='POST',
            endpoint=endpoint,
            api='bulk',
            data=data
        )
        return await result.json()

    async def _get_batch(self, job_id, batch_id):
        """ Get an existing batch to check the status """

        endpoint = "{}{}{}{}".format('job/', job_id, '/batch/', batch_id)

        result = await self.transport.call(
            method='GET',
            endpoint=endpoint,
            api='bulk'
        )
        return await result.json()

    async def _get_batch_results(self, job_id, batch_id, operation):
        """ retrieve a set of results from a completed job """

        endpoint = "{}{}{}{}{}".format('job/', job_id, '/batch/', batch_id, '/result')

        result = await self.transport.call(
            method='GET',
            endpoint=endpoint,
            api='bulk'
        )

        if operation in ('query', 'queryAll'):
            for batch_result in await result.json():
                url_query_results = "{}{}{}".format(endpoint, '/', batch_result)
                batch_query_result = await self.transport.call(
                    method='GET',
                    endpoint=url_query_results,
                    api='bulk'
                )

                yield await batch_query_result.json()
        else:
            yield await result.json()

    async def worker(self, batch, operation, wait=5):
        """ Gets batches from concurrent worker threads.
        self._bulk_operation passes batch jobs.
        The worker function checks each batch job waiting for it complete
        and appends the results.
        """

        batch_status = (await self._get_batch(job_id=batch['jobId'], batch_id=batch['id']))['state']

        while batch_status not in ['Completed', 'Failed', 'Not Processed']:
            sleep(wait)
            batch_status = (await self._get_batch(job_id=batch['jobId'], batch_id=batch['id']))['state']

        batch_results = [x async for x in self._get_batch_results(job_id=batch['jobId'], batch_id=batch['id'], operation=operation)]
        result = batch_results
        return result

    # pylint: disable=R0913
    async def _bulk_operation(self, operation, data, use_serial=False, external_id_field=None, batch_size=10000, wait=5):
        """ String together helper functions to create a complete
        end-to-end bulk API request
        Arguments:
        * operation -- Bulk operation to be performed by job
        * data -- list of dict to be passed as a batch
        * use_serial -- Process batches in serial mode
        * external_id_field -- unique identifier field for upsert operations
        * wait -- seconds to sleep between checking batch status
        * batch_size -- number of records to assign for each batch in the job
        """

        if operation not in ('query', 'queryAll'):
            # Checks to prevent batch limit
            if len(data) >= 10000 and batch_size > 10000:
                batch_size = 10000
            with concurrent.futures.ThreadPoolExecutor() as pool:

                job = await self._create_job(operation=operation, use_serial=use_serial, external_id_field=external_id_field)
                batches = [
                    await self._add_batch(job_id=job['id'], data=i, operation=operation)
                    for i in
                    [data[i * batch_size:(i + 1) * batch_size]
                     for i in range((len(data) // batch_size + 1))] if i]

                multi_thread_worker = partial(self.worker, operation=operation)
                list_of_results = pool.map(multi_thread_worker, batches)

                results = [x for sublist in list_of_results for i in
                           sublist for x in i]

                await self._close_job(job_id=job['id'])

        elif operation in ('query', 'queryAll'):
            job = await self._create_job(operation=operation, use_serial=use_serial, external_id_field=external_id_field)

            batch = await self._add_batch(job_id=job['id'], data=data, operation=operation)

            await self._close_job(job_id=job['id'])

            batch_status = (await self._get_batch(job_id=batch['jobId'], batch_id=batch['id']))['state']

            while batch_status not in ['Completed', 'Failed', 'Not Processed']:
                sleep(wait)
                batch_status = (await self._get_batch(job_id=batch['jobId'], batch_id=batch['id']))['state']

            results = [x async for x in self._get_batch_results(job_id=batch['jobId'], batch_id=batch['id'], operation=operation)]

        return results

    # _bulk_operation wrappers to expose supported Salesforce bulk operations
    async def delete(self, data, batch_size=10000, use_serial=False):
        """ soft delete records """
        results = await self._bulk_operation(use_serial=use_serial, operation='delete', data=data, batch_size=batch_size)
        return results

    async def insert(self, data, batch_size=10000,
               use_serial=False):
        """ insert records """
        results = await self._bulk_operation(use_serial=use_serial, operation='insert', data=data, batch_size=batch_size)
        return results

    async def upsert(self, data, external_id_field, batch_size=10000,
               use_serial=False):
        """ upsert records based on a unique identifier """
        results = await self._bulk_operation(
            use_serial=use_serial,
            operation='upsert',
            external_id_field=external_id_field,
            data=data,
            batch_size=batch_size
        )
        return results

    async def update(self, data, batch_size=10000, use_serial=False):
        """ update records """
        results = await self._bulk_operation(use_serial=use_serial, operation='update', data=data, batch_size=batch_size)
        return results

    async def hard_delete(self, data, batch_size=10000, use_serial=False):
        """ hard delete records """
        results = await self._bulk_operation(use_serial=use_serial, operation='hardDelete', data=data, batch_size=batch_size)
        return results

    async def query(self, data, lazy_operation=False):
        """ bulk query """
        results = await self._bulk_operation(operation='query', data=data)

        if lazy_operation:
            return results

        return list_from_generator(results)

    async def query_all(self, data, lazy_operation=False):
        """ bulk queryAll """
        results = await self._bulk_operation(operation='queryAll', data=data)

        if lazy_operation:
            return results
        return list_from_generator(results)
