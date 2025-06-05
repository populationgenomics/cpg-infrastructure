"""
This cloud function runs every hour, 24 hours behind the regular seqr.py

Reason for this is Cromwell billing data processed by seqr.py are updated much later
(more than 5 hours) after seqr finishes its job of migrating data

Reason we need to create a new function and not reused existing is:
We only allow one cloud function to run in parrallel,
we need to prevent from inserting duplicated records.
BQ does not have concept of unigue fields.

This seqr24 basically calls the same seqr code, just start and datime is shifted back 24 hours
"""

import asyncio
import logging
from datetime import datetime, timedelta

import functions_framework
from flask import Request
from seqr import RunMode
from seqr import main as seqr_main

try:
    from . import utils
except ImportError:
    import utils  # type: ignore[no-redef]

logger = utils.logger.getChild('seqr24')


async def main(
    start: datetime | None = None,
    end: datetime | None = None,
    mode: RunMode = 'prod',
    output_path: str | None = None,
    batch_ids: list[str] | None = None,
):
    """Main body function"""
    logger.info(f'Running Seqr24 Billing Aggregation for [{start}, {end}]')
    start, end = utils.process_default_start_and_end(start, end)
    # move 1D (24H) back
    start = start - timedelta(days=1)
    end = end - timedelta(days=1)
    return await seqr_main(start, end, mode, output_path, batch_ids)


@functions_framework.http
def from_request(request: Request):
    """
    From request object, get start and end time if present
    """
    try:
        start, end = utils.get_start_and_end_from_request(request)
    except ValueError as err:
        logger.warning(err)
        logger.warning('Defaulting to None')
        start, end = None, None

    return asyncio.new_event_loop().run_until_complete(main(start, end))


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logging.getLogger('google').setLevel(logging.WARNING)
    logging.getLogger('google.auth.compute_engine._metadata').setLevel(logging.ERROR)
    logging.getLogger('asyncio').setLevel(logging.ERROR)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    test_start, test_end = None, None

    asyncio.new_event_loop().run_until_complete(
        main(
            start=test_start,
            end=test_end,
        ),
    )
