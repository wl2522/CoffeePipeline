import io
import json
import logging
import traceback

import pandas as pd
import pytz
import requests

from box_sdk_gen.box.errors import BoxAPIError
from box_sdk_gen.managers.uploads import (PreflightFileUploadCheckParent,
                                          UploadFileVersionAttributes)


def catch_exception(err_type, value, trace, config, client, user,
                    timestamp=None):
    """Report any exceptions that were raised during the pipeline run."""
    logger = logging.getLogger(__name__ + '.catch_exception')

    if timestamp is None:
        timestamp = pd.to_datetime(
            'now',
            utc=True
        ).tz_convert(
            config['time_zone']
        ).strftime(
            '%Y-%m-%d %I:%M%p'
        )

    # Extract the error message from the exception traceback
    error_msg = traceback.format_exception_only(err_type, value)[0]

    fail_notif = {"status": "FAIL",
                  "message": f"Failed to update {config['db_name']}!",
                  "error": error_msg.replace('\n', '')
                  }
    fail_msg = f'"{timestamp}": `{str(fail_notif)}`'

    slack_url = 'https://hooks.slack.com/services/' + config['slack_webhook']

    # Report the exception with a Slack message
    requests.post(url=slack_url,
                  data=json.dumps({'text': fail_msg}),
                  headers={"Content-type": "application/json",
                           "Accept": "text/plain"},
                  timeout=config['request_timeout'])

    # Log the exception and update the log file on Slack
    logger.error('Pipeline failed with error: %s',
                 error_msg)
    upload_log_file(client=client,
                    user_id=user,
                    folder_id=config['folder_id'],
                    file_id=config['log_file_id'],
                    config=config,
                    log_fname=config['logging_fname'])

    raise err_type


def get_file_id(file_name, client, user_id, folder_id):
    """Get the file ID associated with each day's log file.

    Since the Box API doesn't support downloading a file
    by providing the file's path, the log file's ID needs
    to be provided instead.

    As a workaround, the `folder.preflight_check()` method is used to get
    the log file ID. This method is meant to be used prior to uploading a
    file to Box to check if that file already exists at the upload destination.

    This method can be used to check if the log file already exists with the
    expected file name and parent folder.

    If this check fails (and returns an error), then that
    indicates that the file already exists. The error
    returned by the Box API will contain the file ID associated
    with the log file, which can then be used to actually download
    the file.

    Parameters
    ----------
    file_name : str
    client : box_sdk_gen BoxClient
    user_id : str
    folder_id : str

    Returns
    -------
    file_id : str
        The file ID that was assigned to the log file by Box

    """
    logger = logging.getLogger(__name__ + '.check_for_file')

    logger.info('Checking folder ID %s for file %s',
                folder_id,
                file_name)

    # Impersonate the account user so that all files and folders are visible
    # (https://github.com/box/box-python-sdk/issues/466#issuecomment-557851831)
    try:
        # Check if the file is already in the folder
        client.with_as_user_header(
            user_id=user_id
        ).uploads.preflight_file_upload_check(
            name=file_name,
            parent=PreflightFileUploadCheckParent(id=folder_id),
            size=None
        )

        # Raise an error if the preflight check succeeds (file doesn't exist)
        err_msg = f"Box folder doesn't contain any file named {file_name}!"
        logger.exception(err_msg)

        raise RuntimeError(err_msg)

    # Return the file ID if the file already exists in the folder
    except BoxAPIError as e:
        logger.info('Found file "%s" in folder ID %s!',
                    file_name,
                    folder_id)

        file_id = e.response_info.body['context_info']['conflicts']['id']

        logger.info('Found file ID %s associated with file name %s!',
                    file_id,
                    file_name)

        return file_id


def download_file(client, user_id, file_id, config):
    """Download the log file that should be uploaded daily.

    Parameters
    ----------
    client : boxsdk Client
    user_id : str
    file_id : str
    config : dict

    Returns
    -------
    logs : pandas DataFrame
        The coffee brewing log data downloaded from the Box folder

    """
    logger = logging.getLogger(__name__ + '.download_file')

    # Get the file's metadata for logging purposes
    log_file = client.with_as_user_header(
        user_id=config['user_id']
    ).files.get_file_by_id(
        file_id
    )

    upload_time = pd.to_datetime(log_file.created_at,
                                 utc=True)

    # Convert the UTC timestamp to EST or EDT
    upload_time = upload_time.tz_convert(pytz.timezone(config['time_zone']))

    logger.info('Downloading file "%s" (file ID: %s), uploaded to Box at %s',
                log_file.name,
                file_id,
                str(upload_time))

    # Download the matching search result using the log file ID
    with open(config['local_fname'], mode='wb') as log_stream:
        client.with_as_user_header(
            user_id=user_id
        ).downloads.download_file_to_output_stream(
            file_id,
            log_stream
        )

    logger.info('Downloaded file ID: %s as "%s"!',
                file_id,
                config['local_fname'])

    logs = pd.read_csv(config['local_fname'],
                       sep=';',
                       iterator=False)

    return logs


def upload_log_file(client, user_id, folder_id, file_id, log_fname, config,
                    timestamp=None):
    """Update the copy of the logging file stored in the Box folder."""
    logger = logging.getLogger(__name__ + '.upload_log_file')

    slack_url = 'https://hooks.slack.com/services/' + config['slack_webhook']

    if timestamp is None:
        timestamp = pd.to_datetime(
            'now',
            utc=True
        ).tz_convert(
            config['time_zone']
        ).strftime(
            '%Y-%m-%d %I:%M%p'
        )

    try:
        logger.info('Updating existing log file %s in Box folder %s...',
                    file_id,
                    folder_id)

        with open(log_fname, 'rb') as f:
            log_stream = f.read()

        client.with_as_user_header(
            user_id=user_id
        ).uploads.upload_file_version(
            file_id,
            UploadFileVersionAttributes(name=config['logging_fname']),
            io.BytesIO(log_stream)
        )

    except BoxAPIError as e:
        if e.message == 'Not Found':
            logger.warning('Log file missing from folder %s! '
                           'Attempting to upload the log as a new file... '
                           '(Check for a Slack notification containing the '
                           'file ID of the newly uploaded log file)',
                           folder_id)

            file = client.as_user(user_id).folder(folder_id).upload(log_fname)

            upload_msg = (f'"{timestamp}": `Uploaded {log_fname} to Box '
                          f"folder {folder_id} with new file ID: {file.id}! "
                          "Remember to update the config file!`")
            requests.post(url=slack_url,
                          data=json.dumps({'text': upload_msg}),
                          headers={"Content-type": "application/json",
                                   "Accept": "text/plain"},
                          timeout=config['request_timeout'])
