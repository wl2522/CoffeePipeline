"""Data pipeline for inserting coffee brewing logs into a SQLite3 database."""
import logging
import re
import sys
import json
import atexit
import sqlite3
import traceback

import numpy as np
import pandas as pd
import requests
import pytz

from yaml import load, SafeLoader
from box_sdk_gen import BoxClient, BoxJWTAuth, JWTConfig
from box_sdk_gen.box.errors import BoxAPIError
from box_sdk_gen.managers.uploads import PreflightFileUploadCheckParent


with open('config.yml', encoding='utf-8') as config_file:
    config = load(config_file, Loader=SafeLoader)

SLACK_URL = 'https://hooks.slack.com/services/' + config['slack_webhook']
DATESTAMP = pd.to_datetime('now', utc=True).tz_convert(config['time_zone'])
DATESTAMP = DATESTAMP.strftime('%Y-%m-%d %I:%M%p')

main_logger = logging.getLogger(__name__)
main_logger.setLevel(logging.INFO)

print_handler = logging.StreamHandler(stream=sys.stdout)
file_handler = logging.FileHandler(filename=config['logging_fname'],
                                   mode='a')
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
print_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
main_logger.addHandler(print_handler)
main_logger.addHandler(file_handler)

# Authenticate using JWTAuth credentials stored in a JSON file
jwt_config = JWTConfig.from_config_file(config['auth_fname'])
auth = BoxJWTAuth(config=jwt_config)
session_client = BoxClient(auth)
app_user = session_client.users.get_user_by_id(
    user_id=config['app_user_id']
)

main_logger.info('Successfully authenticated as the Box API app user "%s"!',
                 app_user.name)


def catch_exception(err_type, value, trace):
    """Report any exceptions that were raised during the pipeline run."""
    logger = logging.getLogger(__name__ + '.catch_exception')

    # Extract the error message from the exception traceback
    error_msg = traceback.format_exception_only(err_type, value)[0]

    fail_notif = {"status": "FAIL",
                  "message": f"Failed to update {config['db_name']}!",
                  "error": error_msg.replace('\n', '')
                  }
    fail_msg = f'"{DATESTAMP}": `{str(fail_notif)}`'

    # Report the exception with a Slack message
    requests.post(url=SLACK_URL,
                  data=json.dumps({'text': fail_msg}),
                  headers={"Content-type": "application/json",
                           "Accept": "text/plain"},
                  timeout=config['request_timeout'])

    # Log the exception and update the log file on Slack
    logger.error('Pipeline failed with error: %s',
                 error_msg)
    upload_log_file(client=session_client,
                    user=app_user,
                    folder_id=config['folder_id'],
                    file_id=config['log_file_id'],
                    log_fname=config['logging_fname'])

    raise err_type


def get_file_id(client, user_id, folder_id):
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
    client : box_sdk_gen BoxClient
    user_id : str
    folder_id : str

    Returns
    -------
    file_id : str
        The file ID that was assigned to the log file by Box

    """
    logger = logging.getLogger(__name__ + '.check_for_file')

    # Check if the latest log file was exported to Box from the coffee.guru app
    fname_date_suffix = f"_{pd.to_datetime('today').strftime('%d%m%Y')}.csv"
    fname = config['local_fname'].replace('.csv', fname_date_suffix)

    logger.info('Checking folder ID %s for file %s',
                folder_id,
                fname)

    # Impersonate the account user so that all files and folders are visible
    # (https://github.com/box/box-python-sdk/issues/466#issuecomment-557851831)
    try:
        # Check if the file is already in the folder
        client.with_as_user_header(
            user_id=user_id
        ).uploads.preflight_file_upload_check(
            name=fname,
            parent=PreflightFileUploadCheckParent(id=folder_id),
            size=None
        )

        # Raise an error if the preflight check succeeds (file doesn't exist)
        err_msg = f"Box folder doesn't contain any file named {fname}!"
        logger.exception(err_msg)

        raise RuntimeError(err_msg)

    # Return the file ID if the file already exists in the folder
    except BoxAPIError as e:
        logger.info('Found file "%s" in folder ID %s!',
                    fname,
                    folder_id)

        file_id = e.response_info.body['context_info']['conflicts']['id']

        logger.info('Found file ID %s associated with file name %s!',
                    file_id,
                    fname)

        return file_id


def download_file(client, user_id, file_id):
    """Download the log file that should be uploaded daily.

    Parameters
    ----------
    client : boxsdk Client
    user_id : str
    file_id : str

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


def check_nan_values(logs):
    """Check for missing user input values prior to updating the database."""
    logger = logging.getLogger(__name__ + '.check_nan_values')

    # Find the row indices containing missing values for each user input column
    nan_msgs = []

    # Find the timestamp of the row with the missing value
    for col in ['Score (out of 5)', 'Bean', 'Grind', 'Flavor', 'Balance']:
        nan_idx = np.where(pd.isnull(logs[col]))[0].tolist()
        nan_times = pd.to_datetime(logs.iloc[nan_idx]['Timestamp'],
                                   unit='s',
                                   utc=True
                                   ).dt.tz_convert('EST')
        nan_times = nan_times.dt.strftime('%Y-%m-%d %I:%M%p')

        if nan_idx:
            msg = (f'Column "{col}" contains missing value(s) in row(s) '
                   f'{nan_idx.tolist()}: {nan_times.tolist()}')
            nan_msgs.append(msg)

    if nan_msgs:
        err_msg = ', \n'.join(nan_msgs)
        logger.exception(err_msg)

        raise ValueError(err_msg)


def check_scores(score_col):
    """Check for rows that where a score was not submitted (score = 0)."""
    logger = logging.getLogger(__name__ + '.check_scores')

    unscored_idx = score_col[score_col == 0].index.tolist()

    if unscored_idx:
        err_msg = f'A brew score was not submitted in row(s): {unscored_idx}'

        logger.exception(err_msg)

        raise ValueError(err_msg)


def validate_text(note_col, adverb_list, adjective_list):
    """Validate a column of tasting notes text from the coffee brewing logs.

    Parameters
    ----------
    notes_col : pandas Series
        The column containing the tasting notes text to validate
    adverbs_list : list of str
        The list of valid adverbs that are allowed to appear in the notes
    adjective_list : list of str
        The list of valid adjectives that are allowed to appear in the notes

    Returns
    -------
    None

    """
    logger = logging.getLogger(__name__ + '.validate_text')

    notes = note_col.str.split(' ', expand=True)
    unexpected_vals = []
    err_msgs = []

    # Confirm that the column contains notes consisting of at least two words
    if len(notes.columns) < 2:
        err_msgs.append(f'Column "{note_col}" contains invalid text!')

    # Confirm that the notes only contain valid adverbs/adjectives
    notes = notes.rename({0: 'adverbs',
                          1: 'adjectives'},
                         axis='columns')

    invalid_adverbs = notes.loc[~notes['adverbs'].isin(adverb_list), 'adverbs']

    if not invalid_adverbs.empty:
        unexpected_vals.append(note_col[invalid_adverbs.index])

    # Find rows that are missing an adjective
    blank_adjs = notes.loc[pd.isna(notes['adjectives']), 'adjectives']

    if not blank_adjs.empty:
        unexpected_vals.append(note_col[blank_adjs.index])

    # Avoid raising TypeError by excluding notes containing only one word
    # (for old records where balance was only described as "Light" or "Heavy")
    invalid_adjs = notes.loc[pd.notna(notes['adjectives']), 'adjectives']

    invalid_adjs = invalid_adjs.loc[~invalid_adjs.isin(adjective_list)]

    if not invalid_adjs.empty:
        unexpected_vals.append(note_col[invalid_adjs.index])

    # Check for any extra words/characters that may be present
    if notes.shape[1] > 2:
        for col in range(2, notes.shape[1]):
            extra_chars = notes.loc[pd.notna(notes[col]), col]
            unexpected_vals.append(note_col[extra_chars.index].tolist())

    if unexpected_vals:
        unexpected_vals = pd.concat(unexpected_vals,
                                    axis=0
                                    ).drop_duplicates().to_dict()

        err_msgs.append((f'Column "{note_col.name}" contains invalid values '
                         f'in row(s): {unexpected_vals}'))

    if err_msgs:
        err_msg = ', \n'.join(err_msgs)

        logger.exception(err_msg)

        raise ValueError(err_msg)


def validate_grind_settings(grind_col, min_val, max_val):
    """Check the grind settings column for invalid and/or out of range values.

    Parameters
    ----------
    grind_col : pandas Series
        The column containing the grind settings data to validate
    min_val : int
        The lower bound for valid grind settings
    max_val : int
        The upper bound for valid grind settings

    Returns
    -------
    None

    """
    logger = logging.getLogger(__name__ + '.validate_grind_settings')

    if not pd.api.types.is_integer_dtype(grind_col.dtype):
        # Check if the column consists entirely of integers in string form
        try:
            grind_col = grind_col.copy().astype(int)

        except ValueError as e:
            non_int_vals = grind_col[~grind_col.map(pd.api.types.is_integer)]

            err_msg = ('Column "Grind" contains non-integer values in row(s) '
                       f'{non_int_vals.index.tolist()}: '
                       f'{non_int_vals.tolist()}')

            logger.exception(err_msg)

            raise ValueError(err_msg) from e

    invalid_vals = grind_col[
        ~grind_col.between(min_val, max_val, inclusive='both')
    ]

    if not invalid_vals.empty:
        err_msg = ('Column "Grind" contains values outside the '
                   f'valid range of [{min_val}, {max_val}] in row(s) '
                   f'{invalid_vals.index.tolist()}: {invalid_vals.tolist()}')
        logger.exception(err_msg)

        raise ValueError(err_msg)


def preprocess_data(logs):
    """Preprocess and validate the raw data from the coffee brewing logs.

    Since the coffee.guru app only contains a "notes" field rather than
    specific fields for describing different aspects of the coffee, the notes
    field contains data in the following format:

    "Bean: <coffee bean name> / Grind: <grind setting> /
     Flavor: <coffee flavor description> Balance: <coffee balance description>"

    Therefore, the column associated with this field needs to be parsed
    and split into separate columns.

    Note: The coffee logs table uses the brew_date column as the primary key.
    Existing records with the same brew date as a record that's being inserted
    will be deleted and overwritten with the new record.
    """
    logger = logging.getLogger(__name__ + '.preprocess_data')

    # Delete the unit of measurement (grams) to convert the column to integers
    logs['Coffee'] = logs['Coffee'].str.replace(' g', '')

    # Split the "Note" column into separate columns on the "/" delimiter
    notes = logs['Note']
    notes = notes.str.replace(
        '(Bean:)|(Grinder:)|(Grind:)|(Flavor:)|(Balance:)', '',
        regex=True
    )
    notes = notes.str.split(r'\s*\/\s*', expand=True)

    # Remove leading/trailing/consecutive whitespace
    notes = notes.replace(to_replace=r'^\s|\s$', value='', regex=True)
    notes = notes.replace(to_replace=r'\s{2,}', value=' ', regex=True)

    notes.columns = ['Bean', 'Grinder', 'Grind', 'Flavor', 'Balance']

    logs = logs.drop('Note', axis=1)
    logs = pd.concat([logs, notes], axis=1)
    logs.to_csv(config['local_fname'], index=False)

    logger.info('Saved the downloaded data to %s!', config['local_fname'])

    # Validate the columns containing user inputted data
    check_nan_values(logs)
    check_scores(logs['Score (out of 5)'])

    validate_text(note_col=logs['Flavor'],
                  adverb_list=config['descriptors']['adverbs'],
                  adjective_list=config['descriptors']['flavors'])

    # Validate only rows where the balance note isn't just the word "Balanced"
    validate_text(note_col=logs.loc[logs['Balance'].ne('Balanced'), 'Balance'],
                  adverb_list=config['descriptors']['adverbs'],
                  adjective_list=config['descriptors']['balance'])

    # Validate only the grind settings column
    # (because non-missing scores from the app must be integer values from 1-5)
    for grinder in logs['Grinder'].unique():
        logger.info("Validating grind setting values for grinder %s", grinder)

        grinder_name = re.sub(r'\s+|-', '_', grinder.lower())

        if grinder_name not in config['grind_setting_ranges']:
            logger.error("Grind settings range not set for grinder %s!",
                         grinder)

            raise ValueError

        grind_setting_range = config['grind_setting_ranges'][grinder_name]


        validate_grind_settings(
            grind_col=logs.loc[logs['Grinder'].eq(grinder), 'Grind'],
            min_val=grind_setting_range['min'],
            max_val=grind_setting_range['max']
        )

    return logs


def update_table(logs):
    """Update the local SQLite3 database with the data downloaded from Box."""
    logger = logging.getLogger(__name__ + '.update_table')

    conn = sqlite3.connect(config['db_name'])

    # Create the table if it doesn't already exist
    with open(config['create_script'], encoding='utf-8') as create_statement:
        conn.executescript(create_statement.read())
        conn.commit()

    logs.to_sql('raw_logs', con=conn, if_exists='replace', index=False)

    with open(config['insert_script'], encoding='utf-8') as insert_statement:
        conn.execute(insert_statement.read())
        conn.commit()

    conn.close()

    logger.info('Successfully updated %s!', config['db_name'])


def upload_log_file(client, user, folder_id, file_id, log_fname):
    """Update the copy of the logging file stored in the Box folder."""
    logger = logging.getLogger(__name__ + '.upload_log_file')

    try:
        logger.info('Updating existing log file %s in Box folder %s...',
                    file_id,
                    folder_id)

        client.as_user(user).file(file_id).update_contents(log_fname)

    except BoxAPIError as e:
        if e.message == 'Not Found':
            logger.warning('Log file missing from folder %s! '
                           'Attempting to upload the log as a new file... '
                           '(Check for a Slack notification containing the '
                           'file ID of the newly uploaded log file)',
                           folder_id)

            file = client.as_user(user).folder(folder_id).upload(log_fname)

            upload_msg = (f'"{DATESTAMP}": `Uploaded {log_fname} to Box '
                          f"folder {folder_id} with new file ID: {file.id}! "
                          "Remember to update the config file!`")
            requests.post(url=SLACK_URL,
                          data=json.dumps({'text': upload_msg}),
                          headers={"Content-type": "application/json",
                                   "Accept": "text/plain"},
                          timeout=config['request_timeout'])


if __name__ == '__main__':
    sys.excepthook = catch_exception

    log_file_id = get_file_id(client=session_client,
                              user_id=config['user_id'],
                              folder_id=config['folder_id'])

    df = download_file(client=session_client,
                       user_id=config['user_id'],
                       file_id=log_file_id)
    df = preprocess_data(logs=df)
    update_table(logs=df)

    success_notif = {"status": "SUCCESS",
                     "message": f"Successfully updated {config['db_name']}!"
                     }
    success_msg = f'"{DATESTAMP}": `{str(success_notif)}`'

    requests.post(url=SLACK_URL,
                  data=json.dumps({'text': success_msg}),
                  headers={"Content-type": "application/json",
                           "Accept": "text/plain"},
                  timeout=config['request_timeout'])

    main_logger.info('Pipeline finished running!')

    # Upload the log file to Box whenever the script terminates
    atexit.register(upload_log_file,
                    client=session_client,
                    user=app_user,
                    folder_id=config['folder_id'],
                    file_id=config['log_file_id'],
                    log_fname=config['logging_fname'])
