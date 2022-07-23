"""Data pipeline for inserting coffee brewing logs into a SQLite3 database."""
import logging
import sys
import json
import atexit
import sqlite3
import traceback
import numpy as np
import pandas as pd
import requests
from yaml import load, SafeLoader
from boxsdk import Client, JWTAuth
from boxsdk.exception import BoxAPIException


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
sdk = JWTAuth.from_settings_file(config['auth_fname'])
session_client = Client(sdk)
app_user = session_client.user(user_id=str(config['app_user_id']))

main_logger.info('Successfully authenticated as the Box API app user "%s"!',
                 app_user.get().name)


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
                           "Accept": "text/plain"})

    # Log the exception and update the log file on Slack
    logger.error('Pipeline failed with error: %s',
                 error_msg)
    upload_log_file(client=session_client,
                    user=app_user,
                    folder_id=config['folder_id'],
                    log_file_id=config['log_file_id'],
                    log_fname=config['logging_fname'])

    raise(err_type)


def download_file(client, user):
    """Search the Box folder for the log file that should be uploaded daily.

    Note: Since the search() method returns inexact matches, the name of the
    search result file must be compared with the expected file name before
    continuing.

    Returns
    -------
    logs : pandas DataFrame
        The coffee brewing log data downloaded from the Box folder
    log_id : str or int
        The file ID that was assigned to the log file by Box

    """
    logger = logging.getLogger(__name__ + '.download_file')

    # Load the latest log file exported to Box from the coffee.guru app
    fname = '{}_{}.csv'.format(config['local_fname'].replace('.csv', ''),
                               pd.to_datetime('today').strftime('%d%m%Y'))

    # Search for the log file by its file name using the Box search API
    log_search = client.as_user(user).search()
    search_results = log_search.query(fname,
                                      result_type='file',
                                      file_extensions=['csv'])
    log_id = search_results.next()['id']
    result_fname = client.as_user(user).file(log_id).get().name
    upload_time = client.as_user(user).file(log_id).get().created_at
    upload_time = pd.to_datetime(upload_time, utc=True).tz_convert('EST')

    logger.info('Found file named %s (file ID: %s), uploaded to Box at %s',
                result_fname,
                log_id,
                str(upload_time))

    # Ensure that the returned match is the correct file
    if not fname == result_fname:
        err_msg = "Box folder doesn't contain any file named %s!" % fname
        logger.exception(err_msg)

        raise RuntimeError(err_msg)

    # Download the matching search result using the log file ID
    with open(config['local_fname'], mode='wb') as log_path:
        client.as_user(user).file(log_id).download_to(log_path)

    logger.info('Downloaded file named %s (file ID: %s) as %s!',
                fname,
                log_id,
                config['local_fname'])

    logs = pd.read_csv(config['local_fname'], sep=';')

    return logs, log_id


def check_nan_values(logs):
    """Check for missing user input values prior to updating the database."""
    logger = logging.getLogger(__name__ + '.check_nan_values')

    # Find the row indices containing missing values for each user input column
    nan_msgs = list()

    # Find the timestamp of the row with the missing value
    for col in ['Score (out of 5)', 'Bean', 'Grind', 'Flavor', 'Balance']:
        nan_idx = np.where(pd.isnull(logs[col]))[0]
        nan_times = pd.to_datetime(logs.iloc[nan_idx]['Timestamp'],
                                   unit='s',
                                   utc=True
                                   ).dt.tz_convert('EST')
        nan_times = nan_times.dt.strftime('%Y-%m-%d %I:%M%p')

        if len(nan_idx) > 0:
            msg = (f'Column "{col}" contains missing value(s) in row(s) '
                   f'{nan_idx.tolist()}: {nan_times.tolist()}')
            nan_msgs.append(msg)

    if len(nan_msgs) > 0:
        err_msg = ', \n'.join(nan_msgs)
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
    unexpected_idx = list()
    unexpected_vals = list()

    # Confirm that the column contains notes consisting of at least two words
    if len(notes.columns) < 2:
        err_msg = 'Column "%s" contains invalid text!' % note_col
        logger.exception('Column "%s" contains invalid text!',
                         note_col)

        raise ValueError(err_msg)

    # Confirm that the notes only contain valid adverbs/adjectives
    notes = notes.rename({0: 'adverbs',
                          1: 'adjectives'},
                         axis='columns')

    invalid_adverbs = notes.loc[
        ~notes['adverbs'].str.contains('|'.join(adverb_list),
                                       regex=True),
        'adverbs']
    unexpected_vals += list(invalid_adverbs.values)
    unexpected_idx += list(invalid_adverbs.index)

    # Find rows that are missing an adjective
    blank_adjs = notes.loc[pd.isna(notes['adjectives']), 'adjectives']
    unexpected_vals += list(blank_adjs.values)
    unexpected_idx += list(blank_adjs.index)

    # Avoid raising TypeError by excluding notes containing only one word
    # (for old records where balance was only described as "Light" or "Heavy")
    invalid_adjs = notes.loc[pd.notna(notes['adjectives']), 'adjectives']
    invalid_adjs = invalid_adjs.loc[
        ~invalid_adjs.str.contains('|'.join(adjective_list),
                                   case=True,
                                   regex=True)]
    unexpected_vals += list(invalid_adjs.values)
    unexpected_idx += list(invalid_adjs.index)

    # Check for any extra words/characters that may be present
    if notes.shape[1] > 2:
        for col in range(2, notes.shape[1]):
            extra_chars = notes.loc[pd.notna(notes[col]), col]
            unexpected_vals += list(extra_chars.values)
            unexpected_idx += list(extra_chars.index)

    # De-duplicate and sort the row numbers/values found in the data
    unexpected_idx = sorted(list(set(unexpected_idx)))
    unexpected_vals = sorted(list(set(unexpected_vals)))

    if len(unexpected_idx) > 0:
        err_msg = 'Column "%s" contains invalid values in row(s): %s, %s' % (
            note_col.name,
            str(unexpected_idx),
            str(unexpected_vals))
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

    invalid_vals = grind_col[~grind_col.between(min_val, max_val,
                                                inclusive='both')]

    if len(invalid_vals) > 0:
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
    notes = notes.str.replace('(Bean:)|(Grind:)|(Flavor:)|(Balance:)', '')
    notes = notes.str.split(r'\s*\/\s*', expand=True)
    notes = notes.replace(to_replace=r'\s{2,}|^\s|\s$', value='', regex=True)
    notes.columns = ['Bean', 'Grind', 'Flavor', 'Balance']

    logs = logs.drop('Note', axis=1)
    logs = pd.concat([logs, notes], axis=1)
    logs.to_csv(config['local_fname'], index=False)

    logger.info('Saved the downloaded data to %s!', config['local_fname'])

    # Validate the columns containing user inputted data
    check_nan_values(logs)

    validate_text(note_col=logs['Flavor'],
                  adverb_list=config['descriptors']['adverbs'],
                  adjective_list=config['descriptors']['flavors'])

    # Validate only rows where the balance note isn't just the word "Balanced"
    validate_text(note_col=logs.loc[logs['Balance'] != 'Balanced', 'Balance'],
                  adverb_list=config['descriptors']['adverbs'],
                  adjective_list=config['descriptors']['balance'])

    # Validate only the grind settings column
    # (because non-missing scores from the app must be integer values from 1-5)
    validate_grind_settings(grind_col=logs['Grind'],
                            min_val=config['min_grind_setting'],
                            max_val=config['max_grind_setting'])

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


def upload_log_file(client, user, folder_id, log_file_id, log_fname):
    """Update the copy of the logging file stored in the Box folder."""
    logger = logging.getLogger(__name__ + '.upload_log_file')

    try:
        logger.info('Updating existing log file %s in Box folder %s...',
                    log_file_id,
                    folder_id)

        client.as_user(user).file(log_file_id).update_contents(log_fname)

    except BoxAPIException as e:
        if e.message == 'Not Found':
            logger.warning(('Log file missing from folder %s! '
                            'Attempting to upload the log as a new file... '
                            '(Check for a Slack notification containing the '
                            'file ID of the newly uploaded log file)'),
                           folder_id)

            file = client.as_user(user).folder(folder_id).upload(log_fname)

            upload_msg = (f'"{DATESTAMP}": `Uploaded {log_fname} to Box '
                          f"folder {folder_id} with new file ID: {file.id}! "
                          "Remember to update the config file!`")
            requests.post(url=SLACK_URL,
                          data=json.dumps({'text': upload_msg}),
                          headers={"Content-type": "application/json",
                                   "Accept": "text/plain"})


if __name__ == '__main__':
    sys.excepthook = catch_exception

    df, file_id = download_file(client=session_client,
                                user=app_user)
    df = preprocess_data(logs=df)
    update_table(logs=df)

    success_notif = {"status": "SUCCESS",
                     "message": f"Successfully updated {config['db_name']}!"
                     }
    success_msg = f'"{DATESTAMP}": `{str(success_notif)}`'

    requests.post(url=SLACK_URL,
                  data=json.dumps({'text': success_msg}),
                  headers={"Content-type": "application/json",
                           "Accept": "text/plain"})

    main_logger.info('Pipeline finished running!')

    # Upload the log file to Box whenever the script terminates
    atexit.register(upload_log_file,
                    client=session_client,
                    user=app_user,
                    folder_id=config['folder_id'],
                    log_file_id=config['log_file_id'],
                    log_fname=config['logging_fname'])
