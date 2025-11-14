import json
import logging

import numpy as np
import pandas as pd
import requests
import sqlite3


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
    note_col : pandas Series
        The column containing the tasting notes text to validate
    adverb_list : list of str
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
    blank_adjectives = notes.loc[pd.isna(notes['adjectives']), 'adjectives']

    if not blank_adjectives.empty:
        unexpected_vals.append(note_col[blank_adjectives.index])

    # Avoid raising TypeError by excluding notes containing only one word
    # (for old records where balance was only described as "Light" or "Heavy")
    invalid_adjectives = notes.loc[pd.notna(notes['adjectives']), 'adjectives']

    invalid_adjectives = invalid_adjectives.loc[
        ~invalid_adjectives.isin(adjective_list)
    ]

    if not invalid_adjectives.empty:
        unexpected_vals.append(note_col[invalid_adjectives.index])

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


def update_table(logs, config):
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


def send_slack_notification(timestamp, config):
    """Use a Slack webhook URL to send a notification of a successful
    pipeline run.
    """
    success_notif = {"status": "SUCCESS",
                     "message": f"Successfully updated {config['db_name']}!"
                     }
    success_msg = f'"{timestamp}": `{str(success_notif)}`'

    slack_url = 'https://hooks.slack.com/services/' + config['slack_webhook']

    requests.post(url=slack_url,
                  data=json.dumps({'text': success_msg}),
                  headers={"Content-type": "application/json",
                           "Accept": "text/plain"},
                  timeout=config['request_timeout'])
