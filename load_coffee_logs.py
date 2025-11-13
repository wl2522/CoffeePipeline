"""Data pipeline for inserting coffee brewing logs into a SQLite3 database."""
import functools
import logging
import re
import sys
import atexit

import pandas as pd

from yaml import load, SafeLoader
from box_sdk_gen import BoxClient, BoxJWTAuth, JWTConfig

from box_utils import (catch_exception, get_file_id, download_file,
                       upload_log_file)
from log_utils import (check_nan_values, check_scores, validate_text,
                       validate_grind_settings, update_table,
                       send_slack_notification)


with open('config.yml', encoding='utf-8') as config_file:
    config = load(config_file, Loader=SafeLoader)

DATESTAMP = pd.to_datetime(
    'now',
    utc=True
).tz_convert(
    config['time_zone']
).strftime(
    '%Y-%m-%d %I:%M%p'
)

main_logger = logging.getLogger(__name__)
main_logger.setLevel(logging.INFO)

print_handler = logging.StreamHandler(stream=sys.stdout)
file_handler = logging.FileHandler(filename=config['logging_fname'],
                                   mode='a')
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

print_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)
main_logger.addHandler(print_handler)
main_logger.addHandler(file_handler)


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


if __name__ == '__main__':
    # Authenticate using JWTAuth credentials stored in a JSON file
    jwt_config = JWTConfig.from_config_file(config['auth_fname'])
    auth = BoxJWTAuth(config=jwt_config)
    session_client = BoxClient(auth)
    app_user = session_client.users.get_user_by_id(
        user_id=config['app_user_id']
    )

    main_logger.info('Successfully authenticated as Box API app user "%s"!',
                     app_user.name)

    sys.excepthook = functools.partial(
        catch_exception,
        config=config,
        client=session_client,
        user=app_user,
        timestamp=DATESTAMP
    )

    # Check if the latest log file was exported to Box from the coffee.guru app
    fname_date_suffix = f"_{pd.to_datetime('today').strftime('%d%m%Y')}.csv"
    fname = config['local_fname'].replace('.csv', fname_date_suffix)

    log_file_id = get_file_id(file_name=fname,
                              client=session_client,
                              user_id=config['user_id'],
                              folder_id=config['folder_id'],
                              config=config)

    df = download_file(client=session_client,
                       user_id=config['user_id'],
                       file_id=log_file_id,
                       config=config)
    df = preprocess_data(logs=df)

    update_table(logs=df, config=config)

    send_slack_notification(timestamp=DATESTAMP,
                            config=config)

    main_logger.info('Pipeline finished running!')

    # Upload the log file to Box whenever the script terminates
    atexit.register(upload_log_file,
                    client=session_client,
                    user=app_user,
                    folder_id=config['folder_id'],
                    file_id=config['log_file_id'],
                    log_fname=config['logging_fname'])
