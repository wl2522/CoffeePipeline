import sqlite3
import pandas as pd
from yaml import load, SafeLoader
from boxsdk import Client, JWTAuth

with open('config.yml') as config_file:
    config = load(config_file, Loader=SafeLoader)


# Authenticate using JWTAuth credentials stored in a JSON file
sdk = JWTAuth.from_settings_file(config['auth_fname'])
session_client = Client(sdk)
app_user = session_client.user(user_id=str(config['app_user_id']))

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

    logs = pd.read_csv(config['local_fname'], sep=';')

    return logs, log_id


def update_table(logs):
    """Update the local SQLite3 database with the data downloaded from Box.

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

    Parameters
    ----------
    logs : pandas DataFrame

    Returns
    -------
    None

    """
    logger = logging.getLogger(__name__ + '.update_table')

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

    conn = sqlite3.connect(config['db_name'])

    # Create the table if it doesn't already exist
    with open(config['create_script']) as create_statement:
        conn.executescript(create_statement.read())
        conn.commit()

    logs.to_sql('raw_logs', con=conn, if_exists='replace', index=False)

    with open(config['insert_script']) as insert_statement:
        conn.execute(insert_statement.read())
        conn.commit()

    conn.close()

    logger.info('Successfully updated %s!', config['db_name'])









if __name__ == '__main__':
    df, file_id = download_file(client=session_client,
                                user=app_user)
    update_table(logs=df)

