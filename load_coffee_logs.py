import yaml
import sqlite3
import pandas as pd
from datetime import date
from boxsdk import Client, JWTAuth

with open('config.yml') as config_file:
    config = yaml.load(config_file)

# Load the latest log file exported to Box from the coffee.guru app
fname = '{}_{}.csv'.format(config['local_fname'],
                           date.today().strftime('%d%m%Y'))

# Authenticate using JWTAuth credentials stored in a JSON file
sdk = JWTAuth.from_settings_file(config['auth_fname'])
client = Client(sdk)
user = client.user(user_id=str(config['user_id']))

# Search for the log file by its file name using the Box search API
log_search = client.as_user(user).search()
search_results = log_search.query(fname,
                                  result_type='file',
                                  file_extensions=['csv'])

# Download the search results using the log file ID
log_id = search_results.next()['id']

with open(config['local_fname'], mode='wb') as log_path:
    client.as_user(user).file(log_id).download_to(log_path)

logs = pd.read_csv(config['local_fname'], sep=';')
logs['Coffee'] = logs['Coffee'].str.replace(' g', '')

# Split the "Note" column into separate columns on the "/" delimiter
notes = logs['Note']
notes = notes.str.replace('(Bean:)|(Grind:)|(Flavor:)|(Balance:)', '')
notes = notes.str.split('/', expand=True)
notes = notes.replace(to_replace=r'\s{2,}', value='', regex=True)
notes.columns = ['Bean', 'Grind', 'Flavor', 'Balance']
logs = logs.drop('Note', axis=1)
logs = pd.concat([logs, notes], axis=1)
logs.to_csv(config['local_fname'], index=False)

conn = sqlite3.connect('coffee_guru.db')

with open(config['create_script']) as create_statement:
    conn.executescript(create_statement.read())
    conn.commit()

logs.to_sql('raw_logs', con=conn, if_exists='replace', index=False)

with open(config['insert_script']) as insert_statement:
    conn.execute(insert_statement.read())
    conn.commit()

conn.close()
