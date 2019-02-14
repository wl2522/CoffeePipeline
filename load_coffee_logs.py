import yaml
import sqlite3
import pandas as pd
from datetime import date
from boxsdk import Client, JWTAuth

with open('config.yml') as config_file:
    config = yaml.load(config_file)
    USER_ID = config['user_id']
    LOCAL_FNAME = config['local_fname']
    AUTH_FNAME = config['auth_fname']

# Load the latest log file exported to Box from the coffee.guru app
fname = 'coffee_guru_log_{}.csv'.format(date.today().strftime('%d%m%Y'))

#auth = JWTAuth(client_id=os.environ['BOX_CLIENT_ID'],
#              client_secret=os.environ['BOX_CLIENT_SECRET'],
#              enterprise_id=os.environ['BOX_ENTERPRISE_ID'],
#              jwt_key_id=os.environ['BOX_JWT_ID']
#              access_token=''
#               )

# Authenticate using JWTAuth credentials stored in a JSON file
sdk = JWTAuth.from_settings_file(AUTH_FNAME)
client = Client(sdk)
user = client.user(user_id='190932855')

# Search for the log file by its file name using the Box search API
log_search = client.as_user(user).search()
search_results = log_search.query(fname,
                                  result_type='file',
                                  file_extensions=['csv'])

log_id = search_results.next()['id']

# Download the search results
with open(LOCAL_FNAME, mode='wb') as log_path:
    client.as_user(user).file(log_id).download_to(log_path)

logs = pd.read_csv(LOCAL_FNAME, sep=';')
logs['Coffee'] = logs['Coffee'].str.replace(' g', '')

# Split the "Note" column into separate columns on the "/" delimiter
notes = logs['Note']
notes = notes.str.replace('(Bean:)|(Grind:)|(Flavor:)|(Balance:)', '')
notes = notes.str.split('/', expand=True)
notes = notes.replace(to_replace=r'\s{2,}', value='', regex=True)
notes.columns = ['Bean', 'Grind', 'Flavor', 'Balance']
logs = logs.drop('Note', axis=1)
logs = pd.concat([logs, notes], axis=1)
logs.to_csv('coffee_guru_log.csv', index=False)

conn = sqlite3.connect('coffee_guru.db')

with open('create_coffee_tables.sql') as create_statement:
    conn.executescript(create_statement.read())
    conn.commit()

logs.to_sql('raw_logs', con=conn, if_exists='replace', index=False)

with open('load_coffee_logs.sql') as insert_statement:
    conn.execute(insert_statement.read())
    conn.commit()

conn.close()
