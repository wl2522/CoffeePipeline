"""Data pipeline for inserting brewing logs into a SQLite3 database from the
Beanconqueror app.

"""
import argparse
import atexit
import functools
import json
import re
import shutil
import sys
from datetime import datetime, UTC
from zipfile import ZipFile

import pandas as pd
import sqlite3
from box_sdk_gen import BoxClient, BoxJWTAuth, JWTConfig
from loguru import logger
from pytz import timezone
from yaml import safe_load

from box_utils import (catch_exception, get_file_id, download_file,
                       rename_file, upload_log_file)
from log_utils import (check_nan_values, check_scores, validate_text,
                       validate_grind_settings, send_slack_notification)

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)


with open('config/config.yml', encoding='utf-8') as config_file:
    config = safe_load(config_file)

DATESTAMP = datetime.now(UTC).astimezone(
    timezone(config['time_zone'])
).strftime(
    '%Y-%m-%d %I:%M%p'
)

logger.add(
    config['beanconqueror']['logging_fname'],
    mode='a',
    delay=True,
    rotation=config['log_rotation'],
    retention=config['log_retention']
)


def unzip_file(file_name):
    """Extract the given ZIP file and save it as a JSON file
    """
    logger.info("Unzipping downloaded file {} and saving it as name {}",
                config['beanconqueror']['local_fname'],
                file_name)

    # Read the ZIP file and write the contents to a JSON file simultaneously
    with ZipFile(config['beanconqueror']['local_fname'], 'r') as zip_file:
        # Read the JSON file within the ZIP file
        with zip_file.open(file_name, 'r') as read_file:
            # Save the JSON file as a new file
            with open(file_name, 'wb') as write_file:
                # noinspection PyTypeChecker
                shutil.copyfileobj(read_file, write_file)


def extract_data(json_dict):
    """Preprocess and validate the raw data from the coffee brewing logs.

    Since the Beanconqueror app exports logs as a relational database in JSON
    format (which should  be loaded as a Python dictionary), the table
    containing the brew logs needs to be joined on the tables containing the
    bean, grinder, and preparation method information using the UUID columns.

    Additionally, certain relevant string/JSON columns that contain multiple
    fields will be split into separate columns.

    **NOTE**: The UUIDs in each of the Beanconqueror tables are stored within a
    JSON string column named `config`, under the key name `uuid`. This field
    needs to be parsed and saved as a separate column in order to join the
    brew logs table with any of the other tables.

    """
    def add_config_field_columns(table_df):
        """Parse the `config` JSON column fields and append them as new columns.
        """
        parsed_df = pd.concat([
            table_df.drop('config', axis=1),
            pd.json_normalize(table_df['config']),
        ],
            axis=1
        )

        parsed_df = parsed_df.assign(
            updated_at=pd.to_datetime(parsed_df['unix_timestamp'], unit='s')
        )

        return parsed_df

    brews_df = pd.DataFrame.from_dict(json_dict['BREWS'])
    beans_df = pd.DataFrame.from_dict(json_dict['BEANS'])
    grinders_df = pd.DataFrame.from_dict(json_dict['MILL'])
    methods_df = pd.DataFrame.from_dict(json_dict['PREPARATION'])

    df_dict = {}

    beans_df = add_config_field_columns(
        beans_df
    ).rename(
        columns={'beanMix': 'bean_mix'}
    )[[
        'name',
        'roaster',
        'uuid',
        'roast',
        'bean_mix',
        'decaffeinated',
        'bean_roasting_type',
        'updated_at'
    ]]

    grinders_df = add_config_field_columns(grinders_df)[[
        'name',
        'uuid',
        'updated_at'
    ]]

    methods_df = add_config_field_columns(methods_df)[[
        'name',
        'uuid',
        'type',
        'style_type',
        'tools',
        'updated_at'
    ]]

    # Convert the `tools` column into its own table
    method_tools_dfs = methods_df.rename(
        columns={'name': 'preparation_method_name'}
    )[[
        'preparation_method_name',
        'tools'
    ]].explode(
        column='tools',
        ignore_index=True
    )

    methods_df = methods_df.drop('tools', axis=1)

    # Ignore methods that aren't associated with any tools
    method_tools_dfs = method_tools_dfs.loc[
        lambda df: df['tools'].notna()
    ]

    method_tools_df = []

    for method in method_tools_dfs['preparation_method_name'].unique():
        tool_df = pd.json_normalize(
            method_tools_dfs.loc[
                lambda df: df['preparation_method_name'].eq(method),
                'tools'
            ],
            max_level=0
        )
        tool_df = add_config_field_columns(
            tool_df.assign(preparation_method_name=method)
        )[[
            'name',
            'uuid',
            'preparation_method_name',
            'updated_at'
        ]]

        method_tools_df.append(tool_df)

    method_tools_df = pd.concat(method_tools_df, axis=0)

    brews_df = add_config_field_columns(brews_df).assign(
        method_of_preparation_tools=brews_df['method_of_preparation_tools'].astype(str)
    )[[
        'uuid',
        'updated_at',
        'grind_size',
        'grind_weight',
        'method_of_preparation',
        'mill',
        'bean',
        'brew_temperature',
        'brew_time',
        'note',
        'rating',
        'coffee_first_drip_time',
        'coffee_blooming_time',
        'brew_beverage_quantity',
        'brew_beverage_quantity_type',
        'method_of_preparation_tools',
        'favourite',
        'best_brew'
    ]]

    df_dict['beans'] = beans_df
    df_dict['grinders'] = grinders_df
    df_dict['methods'] = methods_df
    df_dict['method_tools'] = method_tools_df
    df_dict['brews'] = brews_df

    return df_dict


def insert_dfs_to_tables(dfs, create_script_fname, conn=None, db_name=None):
    """Insert the dictionary of pandas dataframes exported from the Beanconqueror
    app into SQL tables.
    """
    if conn is None:
        conn = sqlite3.connect(db_name)

    # Create the tables if they don't already exist
    with open(create_script_fname, encoding='utf-8') as create_statement:
        conn.executescript(create_statement.read())
        conn.commit()

    for name, df in dfs.items():
        logger.info("Updating table {}",
                    f'beanconqueror_{name}')

        df.to_sql(
            name=f'beanconqueror_{name}',
            con=conn,
            if_exists='replace',
            index=False
        )


def preprocess_data(df_dict):
    """
    """
    # Join the dataframes into a single log table
    logs = df_dict['brews'].merge(
        df_dict['beans'].rename({
            'uuid': 'bean'
        },
            axis=1
        ).drop(
            'updated_at',
            axis=1
        ),
        on='bean',
        how='inner'
    ).merge(
        df_dict['grinders'].rename({
            'name': 'grinder',
            'uuid': 'mill'
        },
            axis=1
        ).drop(
            'updated_at',
            axis=1
        ),
        on='mill',
        how='inner'
    ).merge(
        df_dict['methods'].rename({
            'name': 'method',
            'uuid': 'method_of_preparation'
        },
            axis=1
        ).drop(
            'updated_at',
            axis=1
        ),
        on='method_of_preparation',
        how='inner'
    ).drop(
        'bean',
        axis=1
    ).rename({
        'name': 'bean'
    },
        axis=1
    )[[
        'uuid',
        'updated_at',
        'grind_size',
        'grind_weight',
        'method',
        'grinder',
        'roaster',
        'bean',
        'brew_temperature',
        'brew_time',
        'note',
        'rating',
        'coffee_first_drip_time',
        'coffee_blooming_time',
        'brew_beverage_quantity',
        'brew_beverage_quantity_type',
        'method_of_preparation_tools',
        'favourite',
        'best_brew'
    ]]

    # Split the "Note" column into separate columns on the "/" delimiter
    notes = logs['note']
    notes = notes.str.replace(
        '(Flavor:)|(Balance:)', '',
        regex=True
    )
    notes = notes.str.split(r'\s*\/\s*', expand=True)

    notes.columns = ['flavor', 'balance']

    # Remove leading/trailing/consecutive whitespace
    notes = notes.replace(to_replace=r'^\s|\s$', value='', regex=True)
    notes = notes.replace(to_replace=r'\s{2,}', value=' ', regex=True)

    notes.columns = ['flavor', 'balance']

    logs = logs.drop('note', axis=1)
    logs = pd.concat([logs, notes], axis=1)

    # Validate the columns containing user inputted data
    check_nan_values(
        logs,
        columns=[
            'rating',
            'roaster',
            'bean',
            'grinder',
            'grind_size',
            'brew_temperature',
            'flavor',
            'balance'
        ],
        timestamp_col='updated_at'
    )
    check_scores(logs['rating'])

    validate_text(note_col=logs['flavor'],
                  adverb_list=config['descriptors']['adverbs'],
                  adjective_list=config['descriptors']['flavors'])

    # Validate only rows where the balance note isn't just the word "Balanced"
    validate_text(note_col=logs.loc[logs['balance'].ne('Balanced'), 'balance'],
                  adverb_list=config['descriptors']['adverbs'],
                  adjective_list=config['descriptors']['balance'])

    # Validate only the grind settings column
    # (because non-missing scores from the app must be integer values from 1-5)
    for grinder in logs['grinder'].unique():
        logger.info("Validating grind setting values for grinder {}",
                    grinder)

        grinder_name = re.sub(r'\s+|-', '_', grinder.lower())

        if grinder_name not in config['grind_setting_ranges']:
            logger.error("Grind settings range not set for grinder {}!",
                         grinder)

            raise ValueError

        grind_setting_range = config['grind_setting_ranges'][grinder_name]

        validate_grind_settings(
            grind_col=logs.loc[logs['grinder'].eq(grinder), 'grind_size'],
            min_val=grind_setting_range['min'],
            max_val=grind_setting_range['max']
        )

    # Create a mapping dictionary of method tool UUIDs to names
    tool_dict = {}

    for tool in df_dict['method_tools'][['name', 'uuid']].to_dict(orient='records'):
        tool_dict[tool['uuid']] = tool['name']

    # Substitute the method tool UUIDs with the tool names
    logs = logs.assign(
        method_of_preparation_tools=logs['method_of_preparation_tools'].replace(
            tool_dict,
            regex=True
        )
    )

    return logs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--rename-log-file',
        action=argparse.BooleanOptionalAction,
        default=False,
        help=("Rename the Beanconqueror log file saved in Box at the end of "
              "the pipeline to avoid having duplicate file names")
    )

    # Authenticate using JWTAuth credentials stored in a JSON file
    jwt_config = JWTConfig.from_config_file(config['auth_fname'])
    auth = BoxJWTAuth(config=jwt_config)
    session_client = BoxClient(auth)
    app_user = session_client.users.get_user_by_id(
        user_id=config['app_user_id']
    )

    logger.info('Successfully authenticated as Box API app user "{}"!',
                app_user.name)

    sys.excepthook = functools.partial(
        catch_exception,
        config=config,
        client=session_client,
        user=app_user,
        log_folder_id=config['beanconqueror']['folder_id'],
        log_file_id=config['beanconqueror']['log_file_id'],
        log_filename=config['beanconqueror']['logging_fname'],
        timestamp=DATESTAMP
    )

    log_file_id = get_file_id(
        file_name=config['beanconqueror']['local_fname'],
        client=session_client,
        user_id=config['user_id'],
        folder_id=config['beanconqueror']['folder_id']
    )

    download_file(
        client=session_client,
        user_id=config['user_id'],
        file_id=log_file_id,
        local_fname=config['beanconqueror']['local_fname'],
        config=config
    )

    json_fname = config['beanconqueror']['local_fname'].replace('.zip', '.json')

    unzip_file(json_fname)

    with open(json_fname, 'r') as f:
        logs_dict = json.load(f)

    log_dfs = extract_data(json_dict=logs_dict)

    sql_conn = sqlite3.connect(config['db_name'])

    insert_dfs_to_tables(
        log_dfs,
        create_script_fname=config['beanconqueror']['create_script'],
        conn=sql_conn,
        db_name=config['db_name']
    )

    log_df = preprocess_data(log_dfs)

    # Upload data to a staging table before inserting into the log table
    log_df.to_sql(
        name=f'beanconqueror_logs_tmp',
        con=sql_conn,
        if_exists='replace',
        index=False
    )

    with open(config['beanconqueror']['insert_script'], encoding='utf-8') as insert_statement:
        sql_conn.execute(insert_statement.read())
        sql_conn.commit()

    sql_conn.close()

    # Rename the file in Box to avoid having duplicate file names
    new_fname = config['beanconqueror']['local_fname'].replace(
        '.zip',
        f'_{DATESTAMP}.zip'
    )
    rename_file(
        client=session_client,
        user_id=config['user_id'],
        file_id=log_file_id,
        new_fname=new_fname,
        config=config
    )

    send_slack_notification(
        timestamp=DATESTAMP,
        config=config,
        fname=config['beanconqueror']['local_fname']
    )

    logger.info('Pipeline finished running!')

    # Upload the log file to Box whenever the script terminates
    atexit.register(
        upload_log_file,
        client=session_client,
        user_id=config['user_id'],
        folder_id=config['beanconqueror']['folder_id'],
        file_id=config['beanconqueror']['log_file_id'],
        config=config,
        log_fname=config['beanconqueror']['logging_fname']
    )
