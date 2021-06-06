import pandas as pd
import os
import psycopg2
import yaml
from io import StringIO

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

_WORKING_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(_WORKING_DIRECTORY, 'config.yaml')) as file:
    _CONFIG = yaml.full_load(file)


def read_raw_data():
    """
        Fetches data from kaggle.
        Filters out accidents we don't have vehicles information for.
        Joins accidents + vehicles dataframes. 
    """
    os.system('kaggle datasets download -d tsiaras/uk-road-safety-accidents-and-vehicles -w --unzip -p \"{}\"'.format(_WORKING_DIRECTORY))

    acc = pd.read_csv(_CONFIG['accidents_csv'], usecols=_CONFIG['list_of_acc_columns'], dtype={'Accident_Index': str})
    acc.columns = acc.columns.str.lower()
    veh = pd.read_csv(_CONFIG['vehicles_csv'], usecols=_CONFIG['list_of_veh_columns'], dtype={'Accident_Index': str})
    veh.columns = veh.columns.str.lower()

    acc = acc[acc['accident_index'].isin(veh['accident_index'])]
    acc.sort_values(by=['date', 'time'], inplace=True)
    acc.drop(columns=['time'], inplace=True)

    is_take_subset = bool(_CONFIG['is_take_only_100_recent_accidents'])
    acc_subset = acc[-100:] if is_take_subset else acc

    merged = pd.merge(acc_subset, veh, on='accident_index', how='inner')

    return merged


def create_db():
    """
        Checks if a database with the specified name already exists. 
        If yes - it skips the creation.
        If no - creates the database.
    """
    with psycopg2.connect(host=_CONFIG['db_host'], \
                        port=_CONFIG['db_port'], \
                        user=_CONFIG['db_user'], \
                        password=_CONFIG['db_pass']) as conn:
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = '{}'".format(_CONFIG['db_name']))
            is_exist = cursor.fetchone()
            if not is_exist:
                cursor.execute("create database {} owner {}".format(_CONFIG['db_name'], _CONFIG['db_user']))


def deploy_schema(conn):
    """
        Deploys a predefined schema.
    """
    with conn.cursor() as cursor:
        with open(os.path.join(_WORKING_DIRECTORY, _CONFIG['db_schema'])) as schema:
            cursor.execute(schema.read())
            conn.commit()


def lookup_values_in_db(conn, df):
    """
        The fact table holds foreign keys to a number of dimention tables. 
        This function looks up the dimention table ids.
        With every iteration we add a new *_id column to our frame, while dropping the string representation of the field.
    """
    with conn.cursor() as cursor:
        for dim in _CONFIG['dimension_lookups']:
            dim_table = 'dim_' + dim

            cursor.execute('SELECT * FROM {}'.format(dim_table))
            colnames = [desc[0] for desc in cursor.description]
            lookup_df = pd.DataFrame(cursor.fetchall(), columns=colnames)

            df = pd.merge(df,
                        lookup_df,
                        how='left',
                        on=dim)
            df.drop(columns=[dim], inplace=True)

    return df


def copy_to_db(conn, df):
    """
        Checks for the order of the dataframe columns and inserts it to the fact table.
    """
    with conn.cursor() as cursor:
        cursor.execute('SELECT * FROM {} LIMIT 1'.format(_CONFIG['fact_table']))
        colnames = [desc[0] for desc in cursor.description]
        df = df[colnames]

        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        try:
            cursor.copy_from(buffer, _CONFIG['fact_table'], null="", sep=',')
            conn.commit()
        except Exception as error:
            print("Error: {}".format(error)) 
            conn.rollback()


def main():
    df = read_raw_data()
    create_db()

    with psycopg2.connect(host=_CONFIG['db_host'], \
                        database=_CONFIG['db_name'], \
                        port=_CONFIG['db_port'], \
                        user=_CONFIG['db_user'], \
                        password=_CONFIG['db_pass']) as conn:
        deploy_schema(conn)
        df = lookup_values_in_db(conn, df)
        copy_to_db(conn, df)


if __name__ == '__main__':
    main()