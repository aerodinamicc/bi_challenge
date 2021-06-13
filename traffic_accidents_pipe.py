import pandas as pd
import os
import psycopg2
import yaml
from io import StringIO
from kaggle.api.kaggle_api_extended import KaggleApi
from zipfile import ZipFile
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class Pipeline():
    def __init__(self):
        self._WORKING_DIRECTORY = os.path.dirname(os.path.realpath(__file__))

        with open(os.path.join(self._WORKING_DIRECTORY, 'config.yaml')) as file:
            self._CONFIG = yaml.full_load(file)


    def download_data(self, api, dir_list):
        """
            Checks if files are pre-downloaded and if not fetches them through the Kaggle API.
        """
        if self._CONFIG['accidents_csv'] not in dir_list or self._CONFIG['vehicles_csv'] not in dir_list:
            if self._CONFIG['zipped_dataset'] not in dir_list:
                api.dataset_download_files(self._CONFIG['kaggle_dataset'], \
                                            path=_WORKING_DIRECTORY)

            with ZipFile(os.path.join(self._WORKING_DIRECTORY, self._CONFIG['zipped_dataset'])) as zip_file:
                    zip_file.extractall(self._WORKING_DIRECTORY)


    def read_in_csv(self, file, columns):
        """
            Reads in a pandas dataframe and lowercases its columns.
        """
        source_df = pd.read_csv(os.path.join(self._WORKING_DIRECTORY, file), usecols=columns, dtype={'Accident_Index': str}, engine='python')
        source_df.columns = source_df.columns.str.lower()

        return source_df


    def merge_data(self, acc, veh):
        """
            Filters out accidents we don't have vehicles information for.
            Joins accidents + vehicles dataframes. 
        """
        acc = acc[acc['accident_index'].isin(veh['accident_index'])]
        acc.sort_values(by=['date', 'time'], inplace=True)
        acc.drop(columns=['time'], inplace=True)

        is_take_subset = bool(self._CONFIG['is_take_only_100_recent_accidents'])
        acc_subset = acc[-100:] if is_take_subset else acc

        merged = pd.merge(acc_subset, veh, on='accident_index', how='inner')
        return merged


    def create_db(self, conn):
        """
            Checks if a database with the specified name already exists. 
            If yes - it skips the creation.
            If no - creates the database.
        """
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{}'".format(self._CONFIG['new_db_name']))
            is_exist = cursor.fetchone()
            if not is_exist:
                cursor.execute("create database {} owner {}".format(self._CONFIG['new_db_name'], self._CONFIG['db_user']))


    def deploy_schema(self, conn):
        """
            Deploys a predefined schema.
        """
        with conn.cursor() as cursor:
            with open(os.path.join(self._WORKING_DIRECTORY, self._CONFIG['db_schema'])) as schema:
                cursor.execute(schema.read())
                conn.commit()
                #check whether connection commit is called once


    def lookup_values_in_db(self, conn, df):
        """
            The fact table holds foreign keys to a number of dimention tables. 
            This function looks up the dimention table ids.
            With every iteration we add a new *_id column to our frame, while dropping the string representation of the field.
        """
        with conn.cursor() as cursor:
            for dim in self._CONFIG['dimension_lookups']:
                dim_table = 'dim_' + dim

                cursor.execute('SELECT * FROM {}'.format(dim_table))
                colnames = [desc[0] for desc in cursor.description]
                lookup_df = pd.DataFrame(cursor.fetchall(), columns=colnames)

                df = pd.merge(df,
                            lookup_df,
                            how='left',
                            on=dim)
                df.drop(columns=[dim], inplace=True)
        # test that merged is 8 columns and 170 rows
        # check that *_id columns = len(self._CONFIG['dimension_lookups'])
        return df


    def fetch_columns_order_from_fact_table(self, conn):
        """
            Used for fetching columns order from the fact table.
        """
        with conn.cursor() as cursor:
            cursor.execute('SELECT * FROM {} LIMIT 1'.format(self._CONFIG['fact_table']))
            
            return [desc[0] for desc in cursor.description]


    def copy_to_db(self, conn, df):
        """
            Inserts dataframe to the fact table.
        """
        with conn.cursor() as cursor:
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            try:
                cursor.copy_from(buffer, self._CONFIG['fact_table'], null="", sep=',')
                #check whether connection commit is called once
                conn.commit()
            except Exception as error:
                #simulate side effects
                print("Error: {}".format(error)) 
                conn.rollback()


    def run(self):
        dir_list = os.listdir(self._WORKING_DIRECTORY)
        api = KaggleApi()
        api.authenticate()
        self.download_data(api, dir_list)

        acc = self.read_in_csv(self._CONFIG['accidents_csv'], self._CONFIG['list_of_acc_columns'])
        veh = self.read_in_csv(self._CONFIG['vehicles_csv'], self._CONFIG['list_of_veh_columns'])
        df = self.merge_data(acc, veh)

        with psycopg2.connect(host=self._CONFIG['db_host'], \
                                port=self._CONFIG['db_port'], \
                                database=self._CONFIG['existing_db_name'], \
                                user=self._CONFIG['db_user'], \
                                password=self._CONFIG['db_pass']) as conn:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.create_db(conn)

        with psycopg2.connect(host=self._CONFIG['db_host'], \
                            database=self._CONFIG['new_db_name'], \
                            port=self._CONFIG['db_port'], \
                            user=self._CONFIG['db_user'], \
                            password=self._CONFIG['db_pass']) as conn:
            self.deploy_schema(conn)
            df = self.lookup_values_in_db(conn, df)
            df = df[self.fetch_columns_order_from_fact_table(conn)]
            self.copy_to_db(conn, df)

 
if __name__ == '__main__':
    pipe = Pipeline()
    pipe.run()