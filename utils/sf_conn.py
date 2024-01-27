import os
from dotenv import load_dotenv
import snowflake.connector as sfc
from snowflake.connector.pandas_tools import write_pandas
import json


_ = load_dotenv()

class Config():

    def __init__(self):
         
         self.sf_username = os.getenv('SF_USERNAME')
         self.sf_password = os.getenv('SF_PASSWORD')
         self.sf_account = os.getenv('SF_ACCOUNT')


    def create_sf_conn(self,
                    wh='AI_WH',
                    r='DATA_ENG',
                    keep_alive=False):
        conn = sfc.connect(
                user = self.sf_username,
                password = self.sf_password,
                account = self.sf_account,
                warehouse = wh,
                role = r,
                database = 'analytics_justin', 
                schema = 'spotify_schema', 
                client_session_keep_alive=keep_alive)
        return conn
    
    def drop_and_recreate(self, conn, df, table_name):
        try:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                write_pandas(conn, df, table_name, auto_create_table = True)
                cursor.close()
        except sfc.Error as e:
                print(f"Error occured: {e}")
    

    
 
        

    def close_sf_conn(self, conn):
            conn.close()