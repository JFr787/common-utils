import os
import json
import traceback
import psycopg2.extras


class DbHandler:

    CONFIG_FILE = "db_config.json"
    CONFIG_FOLDER = "qgis_py_config"

    def __init__(self):
        config = self.load_config()
        try:
            self.connection = psycopg2.connect(**config)
        except psycopg2.Error as e:
            raise ConnectionError(f"Error connecting to the database: {e}")
        # # Evtl. später auf eine json-Datei zur Konfiguration umsteigen
        # self.connection = psycopg2.connect(
        #     database="places_directory", 
        #     user="postgres", 
        #     password="postgres", 
        #     host="localhost",
        #     port=5432
        # )
    @staticmethod
    def load_config(config_file=None):
        path = os.getenv("APPDATA")
        if config_file is None:
            config_file = os.path.join(path, DbHandler.CONFIG_FOLDER, DbHandler.CONFIG_FILE)
        
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found at {config_file}")

    def select_from_table(self, tablename, whereclause = ""):
        cursor = self.connection.cursor()

        sql = f"SELECT * FROM {tablename}"
        if whereclause:
            sql = f"SELECT * FROM {tablename} WHERE {whereclause}"
        
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        return rows

    # Beispiel dict: {'cust_name': 'Acme Bread', 'cust_id': 101001, 'lic_consumed': 47, 'lic_purchased': 100}
    def create_row(self, tablename, dict_params):
        if not dict_params:
            print("No data provided for insertion.")
            return

        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

        keys = [s.replace("'", '') for s in dict_params.keys()]
        # Anführungzeichen müssen aus dem query-String um die Feldnamen entfernt werden, da sonst die Query ungültig ist
        sql_insert_first_part = """INSERT INTO {} {} """.format(tablename, tuple(keys)).replace("'","")
        sql_insert_query = sql_insert_first_part + """ VALUES {}""".format(tuple(dict_params.values()))
        try:
            cursor.execute(sql_insert_query)
            self.connection.commit()
            cursor.close()
            return True
        except:
            print ('Exception occurred while saving my_table. %s : %s : %s' % (dict_params['cust_name'], dict_params['cust_id'], dict_params['report_date']))
            print (traceback.format_exc())
            self.connection.rollback()
            return False

    def delete_row(self, tablename, where_clause):
        cursor = self.connection.cursor()

        sql_query = f"DELETE FROM {tablename} WHERE {where_clause}"

        try:
            cursor.execute(sql_query)
            self.connection.commit()
            cursor.close()
            return True
        except:
            print (f"Exception occurred while deleting in {tablename}.")
            print (traceback.format_exc())  
            self.connection.rollback()     
            return False