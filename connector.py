# Import statements
import pyodbc
import os
import pandas as pd

# Global Variables for Standard connection to MS SQL Server
DRIVER = "{ODBC Driver 18 for SQL Server}"
SERVER = "ASFELON"
USER_NAME = "Asfelon/Asfelon"
DB_NAME = 'AdventureWorks2022'


class Connector:
    def __init__(self, driver=DRIVER, server=SERVER, user_name=USER_NAME, db_name=DB_NAME, trusted_connection='yes',
                 trust_server_certificate='yes'):
        """
        Wrapper class that acts like a connection to SQL Server.
        :param driver: Driver used to connect to SQL Server. By default, uses ODBC Driver 18
        :param server: Server name of SQL Server. Set default using global variable SERVER
        :param user_name: Username for the SQL Server. Set default using global variable USER_NAME.
        :param db_name: Database name that you wish to connect to. Set default using global variable DB_NAME.
        :param trusted_connection: Parameter needed for successful connection. Default set to 'yes'.
        :param trust_server_certificate: Parameter needed for successful connection. Default set to 'yes'.
        """
        self.connection = pyodbc.connect(driver=driver,
                                         server=server,
                                         user=user_name,
                                         database_name=db_name,
                                         trusted_connection=trusted_connection,
                                         trustservercertificate=trust_server_certificate)
        self.cursor = self.connection.cursor()
        self.db_name = db_name

    def __del__(self):

        if self.cursor is not None:
            self.cursor.close()
        self.connection.autocommit = False
        if self.connection is not None:
            self.connection.close()

        print("Cursor and Connection closed")

    def set_database(self, db_name: str = DB_NAME):
        """
        Executes Query to set context of database.
        :param db_name: Name of Database to switch context to
        :return: None
        """
        self.cursor.execute(f"USE {db_name}")

    def activate_autocommit(self):
        """
        Set Auto-commit to True for the connection
        :return: None
        """
        self.connection.autocommit = True

    def deactivate_autocommit(self):
        """
        Set Auto-commit to False for the connection
        :return: None
        """
        self.connection.autocommit = False

    def get_cursor(self) -> pyodbc.Cursor:
        """
        Returns the cursor for the connection.
        :return: Cursor for the connection.
        """
        return self.cursor

    def recover_database(self, database_file_name: str):
        """
        Creates a database with the same name as the database file name and restores the database onto SQL Server.

        :param database_file_name: Name of Database file name. Assumes the extension of the file to be ".bak"
        :return: None
        """

        database_file_path = os.path.join(os.getcwd(), database_file_name + ".bak")
        recovery_db_query = f'''
        RESTORE DATABASE {database_file_name}
        FROM DISK = '{database_file_path}'
        WITH REPLACE, RECOVERY;
        '''

        self.activate_autocommit()
        self.cursor.execute(recovery_db_query)
        while self.cursor.nextset():
            pass

    def get_all_tables(self, db_name: str = DB_NAME) -> list:
        """
        Returns a list of Tables with their respective Schema as a pair, ordered by the Schema.
        If given a Database Name, prints the list of table from that Database, else uses the default database set in the
        file

        :param db_name: Database name to get the list of tables. Default value is set to be the global DB_NAME
        :return: List of (Schema, Table) pairs.
        """

        self.set_database(db_name)
        query = f'''
        SELECT TABLE_SCHEMA, TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_SCHEMA != 'dbo'
        ORDER BY TABLE_SCHEMA;
        '''

        tables_list = list()
        self.cursor.execute(query)
        for row in self.cursor:
            tables_list.append(row)

        return tables_list

    def tables_to_csv(self, tables_list: list, db_name: str = DB_NAME):
        """
        Given a list of (Schema, Table) pairs, exports the csv file with data. Recommended to pass the database name
        where the table_list belongs to.

        :param tables_list: List of (Schema, Table) pairs
        :param db_name: Database which owns the table_list. Default value is set to be the global DB_NAME
        :return:
        """

        self.set_database(db_name)
        if not os.path.exists(os.path.join(os.getcwd(), 'Database')):
            os.makedirs('Database')

        for table_schema, table_name in tables_list:
            col_list = list()
            col_query = f'''
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = N'{table_name}';
            '''

            self.cursor.execute(col_query)
            for column_name, data_type in self.cursor:
                col_list.append([column_name, data_type])

            df = pd.DataFrame()
            for column, data_type in col_list:
                if column in ['rowguid', 'ModifiedDate']:
                    continue

                elif data_type == 'geography':
                    query = f'''
                    SELECT {column}.Lat, {column}.Long
                    FROM {table_schema}.{table_name}
                    '''

                    result_list = list()
                    print(table_schema, table_name, column)
                    self.cursor.execute(query)
                    for lat, long in self.cursor:
                        result_list.append([lat, long])
                    df[column] = pd.Series(result_list)

                elif data_type == 'hierarchyid':
                    query = f'''
                    SELECT "{column}".ToString()
                    FROM {table_schema}.{table_name}
                    '''

                    result_list = list()
                    print(table_schema, table_name, column)
                    self.cursor.execute(query)
                    for row in self.cursor:
                        result_list.append(row[0])
                    df[column] = pd.Series(result_list)

                else:
                    query = f'''
                    SELECT "{column}"
                    FROM {table_schema}.{table_name}
                    '''

                    result_list = list()
                    print(table_schema, table_name, column)
                    self.cursor.execute(query)
                    for row in self.cursor:
                        result_list.append(row[0])
                    df[column] = pd.Series(result_list)

            with pd.option_context('expand_frame_repr', False):
                print(df.head())

            df.to_csv(f"Database/{table_schema}.{table_name}.csv", index=False)
