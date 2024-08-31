from connector import Connector
import pandas as pd
import os


connector = Connector()
connection = connector.connection
cursor = connector.get_cursor()

recovery_database_name = "AdventureWorks2022"
# connector.recover_database(recovery_database_name)

connector.tables_to_csv(connector.get_all_tables())
