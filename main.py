from connector import Connector


if "__name__" == "__main__":
    connector = Connector()
    connection = connector.connection
    cursor = connector.get_cursor()

    recovery_database_name = "AdventureWorks2022"
    # connector.recover_database(recovery_database_name)

    connector.tables_to_csv(connector.get_all_tables())
