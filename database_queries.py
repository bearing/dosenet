from mysql_tools.mysql_tools import SQLObject


def get_stations():
    DB = SQLObject()
    df= DB.getStations()
    print(df)
