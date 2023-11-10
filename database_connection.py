# database_connection.py
# Author: sourabhB
import mysql.connector

mysql_host = ''
mysql_port = 1234
mysql_database = '###'
mysql_user = '#####'
mysql_password = '######'

def connect_to_mysql():
    return mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        database=mysql_database,
        user=mysql_user,
        password=mysql_password
    )
