"""Database configuration"""
from os import environ, path
from dotenv import load_dotenv
from urllib.parse import quote


basedir = path.abspath(path.dirname(__file__))
load_dotenv(path.join(basedir, '.env'))

DB_API = environ.get('DB_API')
SERVER_USERNAME = environ.get('SERVER_USERNAME')
SERVER_PASSWORD = environ.get('SERVER_PASSWORD')
SERVER_HOST = environ.get('SERVER_HOST')
SERVER_PORT = environ.get('SERVER_PORT')
DRIVER = environ.get('DRIVER')
DATABASE = environ.get('DATABASE')

password = quote("123456789") 

MS_SQL_SERVER_DATABASE_URI=f'mssql+pyodbc://sa:{password}@DESKTOP-HJ5VAQT\MSSQLSERVER3/cdc_fdb_db?driver=ODBC+Driver+17+for+SQL+Server'