import pandas as pd
import db
from config2.config import config
from sqlalchemy import create_engine
import numpy as np

trans_db = db.DB(
    host=config.db.host,
    user=config.db.user,
    password=config.db.password,
    dbname=config.db.dbname
)

def create_dataframe(file_path):
    df = pd.read_csv(file_path,header=0)
    return df

def create_table():
    sql = '''
        CREATE TABLE points_table(
        season INTEGER,
        rank INTEGER,
        name VARCHAR,
        short_name VARCHAR,
        matchesplayed INTEGER,
        matcheswon INTEGER,
        matcheslost INTEGER,
        noresult INTEGER,
        matchpoints INTEGER,
        nrr FLOAT,
        "for" VARCHAR,
        "against" VARCHAR)'''
    trans_db.execute(sql)

def insert_data(df):
    table_name = 'points_table'
    db_connection_url = 'postgresql://' + config.db.user + ':'+config.db.password +'@' +\
                           config.db.host + '/' + config.db.dbname
    engine = create_engine(db_connection_url)
    df.to_sql(table_name, engine, schema='public', if_exists='replace', index=False)

if __name__ == "__main__":
    file_path = '/Users/adarshbadjate/code/new/Data/points_table.csv'
    df = create_dataframe(file_path)
    create_table()
    insert_data(df)
