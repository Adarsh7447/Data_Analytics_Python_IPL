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
    df['season'] = df['season'].fillna(0).astype(int)
    return df

def create_table():
    sql = '''
        CREATE TABLE IF NOT EXISTS all_season_bowling_card(
        season INTEGER,
        match_id INTEGER,
        match_name VARCHAR,
        home_team VARCHAR,
        away_team VARCHAR,
        bowling_team VARCHAR,
        venue VARCHAR,
        city VARCHAR,
        country VARCHAR,
        innings_id INTEGER,
        name VARCHAR,
        fullName VARCHAR,
        overs FLOAT,
        maidens INTEGER,
        conceded INTEGER,
        wickets INTEGER,
        economyRate VARCHAR,
        dots INTEGER,
        foursConceded INTEGER,
        sixesConceded INTEGER,
        wides INTEGER,
        noballs INTEGER,
        captain BOOLEAN,
        href VARCHAR)'''
    trans_db.execute(sql)

def insert_data(df):
    table_name = 'all_season_bowling_card'
    db_connection_url = 'postgresql://' + config.db.user + ':'+config.db.password +'@' +\
                           config.db.host + '/' + config.db.dbname
    engine = create_engine(db_connection_url)
    df.to_sql(table_name, engine, schema='public', if_exists='replace', index=False)

if __name__ == "__main__":
    file_path = '/Users/adarshbadjate/code/new/Data/all_season_bowling_card.csv'
    df = create_dataframe(file_path)
    create_table()
    insert_data(df)
