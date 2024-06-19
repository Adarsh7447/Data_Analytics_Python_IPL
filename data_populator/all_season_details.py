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
    df = pd.read_csv(file_path,header=0,low_memory=False)
    df['season'] = df['season'].fillna(0).astype(int)
    return df

def create_table():
    sql = '''
        CREATE TABLE IF NOT EXISTS all_season_details(
        comment_id INTEGER,
        season INTEGER,
        match_id INTEGER,
        match_name VARCHAR,
        home_team VARCHAR,
        away_team VARCHAR,
        current_innings VARCHAR,
        innings_id INTEGER,
        over INTEGER,
        ball INTEGER,
        runs INTEGER,
        shortText VARCHAR,
        isBoundary BOOLEAN,
        isWide BOOLEAN,
        isNoball BOOLEAN,
        batsman1_id INTEGER,
        batsman1_name VARCHAR,
        batsman1_runs INTEGER,
        batsman1_balls INTEGER,
        bowler1_id INTEGER,
        bowler1_name VARCHAR,
        bowler1_overs DOUBLE PRECISION,
        bowler1_maidens VARCHAR,
        bowler1_runs INTEGER,
        bowler1_wkts INTEGER,
        batsman2_id VARCHAR,
        batsman2_name VARCHAR,
        batsman2_runs INTEGER,
        batsman2_balls INTEGER,
        bowler2_id FLOAT,
        bowler2_name VARCHAR,
        bowler2_overs FLOAT,
        bowler2_maidens FLOAT,
        bowler2_runs FLOAT,
        bowler2_wkts FLOAT,
        wicket_id FLOAT,
        wkt_batsman_name VARCHAR,
        wkt_bowler_name VARCHAR,
        wkt_batsman_runs FLOAT,
        wkt_batsman_balls FLOAT,
        wkt_text VARCHAR,
        isRetiredHurt BOOLEAN,
        text VARCHAR,
        preText VARCHAR,
        postText VARCHAR)'''
    trans_db.execute(sql)

def insert_data(df):
    table_name = 'all_season_details'
    db_connection_url = 'postgresql://' + config.db.user + ':'+config.db.password +'@' +\
                           config.db.host + '/' + config.db.dbname
    engine = create_engine(db_connection_url)
    df.to_sql(table_name, engine, schema='public', if_exists='replace', index=False)

if __name__ == '__main__':
    file_path = '/Users/adarshbadjate/code/new/Data/all_season_details.csv'
    df = create_dataframe(file_path)
    create_table()
    insert_data(df)

