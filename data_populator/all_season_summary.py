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
    df.rename(columns={'1st_inning_score': 'first_inning_score','2nd_inning_score': 'second_inning_score'}, inplace=True)
    df['season'] = df['season'].fillna(0).astype(int)
    return df

def create_table():
    sql = '''
        CREATE TABLE IF NOT EXISTS all_season_summary(
        season INTEGER,
        id INTEGER,
        name VARCHAR,
        short_name VARCHAR,
        description VARCHAR,
        home_team VARCHAR,
        away_team VARCHAR,
        toss_won VARCHAR,
        decision VARCHAR,
        first_inning_score VARCHAR,
        second_inning_score VARCHAR,
        home_score VARCHAR,
        away_score VARCHAR,
        winner VARCHAR,
        result VARCHAR,
        start_date VARCHAR,
        end_date VARCHAR,
        venue_id INTEGER,
        venue_name VARCHAR,
        home_captain VARCHAR,
        away_captain VARCHAR,
        pom VARCHAR,
        points VARCHAR,
        super_over VARCHAR,
        home_overs FLOAT,
        home_runs FLOAT,
        home_wickets FLOAT,
        home_boundaries FLOAT,
        away_overs FLOAT,
        away_runs FLOAT,
        away_wickets FLOAT,
        away_boundaries FLOAT,
        highlights VARCHAR,
        home_key_batsman VARCHAR,
        home_key_bowler VARCHAR,
        home_playx1 VARCHAR,
        away_playx1 VARCHAR,
        away_key_batsman VARCHAR,
        away_key_bowler VARCHAR,
        match_days VARCHAR,
        umpire1 VARCHAR,
        umpire2 VARCHAR,
        tv_umpire VARCHAR,
        referee VARCHAR,
        reserve_umpire VARCHAR)'''
    trans_db.execute(sql)

def insert_data(df):
    table_name = 'all_season_summary'
    db_connection_url = 'postgresql://' + config.db.user + ':'+config.db.password +'@' +\
                           config.db.host + '/' + config.db.dbname
    engine = create_engine(db_connection_url)
    df.to_sql(table_name, engine, schema='public', if_exists='append', index=False)

if __name__ == '__main__':
    file_path = 'Data/all_season_summary.csv'
    df = create_dataframe(file_path)
    create_table()
    insert_data(df)


