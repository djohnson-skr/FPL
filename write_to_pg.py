import psycopg2  # needed for sqlalchemy as well
from sqlalchemy import create_engine

def main(current_gameweek, league, league_entries, matches, standings, draft_order):
    # ----- Make connection to local postgres db ----- #
    db_conn_string= 'postgresql://postgres:admin@localhost:5432/FPL'
    engine = create_engine(db_conn_string)

    # ----- Get the gameweeks listed in the postgres standings table ----- #
    gameweeks = get_gameweeks_pg(db_conn_string, 'standings')

    # ----- Write to Postgres db ----- #
    league.to_sql('league', engine, if_exists='replace')
    league_entries.to_sql('league_entries', engine, if_exists='replace')
    matches.to_sql('matches', engine, if_exists='replace')
    if current_gameweek not in gameweeks:
      standings.to_sql('standings', engine, if_exists='append')
    draft_order.to_sql('draft_order', engine, if_exists='replace')


def get_gameweeks_pg(db_conn_string, table):
    connection = psycopg2.connect(db_conn_string)
    cursor = connection.cursor()
    postgreSQL_select_Query = f'select * from {table}'
    cursor.execute(postgreSQL_select_Query)
    standings_pg = cursor.fetchall()
    gameweeks_pg_raw = []
    for row in standings_pg:
        gameweeks_pg_raw.append(row[0])
    gameweeks_pg = [] 
    [gameweeks_pg.append(x) for x in gameweeks_pg_raw if x not in gameweeks_pg]
    
    return gameweeks_pg

