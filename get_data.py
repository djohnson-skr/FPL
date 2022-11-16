import json
from pprint import pprint
import gspread
import pandas as pd
import psycopg2  # needed for sqlalchemy as well
import requests
from pandas import json_normalize
from sqlalchemy import create_engine

db_conn_string= 'postgresql://postgres:admin@localhost:5432/FPL'

# ----- Make connection to local postgres db ----- #
engine = create_engine(db_conn_string)

# ----- Get gameweeks listed in FPL.standings table ----- #
# used so I don't append gameweeks into standings if it already exists
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

gameweeks_pg = get_gameweeks_pg(db_conn_string, 'standings')

# ----- Get league_data ----- #
r_league_data = requests.get('https://draft.premierleague.com/api/league/51403/details')
r_draft_data = requests.get('https://draft.premierleague.com/api/draft/51403/choices')
r_current = requests.get('https://draft.premierleague.com/api/game')

# clean up if statement
if r_league_data.status_code == 200 and r_draft_data.status_code == 200:
    league_data = r_league_data.json()
    draft_data = r_draft_data.json()
    current = r_current.json()
else:
    print('There was an issue with the get request')
    exit()

# ----- Global Vars ----- #
league_id = league_data['league']['id']
league_name = league_data['league']['name']
total_players = len(league_data['league_entries'])
current_gameweek = current['current_event']

# ----- Create 'player_id':'player_name'dict ----- #
player_id_name = {}
for i in range(0, len(league_data['league_entries'])):
    player_id_name[league_data['league_entries'][i]['id']] = league_data['league_entries'][i]['entry_name']

# ----- Datasets () ----- #

# LEAGUE 
league = json_normalize(league_data['league'])
league_pg = league.set_index('admin_entry')
#print(league)

# LEAGUE ENTRIES/PLAYERS
league_entries = json_normalize(league_data['league_entries'])
league_entries = league_entries.rename(columns={'id': 'player_id'}) \
                               .drop(columns=['waiver_pick']) 
league_entries_pg = league_entries.set_index('entry_id')                               
#print(league_entries)

# MATCHES
matches = json_normalize(league_data['matches'])
matches = matches.rename(columns={'league_entry_1':'player1', 'league_entry_1_points':'player1_score',
                                    'league_entry_2':'player2', 'league_entry_2_points':'player2_score'})
# replace the player_id (id) with the player name
matches = matches.replace({'player1': player_id_name, 'player2': player_id_name}) \
                 .drop(columns=['winning_league_entry','winning_method']) 
matches_pg = matches.set_index('event')                 
#print(matches)

# CURRENT STANDINGS
standings = json_normalize(league_data["standings"])
standings = standings.replace({'league_entry' : player_id_name}) \
                     .drop(columns=['matches_played', 'rank_sort'])
standings['gameweek'] = current_gameweek                    
standings = standings[['gameweek','rank','league_entry', 'matches_won', 'matches_drawn', 'matches_lost', \
                        'total', 'points_against', 'points_for', 'last_rank']]
# changing the dataframe index means I don't get an index column in
standings_for_pg = standings.set_index('gameweek')

# DRAFT ORDER
draft_order_dict = {}
for i in range(0, total_players):
    draft_order_dict[draft_data['choices'][i]['entry_name']] = draft_data['choices'][i]['pick']

draft_order = pd.DataFrame(draft_order_dict.items(), columns=['player_name', 'pick'])
draft_order_pg = draft_order.set_index('player_name')
#print(draft_order)

# ----- Write to Postgres ---- $
# tables will be auto created as needed with inferred data types
""" league_pg.to_sql('league', engine, if_exists='replace')
league_entries_pg.to_sql('league_entries', engine, if_exists='replace')
matches_pg.to_sql('matches', engine, if_exists='replace')
if current_gameweek not in gameweeks_pg:
    standings_for_pg.to_sql('standings', engine, if_exists='append')
draft_order_pg.to_sql('draft_order', engine, if_exists='replace') """

# ----- Write to CSV file ----- #
""" league.to_csv('league.csv', sep='\t')
standings.to_csv('standings.csv', sep='\t') """

# ----- Write to Google Sheets ----- #
# test this out to see if you do not need to pass the full path
service_acct_path = '/Users/ArminHammer/Documents/FPL/service_account.json'
gc = gspread.service_account(service_acct_path)
spreadsheet = gc.open('FPL 22/23')

# LEAGUE 
ws_league = spreadsheet.worksheet('league')
ws_league.update([league.columns.values.tolist()] + league.values.tolist()) # add the header to the first row + the values below

# LEAGUE ENTRIES/PLAYERS
ws_league_entires = spreadsheet.worksheet('league_entries')
ws_league_entires.update([league_entries.columns.values.tolist()] + league_entries.values.tolist())

# MATCHES
ws_matches = spreadsheet.worksheet('matches')
ws_matches.update([matches.columns.values.tolist()] + matches.values.tolist())

# CURRENT STANDINGS
ws_standings = spreadsheet.worksheet('standings')
gameweeks_gsheet_raw = ws_standings.col_values(1)
# if ws_standings has a blank column A then I can not pop(0) with any empty list
try:
    gameweeks_gsheet_raw.pop(0)
    gameweeks_gsheet = [] 
    [gameweeks_gsheet.append(x) for x in gameweeks_gsheet_raw if x not in gameweeks_gsheet]
    gameweeks_gsheet = [int(x) for x in gameweeks_gsheet]
except IndexError:
    pass
    
if ws_standings.acell('A1').value == 'gameweek' and current_gameweek not in gameweeks_gsheet:
    ws_standings.append_rows(standings.values.tolist())
else:
    ws_standings.update([standings.columns.values.tolist()] + standings.values.tolist())

# DRAFT ORDER
ws_draft_order = spreadsheet.worksheet('draft_order')
ws_draft_order.update([draft_order.columns.values.tolist()] + draft_order.values.tolist())