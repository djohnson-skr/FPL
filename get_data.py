import pandas as pd
import requests
from pandas import json_normalize
import write_to_pg
import write_to_gsheets
import write_to_snowflake

def main():
    # ----- Grab the data ----- #
    league_data = get_data('https://draft.premierleague.com/api/league/51403/details')
    draft_data = get_data('https://draft.premierleague.com/api/draft/51403/choices')
    current_gameweek_data = get_data('https://draft.premierleague.com/api/game')

    # ----- Global Vars ----- #
    league_id = league_data['league']['id']
    league_name = league_data['league']['name']
    total_players = len(league_data['league_entries'])
    current_gameweek = current_gameweek_data['current_event']

    # ----- Create 'player_id':'player_name'dict ----- #
    # used to shed more light on to 
    player_id_name = {}
    for i in range(0, len(league_data['league_entries'])):
        player_id_name[league_data['league_entries'][i]['id']] = league_data['league_entries'][i]['entry_name']

    # ----- Datasets ----- #

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

    # STANDINGS
    standings = json_normalize(league_data["standings"])
    standings = standings.replace({'league_entry' : player_id_name}) \
                        .drop(columns=['matches_played', 'rank_sort'])
    standings['gameweek'] = current_gameweek                    
    standings = standings[['gameweek','rank','league_entry', 'matches_won', 'matches_drawn', 'matches_lost', \
                            'total', 'points_against', 'points_for', 'last_rank']]
    # changing the dataframe index means I don't get an index column in
    standings_pg = standings.set_index('gameweek')

    # DRAFT ORDER
    draft_order_dict = {}
    for i in range(0, total_players):
        draft_order_dict[draft_data['choices'][i]['entry_name']] = draft_data['choices'][i]['pick']

    draft_order = pd.DataFrame(draft_order_dict.items(), columns=['player_name', 'pick'])
    draft_order_pg = draft_order.set_index('player_name')
    #print(draft_order)

    #write_to_pg.main(current_gameweek, league_pg, league_entries_pg, matches_pg, standings_pg, draft_order_pg)
    #write_to_gsheets.main(current_gameweek, league, league_entries, matches, standings, draft_order)
    write_to_snowflake.main(league, league_entries, matches, standings, draft_order)

def get_data(url):
    r = requests.get(url)
    if r.status_code == 200:
        data = r.json()
    else:
        print('There was an issue with a get request')
        exit()
    
    return data

if __name__=="__main__":
    main()