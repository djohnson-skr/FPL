import gspread

def main(current_gameweek, league, league_entries, matches, standings, draft_order):
    # ----- Connect to the spreadsheet ----- #
    service_acct_path = '/Users/ArminHammer/Documents/FPL/service_account.json'
    gc = gspread.service_account(service_acct_path)
    spreadsheet_name = 'FPL 22/23'
    spreadsheet = gc.open(spreadsheet_name)

    # ------ Write to the spreadsheet ----- #
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
    ws_current_standings = spreadsheet.worksheet('current_standings')
    ws_current_standings.update([standings.columns.values.tolist()] + standings.values.tolist())

    # STANDINGS
    ws_standings = spreadsheet.worksheet('standings')
    gameweeks_gsheet_raw = ws_standings.col_values(1) # read all the values of column A in gsheet
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
    if ws_standings.acell('A1').value != 'gameweek':
        ws_standings.update([standings.columns.values.tolist()] + standings.values.tolist()) # update/create the sheet if it is blank

    # DRAFT ORDER
    ws_draft_order = spreadsheet.worksheet('draft_order')
    ws_draft_order.update([draft_order.columns.values.tolist()] + draft_order.values.tolist())