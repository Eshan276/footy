from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sys
import os
from tqdm import tqdm

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from Database.database import Database
from Configuration import Configuration
import datetime

def get_teams(league_url, years, league_id):
    '''Scrape team information for certain leagues and given years.'''
    df = pd.DataFrame()
    for year in years:
        url = league_url + f"/plus/?saison_id={year}"

        # exception handling
        r = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        html = urlopen(r)
        bs = BeautifulSoup(html, 'html.parser')
        
        # Get the team names
        team_rows = bs.find('table', {'class': 'items'}).find_all('td', {"class":"hauptlink no-border-links"})
        
        teams = {}
        for row in team_rows:
            team_name = row.text.strip().split(' \\')[0]
            team_href = row.find('a')['href']
            team_id = team_href.split('/')[4]
            teams[team_name]={'href': team_href, 'id': team_id}
        # TODO maybe also add the market value of the team
        # turn into df
        teams_df = pd.DataFrame.from_dict(teams, orient="index").reset_index(drop=False, names="team_name")
        teams_df["year"] = year
        teams_df["league_id"] = league_id
        df = pd.concat([df, teams_df], axis=0)
    return df

def get_teams_all_leagues(config, start_year): 
    league_urls = config.league_ids
    years = [year for year in range(start_year,2023)]
    combined_df = pd.DataFrame()
    for league_url in league_urls.keys():
        print(f"Starting {list(league_urls.keys()).index(league_url)+1}/{len(list(league_urls.keys()))} - {datetime.datetime.now()}")
        df = get_teams(config.base_url + league_url, years, league_urls[league_url])
        combined_df = pd.concat([df, combined_df], axis=0)
        print(f"Finished {list(league_urls.keys()).index(league_url)+1}/{len(list(league_urls.keys()))} - {datetime.datetime.now()}")
    return combined_df

def add_all_historical_info_selected_teams(df):
    '''This function adds team information for previously selected teams. 
    So that teams that played in lower divisions are also accounted for.
    df: combined_df i.e. the output of get_teams_all_leagues'''

    # Extract existing ID-Year combinations
    existing_combinations = set(zip(df['id'], df['year']))

    # Create a list to store the updated table
    updated_rows = df.values.tolist()

    unique_ids = df.id.unique()
    unique_years = df.year.unique()

    # Iterate through all possible ID-Year-League combinations
    for id in unique_ids:  
        for year in unique_years:  
            if (id, year) not in existing_combinations:
                team_df = df.loc[df.id.isin([id]),:]
                league_id = team_df["league_id"].unique()[0]
                href = team_df["href"].unique()[0].split("/")[1]
                href = f"/{href}/startseite/verein/{id}/saison_id/{year}"
                team_name = team_df["team_name"].unique()[0]
                new_entry = [team_name, href, id, year, league_id]  # Provide the appropriate values
                updated_rows.append(new_entry)
    # Create a new DataFrame with the updated table
    updated_df = pd.DataFrame(updated_rows, columns=df.columns)
    return updated_df

def get_players(team_url):
    r = Request(team_url, headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(r)
    bs = BeautifulSoup(html, 'html.parser')
    # Get the team names
    try:
        player_rows = bs.find('table', {'class': 'items'}).find_all('span', {"class":"hide-for-small"})# problem with index 2449
        players = {}
        for row in player_rows:
            if "wechsel-kader-wappen" in row["class"]:# skip entries not linked to players
                continue
            player_name = row.text
            player_href = row.find("a")["href"]
            player_id = player_href.split("/")[-1]
            players[player_id] = {"player_href": player_href, "players":player_name}

        player_dates, player_numbers = [], []
        team_rows = bs.find('table', {'class': 'items'}).find_all('td', {"class":"zentriert"})
        for row in team_rows:
            if row.get_text()== '':
                continue
            elif len(row.get_text())>=3:
                player_dates.append(row.get_text())
            else:
                player_numbers.append(row.get_text())

        # not ideal but add the dates and numbers based on their index position
        if len(players.keys()) == len(player_dates) & len(players.keys()) == len(player_numbers):
            for player_id in players.keys():
                players[player_id]["Birthday"] = player_dates[list(players.keys()).index(player_id)]
                players[player_id]["Number"] = player_numbers[list(players.keys()).index(player_id)]
        else:
            print("Not matching dates and/or numbers")
        # Create a DataFrame from the dictionary
        player_df = pd.DataFrame.from_dict(players, orient='index').reset_index(drop=False, names="player_id")
    except AttributeError:
        print(f"No data for  {team_url}")
        player_df = pd.DataFrame()
    return player_df

def get_player_info(url):
    # retrieves information from a players web site
    r = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(r)
    bs = BeautifulSoup(html, 'html.parser')
    
    player_info = {}
    player_info["player_id"] = url.split("/")[-1]

    # Get the team names
    hrefs, transfer_years, club_ids = [], [], []
    grid = bs.find_all("div", {"class":"tm-player-transfer-history-grid"})
    for entry in grid:
        old_club = entry.find("div", {"class":"tm-player-transfer-history-grid__old-club"})
        if None == old_club: # handle None matches
            continue
        else:
            if "grid__heading" in old_club["class"]:# exclude the header
                continue
            try:
                href = old_club.find("a", {"class":"tm-player-transfer-history-grid__club-link"})["href"]
            except TypeError as e:
                href = old_club.find("a")["href"]
            transfer_year = href.split("/")[-1]
            club_id = href.split("/")[-3]
            hrefs.append(href)
            transfer_years.append(transfer_year)
            club_ids.append(club_id)
    player_info["transfer_hrefs"] = hrefs
    player_info["transfer_years"] = transfer_years

    # current club id
    current_club_id = []
    for entry in grid:
        old_club = entry.find("div", {"class":"tm-player-transfer-history-grid__new-club"})
        if None == old_club: # handle None matches
            continue
        else:
            if "grid__heading" in old_club["class"]:# exclude the header
                continue
            href = old_club.find("a")["href"]
            club_id = href.split("/")[-3]
            current_club_id.append(club_id)
            break # stop after the first found element
    player_info["current_club"] = current_club_id
    player_info["transfer_club_ids"] = club_ids

    # market value
    current_mv = bs.find_all("div", {"class":"tm-player-market-value-development__current-value"})
    for entry in current_mv:
        player_current_mv = entry.get_text()
        player_info["current_mv"] = player_current_mv

    # max market value
    max_mv = bs.find_all("div", {"class":"tm-player-market-value-development__max-value"})
    for entry in max_mv:
        player_max_mv = entry.get_text()
        player_info["max_mv"]= player_max_mv
    
    # position
    player_positions = []
    positions = bs.find_all("dd", {"class":"detail-position__position"})
    for position in positions:
        player_positions.append(position.get_text())
    if len(player_positions)>0:
        player_main_position = [player_positions[0]]
        player_info["main_position"] = player_main_position
    if len(player_positions)>1: 
        player_other_positions = player_positions[1:]
        player_info["other_positions"] = player_other_positions

    # nationality
    player = bs.find('div', {'class': 'info-table'})
    nations = player.find_all("img", {"class": "flaggenrahmen"})
    player_nations = []
    for nation in nations:
        if "lazy" in nation["class"]:
            continue
        player_nations.append(nation["title"])
    player_info["nationality"] = player_nations

    player_info = {key: [value] for key, value in player_info.items()}
    player_info_df = pd.DataFrame(player_info)
    return player_info_df

def load_players_info_for_team(team_url, base_url):
    # combine the information derived from get_players and get_player_info
    team_id = team_url.split('/')[6]
    df = get_players(team_url)
    # stop here and then continue with additional info
    additional_df = pd.DataFrame()
    for href in df["player_href"]:#tqdm(df['player_href'], total=len(df)):#df.player_href:
        # print(f"Starting {list(df.player_href).index(href)+1}/{len(list(df.player_href))} - {datetime.datetime.now()}")
        href = base_url + href
        player_info_df = get_player_info(href, team_id)
        additional_df = pd.concat([additional_df, player_info_df], axis=0)
    df = pd.merge(df, additional_df, on="player_id", how="left")
    return df

def get_players_for_all_teams(config, df):
    # get the players for all teams in the league
    players_df = pd.DataFrame()
    for index, row in tqdm(df.iterrows(),total=len(df)):
        # maybe dont include the additional info for players but only scrape the team site
        # player_df=load_players_info_for_team(config.base_url + row.href, config.base_url)
        player_df = get_players(config.base_url + row.href)
        players_df = pd.concat([players_df, player_df], axis=0)
    if "Unnamed: 0" in players_df.columns:
        players_df = players_df.drop("Unnamed: 0", axis=1)
    '''
    columns_with_list_type = ["transfer_years", "transfer_hrefs", "transfer_club_ids", "main_position", "other_positions", "nationality"]
    columns_with_list_type = [column for column in columns_with_list_type if column in players_df.columns]
    for column in columns_with_list_type:
        players_df[column] = players_df[column].apply(lambda x: str(x))
    subset = ["players", "player_href", "player_id"]
    '''
    players_df = players_df.drop_duplicates(subset = "player_id").reset_index(drop=True)
    return players_df

if __name__ == "__main__":
    # league_url = "https://www.transfermarkt.de/bundesliga/startseite/wettbewerb/L1"
    config = Configuration()
    df = get_player_info("https://www.transfermarkt.com/georgi-ivanov/profil/spieler/5678")
    print(df)
    # df = get_teams_all_leagues(config, start_year=2000) # pd.read_csv("Combined.csv")
    # print(df)
    #updated_df = add_all_historical_info_selected_teams(df)
    #print(updated_df)
    # unique_ids = df.id.unique()
    # print(unique_ids)
    # print([(id, df.loc[df.id.isin([id]), "team_name"].unique()) for id in unique_ids])
    #load_players_info_for_team(bremen, base_url, team_name="SVWB")

    # bremen = base_url + f"/sv-werder-bremen/startseite/verein/86/saison_id/{year}"

    '''    r = Request(bremen, headers={'User-Agent': 'Mozilla/5.0'})
    html = urlopen(r)
    bs = BeautifulSoup(html, 'html.parser')
    # Get the team names
    team_rows = bs.find('table', {'class': 'items'}).find_all('td', {"class":"zentriert"})
    for row in team_rows:
        if row.get_text()== '':
            continue
        elif len(row.get_text())>=3:
            player_date = row.get_text()
            print(player_date)
        else:
            player_number = row.get_text()
            print( player_number)'''
        

    #urls = ["/bundesliga/startseite/wettbewerb/L1", "/premier-league/startseite/wettbewerb/GB1",
      #      "/primera-division/startseite/wettbewerb/ES1", "/serie-a/startseite/wettbewerb/IT1",
       #     "/ligue-1/startseite/wettbewerb/FR1"]
    #ducksch = base_url + "/marvin-ducksch/profil/spieler/125103"
    #senninger = base_url + "/fabian-senninger/profil/spieler/334490"

    #for url in [senninger, ducksch]:
     #   r = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
      #  html = urlopen(r)
       # bs = BeautifulSoup(html, 'html.parser')
    
    # urls = [ base_url + url for url in urls]
    # for url in urls:
        # teams = get_teams(url)
        # print(teams)