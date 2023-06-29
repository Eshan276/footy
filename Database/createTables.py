import sqlite3
import pandas as pd
import numpy as np
import sys
import os
import ast
import requests
from tqdm import tqdm
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from urllib.error import HTTPError
from http.client import IncompleteRead
# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from itertools import combinations
from Database.database import Database
from WebScraping.scrapeFootyData import get_teams_all_leagues, add_all_historical_info_selected_teams, get_players_for_all_teams, get_player_info
from Configuration import Configuration
import datetime

class createTables(): # alternatively fillTables
    def meta_clubs_table(config, df, start_year=2000):
        
        
        # TODO add an check to add only new data that is different from the aready prominent data 

        # if df == None: # if no combined_df is given, then scrape the data
        #     df = get_teams_all_leagues(config, start_year=start_year)
        # determine the unique clubs within the df
        unique_df = df[["team_name", "id", "league_id"]].drop_duplicates().reset_index(drop=True)

        # check for different team_names that are stored under the same id
        if unique_df.nunique()["team_name"] != unique_df.nunique()["id"]:
           #print(unique_df.nunique())
           duplicate_list = [id for id in unique_df.id.unique() if unique_df[unique_df.id.isin([id])].nunique()["team_name"]>1]
           for id in duplicate_list:
               names = unique_df.loc[unique_df.id.isin([id]), "team_name"].unique()[-1]
               unique_df.loc[unique_df.id.isin([id]), "team_name"] = names
        # lastly remove the new duplicates
        unique_df = unique_df[["team_name", "id", "league_id"]].drop_duplicates().reset_index(drop=True)

        con = sqlite3.connect("Database/database.db")
        cur = con.cursor()
        # insert the df as a new table into our database
        unique_df.to_sql(name="meta_club_table", con=con, if_exists="replace", index=False)
        con.commit()
        cur.close()
        con.close()
    
    def data_clubs_table(config, df):
        # fill the datatable with data for each club in the meta table regrdless of their playing league (e.g. 1st or 2nd division)  

        # TODO add an check to add only new data that is different from the aready prominent data 
        # by loading in the prominent table and removing elements that are already in the to be inserted table


        # find a way to add to table instead of overhauling the entire table.... -> maybe append in combination with remove duplicates?

        # if df == None: # if no df is given, then scrape the data
        #     df = get_teams_all_leagues(config, 2000)

        # TODO Before insertion, check that the team_names are all equal to the meta_table -> maybe compare with meta table or so

        con = sqlite3.connect("Database/database.db")
        cur = con.cursor()
        # insert the df as a new table into our database
        df.to_sql(name="data_club_table", con=con, if_exists="replace", index=False)
        con.commit()
        cur.close()
        con.close()

    def data_player_table(config):
        con = sqlite3.connect("Database/database.db")
        cur = con.cursor()
        # query the data from all available teams
        teams_df = pd.read_sql_query(f"select * from data_club_table", con)
        # determine the unique player data for every team in teams_df
        max_retries = 5
        retry_count = 0
        # todo it could be more efficient to first determine the unique players and then scrape the data for them
        while retry_count < max_retries:
            try:
                df = get_players_for_all_teams(config, teams_df)
                break
            except (ChunkedEncodingError, ConnectionError, ReadTimeout, ValueError, IncompleteRead, HTTPError) as e:
                retry_count += 1
                print(f"Retry {retry_count}/{max_retries} - Error: {e}")
            
            except requests.exceptions.RequestException as e:
                print(f"Error: {e}")
                break  # Break the loop if an unrecoverable error occurs
        df.to_sql(name="data_players_table", con=con, if_exists="replace", index=False)
        con.commit()
        cur.close()
        con.close()
    
    def add_player_info_to_data_players_table(config):

        con = sqlite3.connect("Database/database.db")
        df = pd.read_sql_query(f"select * from data_players_table", con) 
        additional_df = pd.DataFrame()
        for url in tqdm(df.iloc[10000:20000,:].player_href, total=len(df.iloc[10000:20000,:])):
            additional_df = pd.concat([additional_df, get_player_info(config.base_url + url)], axis=0)
        df = pd.merge(df, additional_df, on="player_id", how="left")
        # failed insertion... all types need to be formatted first...
        df.to_sql(name="data_player_table", con=con, if_exists="append", index=False)
        con.commit()
        con.close()



    def get_data_player_data():
        con = sqlite3.connect("Database/database.db")
        df = pd.read_sql_query(f"select * from data_player_table", con)
        print(df.head())
        columns_with_list_type = ["transfer_years", "transfer_hrefs", "transfer_club_ids", "main_position", "other_positions", "nationality"]
        for col in columns_with_list_type:
            #if col == "other_positions": # TODO maybe check more genereally for all columns that contain nan
            df.loc[df[col]=="nan",col] = str([])
            df[col] = df[col].apply(ast.literal_eval)
        con.close()
        return df

    def tic_tac_toe_logic(meta_teams_df, df):
        # -> try out for the players in db
        # then check which players played for both clubs and save their player_ids as list of length 2 into a /dict
        # disregard combinations with no players
        df["club_ids"] = df["transfer_club_ids"] # df["current_club"] + df["transfer_club_ids"]
        club_combinations = {}
        club_ids = meta_teams_df.id.unique()[:5]
        for club_id_1, club_id_2 in combinations(club_ids, 2):
                # first iterate through all possible club_id combinations -> handling for retired club_id!
                if (club_id_1 != club_id_2) & (club_id_1 != config.club_id_retired) & (club_id_2 != config.club_id_retired):
                    club_1_mask = df.club_ids.apply(lambda x: club_id_1 in x)
                    club_2_mask = df.club_ids.apply(lambda x: club_id_2 in x)
                    player_ids = df.loc[club_1_mask & club_2_mask, 'player_id'].tolist()
                    if len(player_ids) > 0:
                        club_combinations[(club_id_1, club_id_2)] = player_ids
        # insert this as a df into the db
        return club_combinations

if __name__ == "__main__":
    config = Configuration()
    #df = get_teams_all_leagues(config, start_year=2000) # pd.read_csv("Combined.csv") or select with db connection, but for insertion this step is probably not needed
    #df = add_all_historical_info_selected_teams(df)
    # createTables.data_player_table(config)

    createTables.add_player_info_to_data_players_table(config)

    # con = sqlite3.connect("Database/database.db")
    # teams_df = pd.read_sql_query(f"select * from data_club_table", con)
    # selection = [1299, 1327, 1692, 2298, 3014, 3828, 4061, 4665, 4690, 4743, 4752, 4786, 4834]
    # print(teams_df.iloc[selection, :])
    # teams_df = teams_df.iloc[selection, :]
    # df = get_players_for_all_teams(config, teams_df)

    # meta_teams_df = pd.read_sql_query(f"select * from meta_club_table", sqlite3.connect("Database/database.db"))
    # df = createTables.get_data_player_data()
    # print(createTables.tic_tac_toe_logic(meta_teams_df, df))
    # print(df.transfer_club_ids[0])
    # print(np.unique(df.transfer_club_ids[0]))
    # print(df.loc[df.other_positions=="nan",:])
    #createTables.meta_clubs_table(config, df)
    #createTables.data_clubs_table(config, df)
