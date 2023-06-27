import sqlite3
import pandas as pd
import numpy as np
import sys
import os

# Add the project root directory to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from Database.database import Database
from WebScraping.scrapeFootyData import get_teams_all_leagues, add_all_historical_info_selected_teams
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

    # next implement the same structure for the player data
    def meta_players_table():
        pass
    def data_players_table():
        pass

if __name__ == "__main__":
    config = Configuration()
    df = get_teams_all_leagues(config, start_year=2000) # pd.read_csv("Combined.csv") or select with db connection, but for insertion this step is probably not needed
    df = add_all_historical_info_selected_teams(df)
    createTables.meta_clubs_table(config, df)
    createTables.data_clubs_table(config, df)
