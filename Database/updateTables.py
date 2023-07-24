import sqlite3
import pandas as pd
from tqdm import tqdm
import sys
import re
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from Configuration import Configuration
from WebScraping.scrapeFootyData import get_teams_all_leagues, add_all_historical_info_selected_teams, get_players_for_all_teams, get_player_info


class updateTables:

    def __init__(self):
        self.con = sqlite3.connect("Database/database.db")
        self.cur = self.con.cursor()


    def update_meta_clubs_table(self, config, start_year=[2023]):
        # load in the current df
        old_df = pd.read_sql_query(f"select * from meta_club_table", self.con)
        # then get the new df
        df = get_teams_all_leagues(config, years=start_year)
        unique_df = df[["team_name", "id", "league_id"]].drop_duplicates().reset_index(drop=True)

        # check for different team_names that are stored under the same id
        if unique_df.nunique()["team_name"] != unique_df.nunique()["id"]:
           duplicate_list = [id for id in unique_df.id.unique() if unique_df[unique_df.id.isin([id])].nunique()["team_name"]>1]
           for id in duplicate_list:
               names = unique_df.loc[unique_df.id.isin([id]), "team_name"].unique()[-1]
               unique_df.loc[unique_df.id.isin([id]), "team_name"] = names
        # lastly remove the new duplicates
        unique_df = unique_df[["team_name", "id", "league_id"]].drop_duplicates().reset_index(drop=True)
        # filter out rows that are not in the db yet
        new_teams_df = unique_df[~unique_df.id.isin(old_df.id)].dropna(how = 'all')
        self.update_data_clubs_table(df=df)
        # handle completely new teams
        if len(new_teams_df)>0:
            # insert the new names into meta_club_table
            for index, row in new_teams_df.iterrows():
                sql = "INSERT INTO meta_club_table (team_name, id, league_id) VALUES (?, ?, ?)"
                values = (row['team_name'], row['id'], row['league_id'])
                # self.cur.execute(sql, values)
        else:
            print("----- No new teams to add to meta_club_table -----")
        # insert the df as a new table into our database
        # unique_df.to_sql(name="meta_club_table", con=con, if_exists="replace", index=False)
        self.con.commit()
        self.cur.close()
        self.con.close()

    def update_data_clubs_table(self, df=None):
        old_df = pd.read_sql_query(f"select * from data_club_table", self.con)
        df = pd.concat([old_df, df], axis=0)
        df = add_all_historical_info_selected_teams(df)
        df["year"] = df["year"].apply(lambda x: int(x))
        new_rows_df = df[~df.apply(lambda x: (int(x['id']), int(x['year'])) in zip(old_df["id"].astype(int), old_df["year"].astype(int)), axis=1)]
        if len(new_rows_df)>0:
            new_rows_df["year"] = new_rows_df["year"].astype(int)
            new_rows_df.to_sql(name="data_club_table", con=self.con, if_exists="append", index=False)
            self.con.commit()
            return new_rows_df
        else:
            print("----- No new rows to add to data_club_table -----")

    
    def update_data_player_table(self, config, new_rows_df=None):
        # 1. detect new players -> and add them to the db with added info
        # 2. then go through each player in db and add new info if present
        old_df = pd.read_sql_query(f"select * from data_player_table", self.con)
        old_ids = old_df["player_id"].tolist()
        print("Fetching the player data for each team.")
        df = get_players_for_all_teams(config, new_rows_df.iloc[:1000,:])
        new_ids = df["player_id"].tolist()
        new_ids = [id for id in new_ids if id not in old_ids]
        new_players_df = df.loc[df["player_id"].isin(new_ids),:]
        added_info_df = pd.DataFrame()
        print("Fetching the individual player data from each player's website.")
        for index, row in tqdm(new_players_df.iterrows(), total=len(new_players_df)):
            new_row = get_player_info(f"{config.base_url}{row['player_href']}")
            added_info_df = pd.concat([added_info_df, new_row], axis=0)
        added_info_df = added_info_df.reset_index(drop=True)
        new_players_df = pd.merge(new_players_df, added_info_df, on="player_id", how="left")
        print(new_players_df)
        # add the new player to the db
        # new_players_df.to_sql(name="data_player_table", con=self.con, if_exists="append", index=False)
        # self.con.commit()
        # 2. then go through each player in db and add new info if present
        print("Updating all players in the current database.")
        
        # Convert the 'birthday' column to datetime type
        old_df['Birthday'] = old_df['Birthday'].astype(str)
        old_df["birthday_date"] = old_df["Birthday"].apply(lambda x: re.search(r'\w+ \d+, \d{4}', x).group(0) if re.search(r'\w+ \d+, \d{4}', x) else None)
        old_df['birthday_date'] = pd.to_datetime(old_df['birthday_date'], format='%b %d, %Y')

        # Access the year from the datetime objects and store it in a new column 'year'
        old_df['year'] = old_df['birthday_date'].dt.year
        # filter the df to exclude players that are too old (i.e. 40)
        filtered_df = old_df.loc[old_df["year"]>=1983,:]
        filtered_df = filtered_df.loc[filtered_df["player_id"].isin(["503482", "125103"])]
        print(filtered_df)
        columns_to_check = ["player_id", "transfer_hrefs", "transfer_years", "current_club", "transfer_club_ids", "current_mv", "max_mv"]
        # If you want to update the old_df with the updated rows, you can do the following:
        for index, row in tqdm(filtered_df.iloc[:,:].iterrows(), total=len(filtered_df.iloc[:,:])):
            new_row = get_player_info(f"{config.base_url}{row['player_href']}")
            boolean_mask = [(new_row[col].astype(str) == row[col])[0] for col in columns_to_check]
            if False in boolean_mask:
                print("----- Updating player -----")
                print(new_row)
                print(row)
                # 1. create the new row
                # 2. update the row
            # added_info_df = pd.concat([added_info_df, new_row], axis=0)

        

if __name__ == "__main__":
    config = Configuration()
    uT = updateTables()
    new_rows_df = pd.read_sql_query(f"select * from data_club_table", uT.con)
    new_rows_df = new_rows_df.loc[(new_rows_df["year"]==2023)|(new_rows_df["id"].isin(["2036", "1031"])),:]
    # print(new_rows_df.iloc[[27,28, 36, 45, 74],:])
    uT.update_data_player_table(config, new_rows_df=new_rows_df.iloc[:5,:])
    # WORKFLOW TO UPDATE CLUB TABLES

    # config = Configuration()
    # uT = updateTables()
    # uT.update_meta_clubs_table(config, start_year=[2022, 2023])
    