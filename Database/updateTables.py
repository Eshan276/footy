import sqlite3
import pandas as pd
import ast
import sys
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
            # TODO add players of these teams to the db
            # df = df.loc[df.id.isin(new_teams_df.id.unique())]
            # print(df)
            # df = add_all_historical_info_selected_teams(df) # update data table
            # print(df)
            # insert the new names into meta_club_table
            for index, row in new_teams_df.iterrows():
                sql = f"""INSERT INTO meta_club_table ({row['team_name']}, {row['id']}, {row['league_id']})"""
                self.cur.execute(sql)
        # insert the df as a new table into our database
        # unique_df.to_sql(name="meta_club_table", con=con, if_exists="replace", index=False)
        self.con.commit()
        self.cur.close()
        self.con.close()

    def update_data_clubs_table(self, df=None):
        old_df = pd.read_sql_query(f"select * from data_club_table", self.con)
        '''
        meta_teams_df = pd.read_sql_query(f"select * from meta_club_table", con)
        # select teams that are present in the meta df but not in the data df
        ids = list(set(meta_teams_df.id.unique())-set(old_df.id.unique()))
        print(ids)'''
        df = pd.concat([old_df, df], axis=0)
        df = add_all_historical_info_selected_teams(df)
        df["year"] = df["year"].apply(lambda x: int(x))
        print(old_df)
        new_rows_df = df[~df.isin(old_df)].dropna(how = 'all') 
        print(new_rows_df)
        if len(new_rows_df)>0:
            new_rows_df["year"] = new_rows_df["year"].astype(int)
            # new_rows_df.to_sql(name="data_club_table", con=self.con, if_exists="append", index=False)
            self.con.commit()
        else:
            print("No new rows to add to data_club_table")
if __name__ == "__main__":
    config = Configuration()
    uT = updateTables()
    uT.update_meta_clubs_table(config)
    # uT.update_data_clubs_table()
    # con =  sqlite3.connect("Database/database.db")

    # old_df = pd.read_sql_query(f"select * from data_club_table", con)
    # old_df = old_df.drop_duplicates()
    # old_df.to_sql(name="data_club_table", con=con, if_exists="replace", index=False)
    # con.commit()
    # con.close()
