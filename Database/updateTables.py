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
    def update_meta_clubs_table(config, start_year=[2023]):
    
        con = sqlite3.connect("Database/database.db")
        cur = con.cursor()
        # load in the current df
        old_df = pd.read_sql_query(f"select * from meta_club_table", con)
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
        unique_df = unique_df[~unique_df.id.isin(old_df.id)].dropna(how = 'all')
        print(unique_df)
        # TODO define a new_varaible or so in 

        # TODO add players of these teams to the db
        df = df.loc[df.id.isin(unique_df.id.unique())]
        print(df)
        df = add_all_historical_info_selected_teams(unique_df)
        print(df)
        # insert the new names Heidenheim and Lutontown into db
        for index, row in unique_df.iterrows():
            sql = f"""INSERT INTO meta_club_table ({row['team_name']}, {row['id']}, {row['league_id']})"""
            # cur.execute(sql)
        # insert the df as a new table into our database
        # unique_df.to_sql(name="meta_club_table", con=con, if_exists="replace", index=False)
        con.commit()
        cur.close()
        con.close()

if __name__ == "__main__":
    config = Configuration()
    updateTables.update_meta_clubs_table(config)