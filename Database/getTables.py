import sqlite3
import pandas as pd
import ast
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from Configuration import Configuration


class getTables:
    def __init__(self):
        self.con = sqlite3.connect("Database/database.db")
        self.cur = self.con.cursor()

    def get_player_data(self):
        df = pd.read_sql_query(f"select * from data_player_table", self.con)
        columns_with_list_type = ["transfer_years", "transfer_hrefs", "current_club", "transfer_club_ids", "main_position", "other_positions", "nationality"]
        for col in columns_with_list_type:
            #if col == "other_positions": # TODO maybe check more genereally for all columns that contain nan
            df.loc[df[col]=="nan",col] = str([])
            df[col] = df[col].apply(ast.literal_eval)
        self.con.close()
        return df
    
    def select_base(self, team_ids=None, league_ids=None):

        if team_ids != None: 
            sql = """SELECT *
                FROM tic_tac_toe_combinations
                WHERE
                    (
                        -- Check if any element in Axis 1 exactly matches the desired elements
                        {axis1_condition} AND
                        -- Check if any element in Axis 2 exactly matches the desired elements
                        {axis2_condition}
                    )"""
            
            axis1_conditions = " OR ".join([f'"Axis 1" LIKE \'% {element},%\' OR "Axis 1" LIKE \'% {element}]%\' OR "Axis 1" LIKE \'[{element},%\' OR "Axis 1" LIKE \'[{element}]%\' OR "Axis 1" LIKE \'% {element} ]\' OR "Axis 1" LIKE \'% {element} ,\' OR "Axis 1" = \'{element}\''
                                        for element in team_ids])
            axis2_conditions = " OR ".join([f'"Axis 2" LIKE \'% {element},%\' OR "Axis 2" LIKE \'% {element}]%\' OR "Axis 2" LIKE \'[{element},%\' OR "Axis 2" LIKE \'[{element}]%\' OR "Axis 2" LIKE \'% {element} ]\' OR "Axis 2" LIKE \'% {element} ,\' OR "Axis 2" = \'{element}\''
                                        for element in team_ids])
            
            formatted_sql = sql.format(axis1_condition=axis1_conditions, axis2_condition=axis2_conditions)
        if league_ids != None: # make exclusive leagues and possible intersection only between 2 leagues? -> possible via team_ids
            sql = """SELECT *
                    FROM tic_tac_toe_combinations
                    WHERE "League ID" IN ({league_ids})"""
            
            league_ids_condition = ", ".join(str(league_id) for league_id in league_ids)
            formatted_sql = sql.format(league_ids=league_ids_condition)
        df = pd.read_sql_query(formatted_sql, self.con)
        self.con.close()
        return df
    
    def get_combination_results(self, team_id_1, team_id_2):
        self.con = sqlite3.connect("Database/database.db")
        df = pd.read_sql_query(f'select * from data_tic_tac_toe_table where ("Club 1" in ({team_id_1}) and "Club 2" in ({team_id_2})) or ("Club 1" in ({team_id_2}) and "Club 2" in ({team_id_1}))', self.con)
        df["Player IDs"] = df["Player IDs"].apply(ast.literal_eval)
        self.con.close()
        return df

if __name__ == "__main__":
    t = getTables()
    config = Configuration()
    df = t.select_base(team_ids=config.top_teams, league_ids=None)
    print(df)
    '''df = t.get_combination_results(583, 1041)
    print(df["Player IDs"][0])
    if str(3964) in df["Player IDs"][0]:
        print(True)'''