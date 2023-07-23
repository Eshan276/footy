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
    
    def select_base_new(self, team_ids=None, league_ids=None, exclusive=True):
        if team_ids != None: 
            if exclusive:
                sql = f"""SELECT team_id, A1.team_1, A1.team_2, A1.team_3, A1.Axis_1_ID
                            FROM tic_tac_toe_axis_2 
                            INNER JOIN (
                                SELECT Axis_1_ID, team_1, team_2, team_3
                                FROM tic_tac_toe_axis_1 
                                WHERE team_1 IN {tuple(team_ids)}
                                AND team_2 IN {tuple(team_ids)}
                                AND team_3 IN {tuple(team_ids)}
                            ) AS A1 
                            ON tic_tac_toe_axis_2.Axis_1_ID = A1.Axis_1_ID
                            WHERE team_id IN {tuple(team_ids)}"""
            else:
                sql = f"""SELECT team_id, A1.team_1, A1.team_2, A1.team_3, A1.Axis_1_ID
                                FROM tic_tac_toe_axis_2 
                                INNER JOIN (
                                    SELECT Axis_1_ID, team_1, team_2, team_3
                                    FROM tic_tac_toe_axis_1 
                                    WHERE team_1 IN {tuple(team_ids)}
                                    OR team_2 IN {tuple(team_ids)}
                                    OR team_3 IN {tuple(team_ids)}
                                ) AS A1 
                                ON tic_tac_toe_axis_2.Axis_1_ID = A1.Axis_1_ID"""
                
        if league_ids != None: 
            sql = f"""SELECT team_id, A1.team_1, A1.team_2, A1.team_3, League_ID, A1.Axis_1_ID
                    FROM tic_tac_toe_axis_2 
                    INNER JOIN (
                        SELECT Axis_1_ID, team_1, team_2, team_3
                        FROM tic_tac_toe_axis_1 
                        WHERE League_ID IN {tuple(league_ids)}
                    ) AS A1 
                    ON tic_tac_toe_axis_2.Axis_1_ID = A1.Axis_1_ID
                    Where League_ID IN {tuple(league_ids)};"""
        df = pd.read_sql_query(sql, self.con)
        self.con.close()
        # format the df into the desired output
        output_df = pd.DataFrame(columns=["Axis 1", "Axis 2"])
        combinations_data = []
        for combination in df["Axis_1_ID"].unique():
            unique_axis_2_teams = list(df.loc[df["Axis_1_ID"]==combination, "team_id"].unique())
            if len(unique_axis_2_teams)>2:
                combination_df = {}
                combination_df["Axis 1"] = list(df.loc[df["Axis_1_ID"]==combination, ["team_1", "team_2", "team_3"]].drop_duplicates().values[0])
                combination_df["Axis 2"] = unique_axis_2_teams
                combinations_data.append(combination_df)
        output_df = pd.DataFrame(combinations_data)
        return output_df
    
    def get_combination_results(self, team_id_1, team_id_2):
        self.con = sqlite3.connect("Database/database.db")
        df = pd.read_sql_query(f'select * from data_tic_tac_toe_table where ("Club 1" in ({team_id_1}) and "Club 2" in ({team_id_2})) or ("Club 1" in ({team_id_2}) and "Club 2" in ({team_id_1}))', self.con)
        df["Player IDs"] = df["Player IDs"].apply(ast.literal_eval)
        self.con.close()
        return df

if __name__ == "__main__":
    t = getTables()
    config = Configuration()
    df = t.select_base_new(team_ids=config.top_teams, league_ids=None, exclusive=True)
    print(df)
    '''df = t.get_combination_results(583, 1041)
    print(df["Player IDs"][0])
    if str(3964) in df["Player IDs"][0]:
        print(True)'''