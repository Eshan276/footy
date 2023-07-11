import sqlite3
import random
import pandas as pd
import ast
from itertools import combinations
# Add the project root directory to the Python path
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from Database.getTables import getTables
from Configuration import Configuration

class TicTacToe:
    def __init__(self):
        pass

    def roll_combinations(league_ids=None, num_random_numbers=50, team_ids=None):
        '''
        Create a dict of [num_random_numbers] of random combinations for a tic tac toe grid. 
        '''
        # select the team_ids of the teams for the given leagues
        gT = getTables()
        if league_ids != None:
            config = Configuration()
            team_ids = [id for league_id in league_ids for id in config.team_ids[league_id]]
            '''OLD IMPLEMENTATION -> requires database connection.
            
            con = sqlite3.connect("Database/database.db")
            sql = """SELECT *
            FROM meta_club_table
            WHERE league_id IN ({league_ids})"""
            league_ids_condition = ", ".join(str(league_id) for league_id in league_ids)
            team_df = pd.read_sql_query(sql.format(league_ids=league_ids_condition), con)
            team_ids = team_df["id"].astype(int).unique().tolist()
            con.close()'''
            df = getTables.select_base(gT, team_ids=None, league_ids=league_ids)
        else:
            df = getTables.select_base(gT, team_ids=team_ids, league_ids=None)
            # TODO maybe include a filter to eclusively select the teams e.g. only those teams are on both axes and no other
        
        # randomly generate the indexes of the df rows to determine the combinations
        random_numbers = [random.randint(0, len(df) - 1) for _ in range(num_random_numbers)]
        df = df.iloc[random_numbers].reset_index(drop=True)
        # transform the string elements of the columns to lists
        df["Axis 1"] = df["Axis 1"].apply(ast.literal_eval)
        df["Axis 2"] = df["Axis 2"].apply(ast.literal_eval)

        if league_ids != None: 
            # if a league is picked then axis 1 is automatically restricted to teams from the league
            # therefore also limit axis 2 to teams from that league
            df["Axis 2"] = df["Axis 2"].apply(lambda x: [i for i in x if i in team_ids])
        # randomly draw the combinations 
        # TODO also randomly swap between the two axes or do it later in the implementation phase
        combinations_df = pd.DataFrame(columns=["Axis1", "Axis2"])
        for indexer in range(0, len(df)):
            combination = list(combinations(df.iloc[indexer]["Axis 2"], 3))
            number = random.randint(0,len(combination)-1)
            combinations_df = pd.concat([combinations_df, pd.DataFrame({"Axis1":[combination[number]],"Axis2": [df.iloc[indexer]["Axis 1"]]})], axis=0)
        return combinations_df.reset_index(drop=True)

    def getNamesForCombinations():
        pass


if __name__ == '__main__':
    # TicTacToe.select_base(league_ids=[1])
    print(TicTacToe.roll_combinations(league_ids = [1, 5]))