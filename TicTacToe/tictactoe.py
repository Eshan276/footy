import sqlite3
import random
import pandas as pd
import ast
from itertools import combinations

class TicTacToe():
    def __init__(self):
        pass
    def roll_combinations(league_id=None, num_random_numbers=50):
        '''Create a dict of [num_random_numbers] of random combinations for a tic tac toe grid. 
        (If wanted the selection choices can be reduced to a certain league)
        '''
        df = pd.read_sql_query(f"select * from tic_tac_toe_combinations", sqlite3.connect("Database/database.db"))
        # filter out combinations for a certain league -> create dummy in data_tic-tac-toe_combinations
        # team_df = pd.read_sql_query(f"select * from data_teams where league = '{league}'", sqlite3.connect("Database/database.db"))
        if league_id != None:
            df = df.loc[df.isna().any(axis=1)==False].copy()
            df = df.loc[df["League ID"].astype(int)==int(league_id)].copy().reset_index(drop=True)
            team_df = pd.read_sql_query(f"select * from meta_club_table where league_id = '{league_id}'", sqlite3.connect("Database/database.db"))
            team_ids = team_df["id"].astype(int).unique().tolist()
        # generate the indexes of the df rows to determine the combinations
        length = len(df)
        random_numbers = [random.randint(0, length - 1) for _ in range(num_random_numbers)]
        # filter the df
        df = df.iloc[random_numbers].reset_index(drop=True)
        df["Axis 1"] = df["Axis 1"].apply(ast.literal_eval)
        df["Axis 2"] = df["Axis 2"].apply(ast.literal_eval)
        if league_id != None:
            df["Axis 2"] = df["Axis 2"].apply(lambda x: [i for i in x if i in team_ids])
        combinations_dict = {}
        for indexer in range(0, len(df)):
            combination = list(combinations(df.iloc[indexer]["Axis 2"], 3))
            number = random.randint(0,len(combination)-1)
            combinations_dict[str(df.iloc[indexer]["Axis 1"])] = combination[number]
        return combinations_dict


if __name__ == '__main__':
    TicTacToe.roll_combinations(league_id = 1)