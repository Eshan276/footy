import sqlite3
import random
import pandas as pd
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
        # get team_df
        self.meta_teams_df = pd.read_sql_query("Select * from meta_club_table", sqlite3.connect("Database/database.db"))

    def roll_combinations(self, league_ids=None, num_random_numbers=50, team_ids=None, exact=True):
        '''
        Create a dict of [num_random_numbers] of random combinations for a tic tac toe grid. 
        '''
        # select the team_ids of the teams for the given leagues
        gT = getTables()
        if league_ids != None:
            # TODO maybe no need to load Configuration instead use self.meta_teams_df
            config = Configuration()
            team_ids = [id for league_id in league_ids for id in config.team_ids[league_id]]
            df = getTables.select_base_new(gT, team_ids=None, league_ids=league_ids, exclusive=exact)
        else:
            df = getTables.select_base_new(gT, team_ids=team_ids, league_ids=None, exclusive=exact)
        if len(df)>20000:            
            # randomly generate the indexes of the df rows to determine the combinations -> faster computation time -> TODO maybe build into query even faster
            random_numbers = [random.randint(0, len(df) - 1) for _ in range(num_random_numbers)]
            df = df.iloc[random_numbers].reset_index(drop=True)
        if league_ids != None: 
            # if a league is picked then axis 1 is automatically restricted to teams from the league
            # therefore also limit axis 2 to teams from that league
            df["Axis 2"] = df["Axis 2"].apply(lambda x: [i for i in x if i in team_ids if len(set(team_ids).intersection(set(x)))>2])
        # randomly draw the combinations 
        # TODO also randomly swap between the two axes or do it later in the implementation phase
        combinations_df = pd.DataFrame(columns=["Axis1", "Axis2"])
        for indexer in range(0, len(df)):
            combination = list(combinations(df.iloc[indexer]["Axis 2"], 3))
            number = random.randint(0,len(combination)-1)
            combinations_df = pd.concat([combinations_df, pd.DataFrame({"Axis1":[combination[number]],"Axis2": [df.iloc[indexer]["Axis 1"]]})], axis=0)
        return self.getNamesForCombinations(combinations_df.reset_index(drop=True))

    def getNamesForCombinations(self, df): #return the team names for a combination
        df["TeamsAxis1"] = df["Axis1"].apply(lambda x: [str(self.meta_teams_df.loc[self.meta_teams_df.id == str(i), "team_name"].values[0]) for i in x])
        df["TeamsAxis2"] = df["Axis2"].apply(lambda x: [str(self.meta_teams_df.loc[self.meta_teams_df.id == str(i), "team_name"].values[0]) for i in x])
        return df

if __name__ == '__main__':
    # TicTacToe.select_base(league_ids=[1])
    config = Configuration()
    t = TicTacToe()
    df = t.roll_combinations(team_ids=config.top_teams, exact=True)
    print(df)
