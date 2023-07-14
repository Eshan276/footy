from kivy.lang import Builder
from kivymd.app import MDApp
import os
import sys
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
from Database.getTables import getTables
from TicTacToe.tictactoe import TicTacToe

class MyApp(MDApp):
	title = "Tic Tac Toe!"

	def build(self):
		self.theme_cls.theme_style = "Dark"
		self.theme_cls.primary_palette = "BlueGray"
		return Builder.load_file('my.kv')

		
	# Define Who's turn it is
	turn = "X"
	# Keep Track of win or lose
	winner = False
	
	# Keep track of winners and losers
	X_win = 0
	O_win = 0
	t = TicTacToe()

	league_id = [1]
	team_combinations = t.roll_combinations(league_id, 50, None)
	index = 0
	# load the first teams ids
	team1_id = str(team_combinations.loc[index, "Axis1"][0])
	team1_name = str(team_combinations.loc[index, "TeamsAxis1"][0])
	team2_id = str(team_combinations.loc[index, "Axis1"][1])
	team2_name = str(team_combinations.loc[index, "TeamsAxis1"][1])
	team3_id = str(team_combinations.loc[index, "Axis1"][2])
	team3_name = str(team_combinations.loc[index, "TeamsAxis1"][2])
	team4_id = str(team_combinations.loc[index, "Axis2"][0])
	team4_name = str(team_combinations.loc[index, "TeamsAxis2"][0])
	team5_id = str(team_combinations.loc[index, "Axis2"][1])
	team5_name = str(team_combinations.loc[index, "TeamsAxis2"][1])
	team6_id = str(team_combinations.loc[index, "Axis2"][2])
	team6_name = str(team_combinations.loc[index, "TeamsAxis2"][2])

	def next_combination(self):
		# maybe include random reroll of positions
		if self.index < (len(self.team_combinations)-1):
			self.index += 1
		else:
			self.team_combinations = self.t.roll_combinations(self.league_id, 50, None)
			self.index = 0
		self.team1_id = str(self.team_combinations.loc[self.index, "Axis1"][0])
		self.team1_name = str(self.team_combinations.loc[self.index, "TeamsAxis1"][0])
		self.team2_id = str(self.team_combinations.loc[self.index, "Axis1"][1])
		self.team2_name = str(self.team_combinations.loc[self.index, "TeamsAxis1"][1])
		self.team3_id = str(self.team_combinations.loc[self.index, "Axis1"][2])
		self.team3_name = str(self.team_combinations.loc[self.index, "TeamsAxis1"][2])
		self.team4_id = str(self.team_combinations.loc[self.index, "Axis2"][0])
		self.team4_name = str(self.team_combinations.loc[self.index, "TeamsAxis2"][0])
		self.team5_id = str(self.team_combinations.loc[self.index, "Axis2"][1])
		self.team5_name = str(self.team_combinations.loc[self.index, "TeamsAxis2"][1])
		self.team6_id = str(self.team_combinations.loc[self.index, "Axis2"][2])
		self.team6_name = str(self.team_combinations.loc[self.index, "TeamsAxis2"][2])
		self.reload_team_labels()
		self.restart()

	def reload_team_labels(self):
		self.root.ids.lab1.text = self.team1_name
		self.root.ids.lab2.text = self.team2_name
		self.root.ids.lab3.text = self.team3_name
		self.root.ids.lab4.text = self.team4_name
		self.root.ids.lab5.text = self.team5_name
		self.root.ids.lab6.text = self.team6_name

	# End The Game
	def end_game(self, a,b,c):
		self.winner = True
		a.color = "red"
		b.color = "red"
		c.color = "red"

		# Disable the buttons
		self.disable_all_buttons()

		# Set Label for winner
		#self.root.ids.score.text = f"{a.text} Wins!"

		# Keep track of winners and loser
		if a.text == "X":
			self.X_win = self.X_win + 1	
		else:
			self.O_win = self.O_win + 1

		self.root.ids.game.text = f"X Wins: {self.X_win}  |  O Wins: {self.O_win}"

	def disable_all_buttons(self):
		# Disable The Buttons
		self.root.ids.btn1.disabled = True
		self.root.ids.btn2.disabled = True
		self.root.ids.btn3.disabled = True
		self.root.ids.btn4.disabled = True
		self.root.ids.btn5.disabled = True
		self.root.ids.btn6.disabled = True
		self.root.ids.btn7.disabled = True
		self.root.ids.btn8.disabled = True
		self.root.ids.btn9.disabled = True

	def win(self):
		# Across
		if self.root.ids.btn1.text != "" and self.root.ids.btn1.text == self.root.ids.btn2.text and self.root.ids.btn2.text == self.root.ids.btn3.text:
			self.end_game(self.root.ids.btn1, self.root.ids.btn2, self.root.ids.btn3)

		if self.root.ids.btn4.text != "" and self.root.ids.btn4.text == self.root.ids.btn5.text and self.root.ids.btn5.text == self.root.ids.btn6.text:
			self.end_game(self.root.ids.btn4, self.root.ids.btn5, self.root.ids.btn6)

		if self.root.ids.btn7.text != "" and self.root.ids.btn7.text == self.root.ids.btn8.text and self.root.ids.btn8.text == self.root.ids.btn9.text:
			self.end_game(self.root.ids.btn7, self.root.ids.btn8, self.root.ids.btn9)
		# Down
		if self.root.ids.btn1.text != "" and self.root.ids.btn1.text == self.root.ids.btn4.text and self.root.ids.btn4.text == self.root.ids.btn7.text:
			self.end_game(self.root.ids.btn1, self.root.ids.btn4, self.root.ids.btn7)

		if self.root.ids.btn2.text != "" and self.root.ids.btn2.text == self.root.ids.btn5.text and self.root.ids.btn5.text == self.root.ids.btn8.text:
			self.end_game(self.root.ids.btn2, self.root.ids.btn5, self.root.ids.btn8)

		if self.root.ids.btn3.text != "" and self.root.ids.btn3.text == self.root.ids.btn6.text and self.root.ids.btn6.text == self.root.ids.btn9.text:
			self.end_game(self.root.ids.btn3, self.root.ids.btn6, self.root.ids.btn9)

		# Diagonal 
		if self.root.ids.btn1.text != "" and self.root.ids.btn1.text == self.root.ids.btn5.text and self.root.ids.btn5.text == self.root.ids.btn9.text:
			self.end_game(self.root.ids.btn1, self.root.ids.btn5, self.root.ids.btn9)

		if self.root.ids.btn3.text != "" and self.root.ids.btn3.text == self.root.ids.btn5.text and self.root.ids.btn5.text == self.root.ids.btn7.text:
			self.end_game(self.root.ids.btn3, self.root.ids.btn5, self.root.ids.btn7)

	def presser(self, btn):
		if self.turn == 'X':
			btn.text = "X"
			btn.disabled = True
			#self.root.ids.score.text = "O's Turn!"
			self.turn = "O"
		else:
			btn.text = "O"
			btn.disabled = True
			#self.root.ids.score.text = "X's Turn!"
			self.turn = "X"

		# Check To See if won
		self.win()

	def restart(self):
		# Reset Who's Turn It Is
		self.turn = "X"

		# Enable The Buttons
		self.root.ids.btn1.disabled = False
		self.root.ids.btn2.disabled = False
		self.root.ids.btn3.disabled = False
		self.root.ids.btn4.disabled = False
		self.root.ids.btn5.disabled = False
		self.root.ids.btn6.disabled = False
		self.root.ids.btn7.disabled = False
		self.root.ids.btn8.disabled = False
		self.root.ids.btn9.disabled = False

		# Clear The Buttons
		self.root.ids.btn1.text = ""
		self.root.ids.btn2.text = ""
		self.root.ids.btn3.text = ""
		self.root.ids.btn4.text = ""
		self.root.ids.btn5.text = ""
		self.root.ids.btn6.text = ""
		self.root.ids.btn7.text = ""
		self.root.ids.btn8.text = ""
		self.root.ids.btn9.text = ""

		# Reset The Button Colors
		self.root.ids.btn1.color = "green"
		self.root.ids.btn2.color = "green"
		self.root.ids.btn3.color = "green"
		self.root.ids.btn4.color = "green"
		self.root.ids.btn5.color = "green"
		self.root.ids.btn6.color = "green"
		self.root.ids.btn7.color = "green"
		self.root.ids.btn8.color = "green"
		self.root.ids.btn9.color = "green"

		# Reset The Score Label
		# self.root.ids.score.text = "X GOES FIRST!"

		# Reset The Winner Variable
		self.winner = False

	def get_matching_player_names(self, text):
		player_names = ["Thomas Müller", "thomas Müller", "Timmy", "Marco Friedl"]		
		matching_names = [name for name in player_names if text.lower() in name.lower()]
		self.root.ids.labell.text = str(matching_names)


if __name__ == '__main__':
    MyApp().run()