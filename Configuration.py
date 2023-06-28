

class Configuration():
    def __init__(self):
        self.league_ids = {"/bundesliga/startseite/wettbewerb/L1":1, "/premier-league/startseite/wettbewerb/GB1":2,
                            "/primera-division/startseite/wettbewerb/ES1":3, "/serie-a/startseite/wettbewerb/IT1":4,
                            "/ligue-1/startseite/wettbewerb/FR1":5}
        self.base_url = "https://www.transfermarkt.com"
        self.club_id_retired = 123