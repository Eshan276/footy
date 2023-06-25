from urllib.request import urlopen, Request
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


if __name__ == "__main__":
    #url = "https://www.transfermarkt.de/bundesliga/startseite/wettbewerb/L1.html"
    base_url = "https://www.transfermarkt.com"

    urls = ["/bundesliga/startseite/wettbewerb/L1", "/premier-league/startseite/wettbewerb/GB1",
            "/primera-division/startseite/wettbewerb/ES1", "/serie-a/startseite/wettbewerb/IT1",
            "/ligue-1/startseite/wettbewerb/FR1"]
    urls = [ base_url + url for url in urls]
    for url in urls:
        r = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        html = urlopen(r)
        bs = BeautifulSoup(html, 'html.parser')
        
        # Get the team names
        team_table = bs.find('table', {'class': 'items'})
        team_rows = team_table.find_all('td', {"class":"hauptlink no-border-links"})
        
        teams = {}
        for row in team_rows:
            team_name = row.text.strip().split(' \\')[0]
            team_href = row.find('a')['href']
            team_id = team_href.split('/')[4]
            teams[team_name]={'href': team_href, 'id': team_id}
        print(teams)
        #teams = [row.text.strip().split(' \\')[0] for row in team_rows]
        #print(teams)