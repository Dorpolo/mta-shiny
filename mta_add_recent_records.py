import re
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import psycopg2
from psycopg2.extras import execute_values
from tabulate import tabulate


MONTH_MAP = {
    date(2020, i, 1).strftime('%B')[:3]: date(2020, i, 1).strftime('%m')[:3]
    for i in range(1, 13)
}


class ScrapMta:
    def __init__(self, n=10):
        now = datetime.now()
        s_year, s_month = now.year, now.month
        s_year_1 = s_year if s_month > 7 else s_year - 1
        s_year_2 = s_year + 1 if s_month > 7 else s_year
        self.url = 'https://www.maccabi-tlv.co.il/en/result-fixtures/first-team/results/'
        self.n = n
        self.season = str(s_year_1)[2:] + '-' + str(s_year_2)[2:]
        self.soup_mta = BeautifulSoup(requests.get(self.url).content, 'html.parser')
        self.game_links = [link.get('href') for link in self.soup_mta.find_all('a')]
        self.relevant_links = [item for item in self.game_links if
                               ('https://www.maccabi-tlv.co.il/en/match/' in item) &
                               ('overview/' not in item)][0:(self.n+1)]

    @staticmethod
    def generate_game_id(date: list, opp: list) -> tuple:
        splitted_date = [item.split(' ') for item in date]
        days = [item[0] for item in splitted_date]
        months = [MONTH_MAP[item[1]] for item in splitted_date]
        years = [item[2] for item in splitted_date]
        opponent = [w.replace(' ', '') for w in opp]
        game_id = [f"{day}{month}{year}{opp}" for day, month, year, opp in zip(days, months, years, opponent)]
        fixed_date = [
            datetime.strptime(f"{month}-{day}-{year} 00:00:00", "%m-%d-%Y %H:%M:%S")
            for day, month, year in zip(days, months, years)
        ]
        return game_id, fixed_date

    def soup_list_producer(self, tag: str, value: dict):
        return [item.text for item in self.soup_mta.find_all(tag, value)]

    def fetch_game_level_data(self) -> dict:
        locations_and_dates = self.soup_mta.find_all("div", {"class": "location"})
        locations = [item.find_all("div")[0].text for item in locations_and_dates]
        splitter_locations = [item.split(' ', 2) for item in locations]
        hours = [item[0] for item in splitter_locations]
        stadiums = [item[1] for item in splitter_locations]
        date_ids = [item.find_all("span")[0].text for item in self.soup_mta.find_all("div", {"class": "location"})]
        opponent_ids = self.soup_list_producer(tag='div', value={"class": "holder notmaccabi nn"})
        n = len(opponent_ids)

        output = {
                'game_id': self.generate_game_id(date_ids, opponent_ids)[0],
                'season': [self.season]*n,
                'date': self.generate_game_id(date_ids, opponent_ids)[1],
                'hour': hours,
                'stadium': stadiums,
                'maccabi': ['MTA']*n,
                'opponent': self.soup_list_producer('div', {"class": "holder notmaccabi nn"}),
                'res_1': self.soup_list_producer('span', {"class": "ss maccabi h"}),
                'res_2': self.soup_list_producer('span', {"class": "ss h"}) ,
                'league': self.soup_list_producer('div', {"class": "league-title"})
            }

        df = pd.DataFrame(output).sort_values(by=['date'])
        df['round'] = df.groupby(['season', 'league'])['date'].rank(method="first", ascending=True).astype(int)
        return df

    @staticmethod
    def fetch_id_from_game_page(url: str, player_page = False) -> str:
        soup_mta = BeautifulSoup(requests.get(f"{url}teams").content, "html.parser")
        game_id_date_text = soup_mta.find_all("header", {"class": "entry-header"})[0]\
            .text.replace('\n', '').replace('\t', '')
        loc_date_text = game_id_date_text.find(" ", 2)
        date_final = game_id_date_text[loc_date_text + 1:loc_date_text + 11]
        opponent_final = soup_mta.find_all("div", {"class": "team not-maccabi"})[0]\
            .text.replace('\n', '').replace('\t', '').replace('0', '')
        game_id = date_final + opponent_final
        if not player_page:
            return game_id.replace(' ', '').replace('/', '')
        else:
            game_id = game_id[:-1] if game_id[len(game_id) - 1].isdigit() else game_id
            return game_id.replace(' ', '').replace('/', '')

    def produce_valid_game_ids_table(self):
        game_ids = [self.fetch_id_from_game_page(x) for x in self.relevant_links]
        game_ids_adjusted = [string[:-1] if re.search(r'\d+$', string) is not None else string for string in game_ids]
        relevant_links = [f"{item}teams/" for item in self.relevant_links]
        game_mapping = {
            'game_id': [id for id in game_ids_adjusted],
            'game_url': [url for url in relevant_links],
            'date': [id[0:8] for id in game_ids_adjusted],
            'is_relevant': [id[0:8].isdigit() for id in game_ids_adjusted]
        }
        df = pd.DataFrame(game_mapping)
        return df.loc[df.is_relevant].drop(columns=['is_relevant', 'date'])

    @staticmethod
    def number_adj(number: int):
        num = str(number[0])
        start, end = num.find('>'), num.find('<', 2)
        output = num[start + 1:end]
        return output if output != '' else None

    def name_adj(item: int, txt: str):
        name = txt.find_all("li")[item].text
        valid_str = []
        index = 0
        for char in name:
            if char.isalpha():
                valid_str.append(index)
            index += 1
        if not valid_str:
            return None
        else:
            return name[valid_str[0]:valid_str[len(valid_str) - 1] + 1]

    @staticmethod
    def icon_extraction(icon_list: list) -> list:
        data = []
        for item in icon_list:
            if item.text != '' and item.text != '\n':
                if len(item.text) > 4:
                    data.append(float(re.sub(" ", '.', re.sub("'", '', item.text)[0:len(item.text)])))
                else:
                    data.append(float(re.sub("'", '', item.text)[0:len(item.text)]))
            else:
                data.append(None)
        return data

    @staticmethod
    def card_extraction(card_list: list) -> list:
        return [item.text.replace('\t', '').replace('\n', '') if item.text != '' and item.text != '\n' else None
                for item in card_list]

    def fetch_game_player_level_data(self, url: str):
        soup_mta = BeautifulSoup(requests.get(f"{url}teams").content, "html.parser")
        players = soup_mta.find_all("div", {"class": "p50 yellow"})
        player_numbers_start = [self.number_adj(item.find_all("b")) for item in players[0].find_all("li")[1:]]
        player_numbers_sub = [self.number_adj(item.find_all("b")) for item in players[1].find_all("li")[0:]]
        player_numbers = player_numbers_start + player_numbers_sub
        n1, n2 = len(player_numbers_start), len(player_numbers_sub)
        game_id = self.game_id_page(url, True)

        names_raw_data = soup_mta.find_all("div", {"class": "p50 yellow"})
        player_names_1 = [self.name_adj(i, names_raw_data[0]) for i in range(1, n1 + 1)]
        player_names_2 = [self.name_adj(i, names_raw_data[1]) for i in range(0, n2)]
        player_names = player_names_1 + player_names_2
        player_names = [item[:-3].replace('  ', ' ')
                        if '(C' in item else item.replace('  ', ' ') for item in player_names]
        is_captain = [True if '(C' in item else False for item in player_names]
        icon_goal = soup_mta.find_all("div", {"class": "p50 yellow"})
        goal_list = icon_goal[0].find_all("div", {'class': 'icons team-players goals'})
        goal_sub_list = icon_goal[1].find_all("div", {'class': 'icons team-players goals'})
        icon_exchange_list = icon_goal[0].find_all("div", {"class": "icons team-players", 'id': re.compile('exchange')})
        icon_exchange_sub_list = icon_goal[1].find_all("div", {
            "class": "icons team-players", 'id': re.compile('exchange')
        })
        goals = [item.text if item.text != '' else None for item in goal_list]
        goals_sub = [item.text if item.text != '' else None for item in goal_sub_list]
        goal_list = goals + goals_sub
        exchange_list = self.icon_extraction(icon_exchange_list) + self.icon_extraction(icon_exchange_sub_list)
        icon_card_list = icon_goal[0].find_all("div", {"class": "icons team-players", 'id': re.compile('red')})
        icon_card_sub_list = icon_goal[1].find_all("div", {"class": "icons team-players", 'id': re.compile('red')})
        card_list = self.card_extraction(icon_card_list) + self.card_extraction(icon_card_sub_list)

        data = {
            'game_id': [game_id]*(n1+n2),
            'player_number': player_numbers[:(n1+n2)],
            'game_status': ['opening']*n1 + ['substitute']*n2,
            'player_name': player_names[:(n1+n2)],
            'is_captain': is_captain[:(n1+n2)],
            'goals': goal_list[:(n1+n2)],
            'substitution': exchange_list[:(n1+n2)],
            'card': card_list[:(n1+n2)]
            }

        players_df = pd.DataFrame(data)

        players_df['minute_played'] = np.where(
            (players_df.subtitution.notna()) & (players_df.game_status == 'substitute'),
            90 - players_df['substitution'], np.where((players_df.subtitution.notna()) &
                                                      (players_df.game_status == 'opening'), players_df.subtitution,
                                                       np.where(players_df.game_status == 'opening', 90, 0)))
        players_df['is_played'] = np.where(players_df.minute_played > 0, True, False)
        # special treatment for M. Baltaxa
        players_df['player_name'] = np.where((players_df.player_name == 'NA') & (players_df.player_number == 3),
                                             'Matan Baltaxa', players_df.player_name)

        return players_df

    @staticmethod
    def home_away_extraction(url: str) -> str:
        scraped = BeautifulSoup(requests.get(url).content, "html.parser")
        data = scraped.find_all("div", {'class': re.compile('teams')})[0]
        return str(data)[18:22]

    @staticmethod
    def get_game_coach(url: str) -> str:
        try:
            scraped = BeautifulSoup(requests.get(url).content, "html.parser")
            coaches = scraped.find_all("div", {"class": "p50 yellow"})
            return coaches[2].find_all("li")[0].text
        except:
            return None

    @staticmethod
    def date_adjustment(col_name: str) -> str:
        return col_name[4:8] + "-" + col_name[2:4] + "-" + col_name[0:2]





