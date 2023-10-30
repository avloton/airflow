import requests
from bs4 import BeautifulSoup
import pandas as pd


class MyParser:

    def __init__(self, category_name):
        self.category_name = category_name

    def get_data(self, page_number=1):
        response = requests.get(f'https://zoomag.ru/{self.category_name}/page-{page_number}/')
        soup = BeautifulSoup(response.text)
        all_items = soup.find_all('div', {"class": 'col-tile'})
        if len(all_items):
            return all_items
        else:
            return None

    def create_dataframe(self, items):
        d = {'item_name': [],
             'price': []}

        for i in items:
            name = i.find_all('a', {'class': 'product-title'})
            if len(name):
                d['item_name'].append(name[0].text)
            price = i.find_all('span', {'class': 'ty-list-price'})
            if len(price):
                d['price'].append(price[0].text)
        return pd.DataFrame(d)

    def take_all_files(self):
        buf = []
        page_number = 1
        while True:
            data = self.get_data(page_number)
            if data is None:
                break
            df = self.create_dataframe(data)
            buf.append(df)
            page_number += 1
        full_data = pd.concat(buf)
        return full_data





