from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
from pandas import DataFrame
import time
from bs4 import BeautifulSoup as bs
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import NoSuchElementException
import logging
import os
from datetime import datetime
import time
import threading
from time import sleep
from multiprocessing import Process,Pool,Lock
from dateutil.parser import parse
import functools
import re
options = Options()
options.add_argument("--disable-infobars") 
options.add_argument("--window-size=1920x1080")
# options.add_argument("--headless")

lock = Lock()

def parse_okpdtr(page_n, page_amount, page_sample, url) -> None:
    driver: webdriver.Chrome = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    result = []
    while page_n<=page_amount:
        try:
            if page_n>1:
                url = page_sample.format(page_n)

            driver.get(url)
            source_data = driver.page_source
            soup = bs(source_data)
            elements = soup.select('.tablica a')
            for element in elements:
                if element.find('div', {'class': 'exclude'}):
                    continue
                id = element.find('div',{'class': 'my_col1'}).text
                value = element.find('div',{'class': 'my_col2'}).text
                result.append({
                    'id': int(id),
                    'okpdtr_name': value
                })

            page_n += 1
        except:
            pass
    
    driver.close()
    lock.acquire()
    with open('result.txt', 'a') as f:
        for obj in result:
            f.write(f"Код: {obj['id']} Наименование: {obj['okpdtr_name']}\n")
    lock.release()



def tableparse_rst_gov(tbl: DataFrame):
    content = tbl.get_attribute("outerHTML")
    df = pd.read_html(content)
    table: DataFrame = df[0]
    result = []
    for k, v in table.iterrows():
        result.append({
            'car_model': v['Марка трансп. средства (шасси)'],
            'car_type': v['Тип транспортного средства(шасси)'],
            'approval_number': v['Номер "Одобрения типа транспортного средства"'],
            'validity_start_date': parse(v['Дата начала срока действия⨯']) if v['Дата начала срока действия⨯'] else None,
            'code': v['Эк. кл.'],
            'notion': v['Примечание']
        })
    return result

def parse_rst_gov():
    page_n = 27
    page=0
    url = 'https://www.rst.gov.ru/portal/gost/home/activity/compliance/evaluationcompliance/AcknowledgementCorrespondence/vehicletypeapproval018'   
    
    driver: webdriver.Chrome = webdriver.Chrome(ChromeDriverManager().install(), options=options)
    driver.get(url)
    next_page_button = driver.find_element(By.XPATH, "//*[contains(text(), 'Вперед →')]")
    while next_page_button and page < page_n:
        try:
            tbl = WebDriverWait(driver, timeout=10).until(lambda d: d.find_element(By.ID, "standartsList"))
            with open('result_rst_gov.txt', 'a') as f:
                for obj in tableparse_rst_gov(tbl):
                    f.write(f"{str(obj)}\n")
                    print('write rst_gov')
        except Exception as e:
            print(e)
        next_page_button = driver.find_element(By.XPATH, "//*[contains(text(), 'Вперед →')]")
        next_page_button.click()
        page+=1
    driver.close()


def smap(f, *args):
    return f(*args)

if __name__ == '__main__':
    url_1 = 'http://okpdtr.ru/professii-rabochih/'
    page_sample1= 'http://okpdtr.ru/professii-rabochih/page/{}/'

    url_2 = 'http://okpdtr.ru/dolzhnosti-sluzhashih/'
    page_sample_2 = 'http://okpdtr.ru/dolzhnosti-sluzhashih/page/{}/'

    functions = [
        functools.partial(parse_okpdtr, 1,9,page_sample1,url_1),
        functools.partial(parse_okpdtr, 10,27,page_sample1,url_1),
        functools.partial(parse_okpdtr, 1,6,page_sample_2,url_2),
        functools.partial(parse_okpdtr,7,12,page_sample_2,url_2),
        functools.partial(parse_rst_gov),
    ]

    with Pool(5) as p:
        p.map(smap, functions)
