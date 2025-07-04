import requests
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup

from src.parsing.base import PathsList


def get_companies_list() -> list:
    """Получение списка компаний и ссылки на их страницы"""
    
    url = "https://закрытияреестров.рф/_/"
    base_company_url = "https://закрытияреестров.рф/{company_short_name}/"
    
    response = requests.get(url)
    html = response.content.decode('utf-8', errors='ignore')
    soup = BeautifulSoup(html, "html.parser")
    soup_widget_content = soup.find_all("div", class_="widget-content")[4]

    companies_list = []
    for link in soup_widget_content.find_all("a", class_="link")[:-2]:
            
        full_text = link.get_text('', strip=True)
        company_short_name = link.get('href')[3:-1]
    
        company_url = base_company_url.format(company_short_name=company_short_name)
        companies_list.append({
            "company_short_name": company_short_name,
            "company_name": full_text,
            "company_url": company_url
        })
    return companies_list


def get_company_dividends_data(company_url: str) -> list:
    """Получить информацию и дивидендах компании"""
    response = requests.get(company_url)
    html = response.content.decode('utf-8', errors='ignore')
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select('table tr')[1:] 

    dividends_list = []
    for row in rows:
        cols = row.find_all('td')
        period = cols[0].get_text(' ', strip=True)
        period = ' '.join(period.split())
        amount = cols[1].get_text(' ', strip=True)
        dividends_list.append(
            {
                "period":period,
                "amount":amount
            }
        )
    return dividends_list


def update_companies_dividends_data():
    """Обновить данные о дивидендах компаний"""
    dataframe = pd.DataFrame()

    companies_list = get_companies_list()
    for company in tqdm(companies_list):
    
        company_url = company.get("company_url")
        company_dividends = get_company_dividends_data(company_url)
    
        dataframe_company = pd.DataFrame(company_dividends)\
            .assign(company_short_name=company.get("company_short_name"))\
            .assign(company_name=company.get("company_name"))
    
        dataframe = pd.concat(
            (
                dataframe, 
                dataframe_company
            ), ignore_index=True 
        )
    dataframe.to_csv(PathsList.raw_dividends_path, index=False)

    