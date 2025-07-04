import os
import json
import time
import random
import logging
import requests
import datetime
import pandas as pd

from tqdm import tqdm

from src.parsing.base import PathsList

def update_moex_instruments_data() -> pd.DataFrame:
    """Получение списка доступных инструментов ММВБ"""
    try:
        url = "https://data.moex.com/products/moexdata/api/v1/bidding/shares?limit=1000"
        response = requests.get(url, timeout=10)
        data = json.loads(response.text)
        df = pd.DataFrame(data)
        df_instruments = df.instruments.apply(pd.Series)
        df_instruments.to_csv(PathsList.instruments_path, index=False)
    except Exception as ex:
        logger.error(ex)



def get_tickers_dict():
    """Загружает словарь сопоставления {ticker:id}"""
    if os.path.isfile(PathsList.instruments_path) == False:
        df_instruments = update_moex_instruments_data()
    return (
        pd.read_csv(PathsList.instruments_path)
            .rename(columns={"secId":"ticker"})
            .set_index("ticker").id\
            .to_dict()
        )


def send_get_request(
    instrument_id: str,
    startDate: str,
    endDate: str,
    limit: int=365,
    page: int=1
) -> requests.models.Response:
    """Отправить запрос для получение данных динамики одного ticker"""
    base_url = ("https://data.moex.com/products/moexdata/api/v1/bidding/shares/"
        "{instrument_id}?startDate={startDate}&endDate={endDate}&page={page}&limit={limit}")
    url = base_url.format(
        instrument_id=instrument_id,
        startDate=startDate,
        endDate=endDate,
        limit=limit,
        page=page
    )
    response = requests.get(url, timeout=10)
    return response


def get_instruments_dynamics_data(ticket: str, years: int=20, tickers_dict=None) -> pd.DataFrame:
    """Получение данных динамика инструмента"""
    if tickers_dict is None:
        tickers_dict = get_tickers_dict()
    instrument_id = tickers_dict.get(ticket)
    
    dataframe = pd.DataFrame()

    endDate = datetime.datetime.now()
    timedelta = datetime.timedelta(days=365)

    try:
        for year in range(years):
            startDate = endDate - timedelta
            
            startDate_str = startDate.strftime("%Y-%m-%d")
            endDate_str = endDate.strftime("%Y-%m-%d")
            
            endDate = startDate
            
            response = send_get_request(
                instrument_id=instrument_id,
                startDate=startDate_str,
                endDate=endDate_str
            )
    
            data = json.loads(response.text)
            dataframe_one_year = pd.DataFrame(data)["instruments"].apply(pd.Series)
            if dataframe_one_year.shape[0] == 0:
                break
                
            dataframe = pd.concat(
                (dataframe, dataframe_one_year),
                ignore_index=True
            )
    except Exception as ex:
        dataframe = get_instruments_dynamics_data(ticket, years, tickers_dict)
    finally:
        return dataframe

        
def update_moex_dynamics_data(years: int = 20) -> None:
    """Выгрузить и сохранить в csv файлы актуальные данные динамики инструментов"""
    tickers_dict = get_tickers_dict()
    for ticket in tqdm(get_tickers_dict().keys()):
        file_path = PathsList.raw_dynamics_path + ticket + ".csv"
        dataframe = get_instruments_dynamics_data(
            ticket=ticket, 
            tickers_dict=tickers_dict,
            years=years
        )
        dataframe.to_csv(file_path, index=False)
        time.sleep(random.random())


def read_moex_dynamics_data() -> pd.DataFrame:
    """Сбор датафрейма с динамикой из файлов"""
    dataframe = pd.DataFrame()
    file_name_list = os.listdir(PathsList.raw_dynamics_path)
    try:
        for file_name in tqdm(file_name_list):
            if file_name.split(".")[-1] == "csv":
                file_path = PathsList.raw_dynamics_path + file_name
                dataframe = pd.concat(
                    (
                        dataframe,
                        pd.read_csv(file_path)
                    ), ignore_index=True
                )
    except Exception as ex:
        print(ex)
    finally:
        return dataframe