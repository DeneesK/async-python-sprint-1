import sys
import json
import multiprocessing

import pytest
import pydantic

sys.path.append('..\\async-python-sprint-1')  # for Linux '../async-python-sprint-1'

from tasks import DataFetchingTask, DataAggregationTask, DataCalculationTask, DataAnalyzingTask  # noqa
from api_client import YandexWeatherAPI  # noqa
from utils import GOOD_CONDITION  # noqa
from models import (WheatherForecastModel, CityForecastModel)  # noqa


@pytest.fixture
def data_fetch():
    def wrapper(cities):
        yw_api = YandexWeatherAPI()
        data = DataFetchingTask(cities=cities, wheather_api=yw_api)
        return data.get_data()
    return wrapper


@pytest.fixture
def calc_data():
    with open('tests\\test.json', 'r') as file:  # for Linux 'tests/test.json'
        json_data = json.load(file)
    wheather_data = pydantic.parse_obj_as(WheatherForecastModel, json_data)
    data = CityForecastModel(city='MOSCOW', forecasts=wheather_data)
    queue = multiprocessing.Queue()
    data_calc = DataCalculationTask(queue=queue, good_condition=GOOD_CONDITION, wheather_data=[data])
    calculated_data = data_calc._prepare_data(data)
    return calculated_data


@pytest.fixture
def agregate_data():
    def wrapper():
        with open('tests\\test.json', 'r') as file:  # for Linux 'tests/test.json'
            json_data = json.load(file)
        wheather_data = pydantic.parse_obj_as(WheatherForecastModel, json_data)
        data = CityForecastModel(city='MOSCOW', forecasts=wheather_data)
        queue = multiprocessing.Queue()
        producer = DataCalculationTask(queue=queue, good_condition=GOOD_CONDITION, wheather_data=[data])
        consumer = DataAggregationTask(queue=queue, filepath='tests\\test.csv')  # for Linux 'tests/test.csv'
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()
    return wrapper


@pytest.fixture
def analyz_data():
    def wrapper():
        analyz = DataAnalyzingTask(filepath='tests\\test.csv')  # for Linux 'tests/test.csv'
        recomendation = analyz.make_analyz()
        return recomendation
    return wrapper
