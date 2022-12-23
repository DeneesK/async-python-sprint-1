# import logging
import multiprocessing

from ttm import measure_time
from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, GOOD_CONDITION


@measure_time
def forecast_weather(queue: multiprocessing.Queue):
    """
    Анализ погодных условий по городам
    """
    cities_names = CITIES.keys()
    ywAPI = YandexWeatherAPI()
    data_fetch = DataFetchingTask(ywAPI, cities_names)
    forecast_data = data_fetch.get_data()
    producer = DataCalculationTask(queue=queue, good_condition=GOOD_CONDITION, wheather_data=forecast_data)
    consumer = DataAggregationTask(queue=queue)
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()
    forecast_analyz = DataAnalyzingTask('forecast_table.xlsx')
    forecast_analyz.make_analyz()


if __name__ == "__main__":
    queue = multiprocessing.Queue()
    forecast_weather(queue)
