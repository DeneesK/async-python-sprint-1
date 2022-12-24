import multiprocessing
from multiprocessing import ProcessError

from config import logging
from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, GOOD_CONDITION, FILEPATH


logger = logging.getLogger(__name__)


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    logger.info('Forecast Wheather started')
    cities_names = CITIES.keys()
    yw_api = YandexWeatherAPI()
    data_fetch = DataFetchingTask(yw_api, cities_names)
    forecast_data = data_fetch.get_data()
    queue = multiprocessing.Queue()
    try:
        producer = DataCalculationTask(queue=queue, good_condition=GOOD_CONDITION, wheather_data=forecast_data)
        consumer = DataAggregationTask(queue=queue, filepath=FILEPATH)
        producer.start()
        consumer.start()
        producer.join()
        logger.info('Process - Calculation Data completed')
        consumer.join()
        logger.info('Process - Agregation Data completed')
    except ProcessError as err:
        logger.error(err)
    forecast_analyz = DataAnalyzingTask(filepath=FILEPATH)
    forecast_analyz.make_analyz()


if __name__ == "__main__":
    forecast_weather()
    logger.info('Forecast Wheather finished')
