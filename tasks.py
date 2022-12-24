import csv
from multiprocessing import Process, Queue
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor

from pydantic import parse_obj_as, ValidationError

from config import logging
from api_client import YandexWeatherAPI
from utils import MSG_RCMND_CITIES, START_TIME, END_TIME, FINISH
from models import (
    WheatherForecastModel,
    CityForecastModel,
    WheatherDateModel,
    CalculatedCityModel,
    CalculatedDataModel
)


logger = logging.getLogger(__name__)


class DataFetchingTask:
    def __init__(self, wheather_api: YandexWeatherAPI, cities: Iterable):
        self.wheather_api = wheather_api
        self.cities = cities

    def _run(self) -> Iterable:
        logger.info('DataFetching started')
        with ThreadPoolExecutor() as pool:
            data = pool.map(self.wheather_api.get_forecasting, self.cities)
        logger.info('DataFetching completed')
        return data

    def get_data(self) -> list[CityForecastModel]:
        resp = self._run()
        result = []
        for x in resp:
            try:
                result.append((x[0], parse_obj_as(WheatherForecastModel, x[1])))
            except ValidationError as err:
                logger.error(err)
                continue
        return [CityForecastModel(city=x[0], forecasts=x[1]) for x in result]


class DataCalculationTask(Process):
    def __init__(self, queue: Queue, wheather_data: list, good_condition: set):
        super().__init__()
        self.queue = queue
        self.weather_data = wheather_data
        self.good_condition = good_condition

    def _check_condition(self, condition: str) -> bool:
        return condition in self.good_condition

    def _calc_average_temp(self, data: WheatherDateModel) -> float | None:
        hours = data.hours
        temperatures = [x.temp for x in hours if int(x.hour) >= START_TIME and int(x.hour) <= END_TIME]
        if not len(temperatures):
            return None
        avg_temp = round((sum(temperatures) / len(temperatures)), 1)
        return avg_temp

    def _calc_good_cond_hours(self, data: WheatherDateModel) -> int:
        hours = data.hours
        sum_good_hours = sum([1 for x in hours if all((self._check_condition(x.condition),
                                                      int(x.hour) >= START_TIME,
                                                      int(x.hour) <= END_TIME)
                                                      )])
        return sum_good_hours

    def _data_by_day(self, data: WheatherForecastModel) -> list[CalculatedDataModel]:
        result = []
        forecasts = data.forecasts
        for x in forecasts:
            temp = self._calc_average_temp(x)
            good_hours = self._calc_good_cond_hours(x)
            result.append(CalculatedDataModel(date=x.date, avg_temp=temp, good_hours=good_hours))
        return result

    def _prepare_data(self, data: CityForecastModel) -> CalculatedCityModel:
        dates = self._data_by_day(data.forecasts)
        return CalculatedCityModel(city=data.city, dates=dates)

    def run(self) -> None:
        logger.info('Process - Calculation Data started')
        for data in self.weather_data:
            item = self._prepare_data(data)
            self.queue.put(item)
        self.queue.put(FINISH)


class DataAggregationTask(Process):
    def __init__(self, queue: Queue, filepath: str):
        super().__init__()
        self.queue = queue
        self.df_list = []
        self.filepath = filepath

    def _agregate_data(self, data: CalculatedCityModel) -> dict:
        df = {}
        df['City'] = data.city
        df[' '] = 'Avg temp° / No precipitation, h'
        for d in data.dates:
            date = d.date
            avg_temp = d.avg_temp
            good_hours = d.good_hours
            df[date] = f'{avg_temp}/{good_hours}'
        return df

    def run(self) -> None:
        logger.info('Process - Agregation Data started')
        while data := self.queue.get():
            item = self._agregate_data(data)
            self.df_list.append(item)
        logger.info('All tasks done')
        with open(self.filepath, 'w') as file:
            writer = csv.DictWriter(file, delimiter=';', fieldnames=[*self.df_list[0]])
            writer.writeheader()
            writer.writerows(self.df_list)


class DataAnalyzingTask:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def _prepare_to_analyz(self) -> list[dict]:
        with open(self.filepath, 'r') as file:
            reader = csv.DictReader(file, delimiter=';')
            data = [d for d in reader]
        return data

    def _calc_rating(self, item: dict) -> int:
        data = []
        for k in [*item]:
            if item[k][0].isalpha():
                continue
            data.extend(item[k].split('/'))
        data = list(map(lambda n: float(n), data))
        try:
            return int(sum(data) / len(data))
        except ZeroDivisionError as err:
            logger.error(err)
            return 0

    def _add_raiting(self) -> list[dict]:
        result = []
        data = self._prepare_to_analyz()
        for d in data:
            r = self._calc_rating(d)
            result.append(d | {'Rating': r})
        with open(self.filepath, 'w') as file:
            writer = csv.DictWriter(file, delimiter=';', fieldnames=[*result[0]])
            writer.writeheader()
            writer.writerows(result)

        return result

    def make_analyz(self) -> str:
        logger.info('Analyzing data started')
        analyz = self._add_raiting()
        raitings = [int(item['Rating']) for item in analyz]
        max_raiting = max(raitings)
        cities = [item['City'] for item in analyz if item['Rating'] == max_raiting]

        print(MSG_RCMND_CITIES.format(*cities))

        return MSG_RCMND_CITIES.format(*cities)
