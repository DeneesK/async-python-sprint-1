from multiprocessing import Process, Queue
from typing import Iterable
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from pydantic import parse_obj_as

from api_client import YandexWeatherAPI
from utils import MAX_WKRS, MSG_RCMND_CITIES
from models import (
    WheatherForecastModel,
    CityForecastModel,
    WheatherDateModel,
    CalculatedCityModel,
    CalculatedDataModel
)


class DataFetchingTask:
    def __init__(self, wheather_api: YandexWeatherAPI, cities: Iterable):
        self.wheather_api = wheather_api
        self.cities = cities

    def _run(self) -> Iterable:
        with ThreadPoolExecutor(max_workers=MAX_WKRS) as pool:
            data = pool.map(self.wheather_api.get_forecasting, self.cities)
        return data

    def get_data(self) -> list[CityForecastModel]:
        resp = self._run()
        result = [
            (x[0], parse_obj_as(WheatherForecastModel, x[1]))
            for x in resp
        ]
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
        temperatures = [x.temp for x in hours if int(x.hour) > 8 and int(x.hour) < 20]
        if not len(temperatures):
            return None
        avg_temp = round((sum(temperatures) / len(temperatures)), 1)
        return avg_temp

    def _calc_good_cond_hours(self, data: WheatherDateModel) -> int:
        hours = data.hours
        sum_good_hours = sum([1 for x in hours if all((self._check_condition(x.condition),
                                                      int(x.hour) > 8,
                                                      int(x.hour) < 20)
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
        for data in self.weather_data:
            item = self._prepare_data(data)
            self.queue.put(item)


class DataAggregationTask(Process):
    def __init__(self, queue: Queue):
        super().__init__()
        self.queue = queue
        self.df_list = []

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
        while True:
            if self.queue.empty():
                print("Очередь пуста")
                pd.DataFrame.from_dict(self.df_list).to_excel('forecast_table.xlsx', index=False)  # type: ignore
                break
            else:
                data = self.queue.get()
                item = self._agregate_data(data)
                self.df_list.append(item)


class DataAnalyzingTask:
    def __init__(self, filepath):
        self.filepath = filepath

    def _prepare_to_analyz(self):
        df = pd.read_excel(self.filepath, index_col=None)
        data = df.to_dict(orient='records')
        return data

    def _calc_rating(self, item: dict) -> int:
        raiting = []
        for k in [*item]:
            if item[k][0].isalpha():
                continue
            raiting.extend(item[k].split('/'))
        del raiting[0]
        raiting = list(map(lambda n: float(n), raiting))
        return int(sum(raiting) / len(raiting))

    def _add_raiting(self) -> list[dict]:
        result = []
        data = self._prepare_to_analyz()
        for d in data:
            r = self._calc_rating(d)
            result.append(d | {'Rating': r})
        pd.DataFrame.from_dict(result).to_excel('forecast_table.xlsx', index=False)  # type: ignore
        return result

    def make_analyz(self) -> None:
        analyz = self._add_raiting()
        raitings = [int(item['Rating']) for item in analyz]
        max_raiting = max(raitings)
        cities = [item['City'] for item in analyz if item['Rating'] == max_raiting]
        print(MSG_RCMND_CITIES.format(*cities))
