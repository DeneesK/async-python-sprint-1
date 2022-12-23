from pydantic import BaseModel


class WheatherHourModel(BaseModel):
    hour: str
    temp: int
    condition: str


class WheatherDateModel(BaseModel):
    date: str
    hours: list[WheatherHourModel]


class WheatherForecastModel(BaseModel):
    forecasts: list[WheatherDateModel]


class CityForecastModel(BaseModel):
    city: str
    forecasts: WheatherForecastModel


class CalculatedDataModel(BaseModel):
    date: str
    avg_temp: float | None
    good_hours: int


class CalculatedCityModel(BaseModel):
    city: str
    dates: list[CalculatedDataModel]
