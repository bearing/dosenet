from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Tuple, Union
from typing_extensions import Protocol

class SensorData(Enum):
    AQ = AQSensorData,
    CO2 = CO2SensorData,
    D3S = D3SSensorData,
    PG = PGSensorData,
    Weather = WeatherSensorData


class CSVAble(Protocol):
    def to_csv(self) -> str: ...

@dataclass(frozen=True)
class WeatherSensorData:
    unix_time: int
    local_time: datetime
    utc_time: datetime
    temperature: float
    pressure: float
    humidity: float

    def to_csv(self) -> str:
        return f'{self.utc_time},{self.local_time},{self.unix_time},{self.temperature},{self.pressure},{self.humidity}\n'


@dataclass(frozen=True)
class D3SSensorData:
    unix_time: int
    local_time: datetime
    utc_time: datetime
    cpm: float
    cpm_error: float
    keV_per_channel: float
    readings: Tuple[int]

    def to_csv(self) -> str:
        string = f'{self.utc_time},{self.local_time},{self.unix_time},{self.cpm},{self.cpm_error},{self.keV_per_channel}'
        for reading in self.readings:
            string += f',{reading}'
        string += '\n'

        return string


@dataclass(frozen=True)
class PGSensorData:
    unix_time: int
    local_time: datetime
    utc_time: datetime
    cpm: float
    cpm_error: float

    def to_csv(self) -> str:
        return f'{self.utc_time},{self.local_time},{self.unix_time},{self.cpm},{self.cpm_error}\n'


@dataclass(frozen=True)
class AQSensorData:
    unix_time: int
    local_time: datetime
    utc_time: datetime
    pm_1: float
    pm_25: float
    pm_10: float

    def to_csv(self) -> str:
        return f'{self.utc_time},{self.local_time},{self.unix_time},{self.pm_1},{self.pm_25},{self.pm_10}\n'


@dataclass(frozen=True)
class CO2SensorData:
    unix_time: int
    local_time: datetime
    utc_time: datetime
    co2_ppm: float
    noise: float

    def to_csv(self) -> str:
        return f'{self.utc_time},{self.local_time},{self.unix_time},{self.co2_ppm},{self.noise}\n'
