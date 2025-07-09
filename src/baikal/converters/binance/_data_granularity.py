import datetime

from enum import StrEnum

from dateutil.relativedelta import relativedelta


class BinanceDataGranularity(StrEnum):
    DAILY = "daily"
    MONTHLY = "monthly"

    def next_chunk(self, date_time: datetime.datetime) -> datetime.datetime:
        match self:
            case BinanceDataGranularity.DAILY:
                next_day = date_time.date() + relativedelta(days=1)
                return datetime.datetime(
                    next_day.year,
                    next_day.month,
                    next_day.day,
                    tzinfo=date_time.tzinfo,
                )

            case BinanceDataGranularity.MONTHLY:
                next_month = date_time.date() + relativedelta(months=1)
                return datetime.datetime(
                    next_month.year,
                    next_month.month,
                    1,
                    tzinfo=date_time.tzinfo,
                )

        error = f"Unsupported data granularity: {self}"
        raise ValueError(error)

    def file_date(self, date: datetime.date) -> str:
        match self:
            case BinanceDataGranularity.DAILY:
                return f"{date.year:02d}-{date.month:02d}-{date.day:02d}"
            case BinanceDataGranularity.MONTHLY:
                return f"{date.year:02d}-{date.month:02d}"

        error = f"Unsupported data granularity: {self}"
        raise ValueError(error)
