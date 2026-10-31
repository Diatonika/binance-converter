"""Options shared by the commands."""

from click import Choice, DateTime, option

from baikal.binance_converter.enums import InstrumentType

instrument_type_option = option(
    "--instrument-type",
    required=True,
    type=Choice(member.value for member in InstrumentType),
)

instrument_option = option("--instrument", required=True, type=str)

start_option = option(
    "--start", required=True, type=DateTime(), help="Interval start, inclusive (UTC)"
)

end_option = option(
    "--end", required=True, type=DateTime(), help="Interval end, exclusive (UTC)"
)
