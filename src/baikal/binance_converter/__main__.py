from click import group

from baikal.binance_converter.download.command import download
from baikal.binance_converter.funding_rate.command import save_funding_rate
from baikal.binance_converter.klines.command import save_klines
from baikal.binance_converter.metrics.command import save_metrics


@group()
def main() -> None:
    pass


main.add_command(download)
main.add_command(save_funding_rate)
main.add_command(save_klines)
main.add_command(save_metrics)

main()
