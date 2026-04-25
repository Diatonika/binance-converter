from click import group

from baikal.binance_converter.klines.command import save_klines


@group()
def main() -> None:
    pass


main.add_command(save_klines)

main()
