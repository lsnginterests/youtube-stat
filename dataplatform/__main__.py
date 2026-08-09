import argparse
import sys
import time
from datetime import datetime

from dataplatform.registry import BY_NAME, LAYERS, Pipeline, graph, layer, order, with_deps
from dataplatform.setting.spark_session import get_spark


def ingestion_date(value: str) -> str:
    datetime.strptime(value, '%Y-%m-%d')
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='python -m dataplatform',
        description='Run lakehouse pipelines in dependency order.'
    )
    subparsers = parser.add_subparsers(dest='command', required=True, metavar='COMMAND')

    subparsers.add_parser('list', help='show pipelines in execution order')

    run_parser = subparsers.add_parser('run', help='run one or more pipelines')
    run_parser.add_argument('names', nargs='+', choices=list(BY_NAME), metavar='PIPELINE')
    run_parser.add_argument('--with-deps', action='store_true', help='run upstream pipelines first')

    layer_parser = subparsers.add_parser('run-layer', help='run every pipeline of a layer')
    layer_parser.add_argument('layer', choices=list(LAYERS), metavar='LAYER')
    layer_parser.add_argument('--with-deps', action='store_true', help='run upstream layers first')

    all_parser = subparsers.add_parser('run-all', help='run every pipeline')

    for command in (run_parser, layer_parser, all_parser):
        command.set_defaults(parser=command)
        command.add_argument('--date', type=ingestion_date, metavar='YYYY-MM-DD',
                             help='ingestion date, applies to bronze pipelines only')
        command.add_argument('--dry-run', action='store_true',
                             help='print the execution plan without starting Spark')
    return parser


def select(args: argparse.Namespace) -> list[str]:
    if args.command == 'run':
        return with_deps(args.names) if args.with_deps else order(args.names)
    if args.command == 'run-layer':
        names = layer(args.layer)
        return with_deps(names) if args.with_deps else names
    return order()


def show_list() -> None:
    names = order()
    width = max(len(name) for name in names)
    dependencies = graph()
    for position, name in enumerate(names, 1):
        after = ', '.join(sorted(dependencies[name])) or '-'
        date_flag = ' [--date]' if BY_NAME[name].takes_date else ''
        print(f'{position:2}. {name:{width}}{date_flag}  after: {after}')


def show_plan(names: list[str]) -> None:
    for position, name in enumerate(names, 1):
        print(f'{position:2}. {name}')


def execute(names: list[str], date: str | None) -> None:
    spark = get_spark()
    try:
        for position, name in enumerate(names, 1):
            pipeline: Pipeline = BY_NAME[name]
            print(f'[{position}/{len(names)}] {name}', end='', flush=True)
            started = time.monotonic()
            if pipeline.takes_date:
                pipeline.run(date)
            else:
                pipeline.run()
            print(f' — {time.monotonic() - started:.1f} s', flush=True)
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == 'list':
        show_list()
        return 0

    names = select(args)
    if args.date and not any(BY_NAME[name].takes_date for name in names):
        args.parser.error('--date applies to bronze pipelines only, none of the selected accept it')

    if args.dry_run:
        show_plan(names)
        return 0

    execute(names, args.date)
    return 0


if __name__ == '__main__':
    sys.exit(main())
