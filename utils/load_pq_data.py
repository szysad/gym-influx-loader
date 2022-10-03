from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings, Point, WriteOptions
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
import pandas as pd
from typing import Generator, Tuple
import reactivex as rx
from reactivex import operators as ops


BUCKET_NAME = 'binance-v0.0'
DS_PATH = Path('/home/szysad/Projects/rl-projects/binance-history-retriever/data')


def point_generator(df: pd.DataFrame, measurement: str, base: str, quote: str) -> Generator[Point, None, None]:
    for dt, row in df.iterrows():
        p = Point(measurement) \
            .tag("base", base) \
            .tag("quote", quote) \
            .time(dt.to_pydatetime()) \

        for col in df.columns:
            p = p.field(col, getattr(row, col))        
                
        yield p

def parse_row(row: Tuple["pandas.Timestamp", "namedtuple"], measurement: str, base: str, quote: str, cols) -> Point:
    p = Point(measurement) \
            .tag("base", base) \
            .tag("quote", quote) \
            .time(row[0].to_pydatetime())

    for col in cols:
        p = p.field(col, getattr(row[1], col))        
            
    return p


def load_parquet_to_influx(pq_path: str, client: InfluxDBClient, bucket: str):

    base, quote = Path(pq_path).stem.split('-')

    pointSettings = PointSettings()
    pointSettings.add_default_tag("base", base)
    pointSettings.add_default_tag("quote", quote)

    write_api = client.write_api(
        point_settings=pointSettings,
        write_options=WriteOptions(batch_size=500_000, flush_interval=1000)
    )

    df = pq.read_table(pq_path).to_pandas()

    print("creating data points")
    dp_stream = rx \
        .from_iterable(df.iterrows()) \
        .pipe(ops.map(lambda r: parse_row(r, "kline", base, quote, df.columns)))
    print("finished creating data points")

    print("saving datapoints")
    write_api.write(
        bucket=bucket,
        record=dp_stream,
    )
    print("finished saving datapoints")


if __name__ == '__main__':
    with InfluxDBClient(url='http://localhost:8086', debug=True, token='local-token', org='rl.trading', timeout=1000 * 60 * 10) as cli:
        pq_files = list(DS_PATH.glob("*.parquet"))
        n_total = len(pq_files)
        for i, pq_file in enumerate(pq_files):
            t0 = datetime.now()
            print(f"({i} / {n_total}) starting to load {pq_file.name}")
            load_parquet_to_influx(pq_path=pq_file.as_posix(), client=cli, bucket=BUCKET_NAME)
            print(f"({i} / {n_total}) finished loading {pq_file.name}, done in {datetime.now() - t0}")
