import base64
import json
import os
from pathlib import Path

import polars as pl
from google.cloud import bigquery
from google.oauth2 import service_account

creds_json = base64.b64decode(
    os.environ["ENCODED_GOOGLE_APPLICATION_CREDENTIALS"]
).decode()

info = json.loads(creds_json)
creds = service_account.Credentials.from_service_account_info(info)
client = bigquery.Client(credentials=creds, project=info["project_id"])

sql = """
-- Use miner_sector_events to count individual sector extensions, not messages.
-- A single ExtendSectorExpiration message can extend hundreds of sectors;
-- SECTOR_EXTENDED events give one row per sector.
with daily as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        count(*) as extensions_per_day
    from `lily-data.lily.miner_sector_events`
    where height > 4000000
      and event = 'SECTOR_EXTENDED'
    group by 1
)

select
    date,
    extensions_per_day,
    avg(extensions_per_day) over (
        order by date
        rows between 29 preceding and current row
    ) as baseline_rolling_30d
from daily
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
