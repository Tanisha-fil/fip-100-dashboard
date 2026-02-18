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
-- For each miner, take their last-seen power state each day (latest height).
-- Sum across all miners to get total network power per day.
-- Diff day-over-day to get daily net power added (can be negative on termination days).
-- Dividing by pow(1024,5) converts bytes to PiB.
with ranked as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        miner_id,
        cast(raw_byte_power as float64) as raw_byte_power,
        cast(quality_adj_power as float64) as quality_adj_power,
        row_number() over (
            partition by
                datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day),
                miner_id
            order by height desc
        ) as rn
    from `lily-data.lily.power_actor_claims`
    where height > 4000000
),
network_daily as (
    select
        date,
        sum(raw_byte_power) as total_raw_byte_power,
        sum(quality_adj_power) as total_quality_adj_power
    from ranked
    where rn = 1
    group by 1
)
select
    date,
    (total_raw_byte_power - lag(total_raw_byte_power) over (order by date)) / pow(1024, 5) as raw_power_added,
    (total_quality_adj_power - lag(total_quality_adj_power) over (order by date)) / pow(1024, 5) as quality_adjusted_power_added
from network_daily
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
