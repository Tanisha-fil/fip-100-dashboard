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
-- chain_power records total network raw/QA power at each epoch.
-- We take the daily max-epoch snapshot then diff day-over-day to get daily additions.
-- Dividing by pow(1024,5) converts bytes to PiB.
with daily as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        max(cast(raw_byte_power as float64)) as raw_byte_power,
        max(cast(quality_adj_power as float64)) as quality_adj_power
    from `lily-data.lily.chain_power`
    where height > 4000000
    group by 1
)
select
    date,
    (raw_byte_power - lag(raw_byte_power) over (order by date)) / pow(1024, 5) as raw_power_added,
    (quality_adj_power - lag(quality_adj_power) over (order by date)) / pow(1024, 5) as quality_adjusted_power_added
from daily
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
