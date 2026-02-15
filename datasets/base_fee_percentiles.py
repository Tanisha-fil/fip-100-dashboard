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
with daily as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        cast(parent_base_fee as float64) as base_fee
    from `lily-data.lily.block_headers`
    where height > 4000000
)

select
    date,
    percentile_cont(base_fee, 0.5) over (partition by date) as base_fee_p50_nanofil,
    percentile_cont(base_fee, 0.95) over (partition by date) as base_fee_p95_nanofil
from daily
qualify row_number() over (partition by date order by base_fee) = 1
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
