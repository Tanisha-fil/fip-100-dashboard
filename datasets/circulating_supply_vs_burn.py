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
        avg(cast(circulating_fil as float64)) / 1e18 as circulating_fil,
        avg(cast(burnt_fil as float64)) / 1e18 as burnt_fil
    from `lily-data.lily.chain_economics`
    where height > 4000000
    group by 1
)
select
    date,
    circulating_fil,
    burnt_fil,
    -- day-over-day % change in circulating supply
    round(100.0 * safe_divide(
        circulating_fil - lag(circulating_fil) over (order by date),
        lag(circulating_fil) over (order by date)
    ), 6) as circulating_growth_pct,
    -- daily new burn as % of current circulating supply
    round(100.0 * safe_divide(
        burnt_fil - lag(burnt_fil) over (order by date),
        circulating_fil
    ), 6) as daily_burn_rate_pct
from daily
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
