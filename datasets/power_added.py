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
-- power_actor_claims is change-driven: a miner only has a row on days their power changed.
-- To get stable total-network-power per day we must fill-forward each miner's last known
-- power to days they are absent, then sum across all miners, then diff day-over-day.
with activity as (
    -- Last known power per miner per day (only days they had activity)
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        miner_id,
        cast(raw_byte_power     as float64) as raw_byte_power,
        cast(quality_adj_power  as float64) as quality_adj_power,
        row_number() over (
            partition by
                datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day),
                miner_id
            order by height desc
        ) as rn
    from `lily-data.lily.power_actor_claims`
    where height > 4000000
),
daily_activity as (
    select date, miner_id, raw_byte_power, quality_adj_power
    from activity
    where rn = 1
),
date_spine  as (select distinct date    from daily_activity),
miner_spine as (select distinct miner_id from daily_activity),
-- All date × miner combinations
all_combos as (
    select d.date, m.miner_id
    from date_spine d cross join miner_spine m
),
-- Fill-forward: carry each miner's last known power through subsequent days
filled as (
    select
        a.date,
        a.miner_id,
        last_value(da.raw_byte_power    ignore nulls) over (
            partition by a.miner_id order by a.date
            rows between unbounded preceding and current row
        ) as raw_byte_power,
        last_value(da.quality_adj_power ignore nulls) over (
            partition by a.miner_id order by a.date
            rows between unbounded preceding and current row
        ) as quality_adj_power
    from all_combos a
    left join daily_activity da on a.date = da.date and a.miner_id = da.miner_id
),
network_daily as (
    select
        date,
        sum(coalesce(raw_byte_power,    0)) as total_raw,
        sum(coalesce(quality_adj_power, 0)) as total_qap
    from filled
    group by 1
)
select
    date,
    (total_raw - lag(total_raw) over (order by date)) / pow(1024, 5) as raw_power_added,
    (total_qap - lag(total_qap) over (order by date)) / pow(1024, 5) as quality_adjusted_power_added
from network_daily
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
