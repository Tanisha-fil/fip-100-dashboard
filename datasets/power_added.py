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
--
-- Bug fix: miners whose power was established before height 4000000 had no baseline,
-- so their full historical power appeared as a spike on day 1 of the analysis window.
-- We fix this by seeding each miner's initial state from their last known power at or
-- before height 4000000 and injecting it as a virtual anchor entry on the day before
-- the analysis window opens.
with activity as (
    -- Last known power per miner per day (only days they had activity in-window)
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
-- Seed: last known power per miner at or before the window boundary.
-- Only fetch miners that actually appear in the analysis window (inner join)
-- to keep the cross-join bounded.
seed_raw as (
    select
        p.miner_id,
        cast(p.raw_byte_power    as float64) as raw_byte_power,
        cast(p.quality_adj_power as float64) as quality_adj_power,
        row_number() over (partition by p.miner_id order by p.height desc) as rn
    from `lily-data.lily.power_actor_claims` p
    inner join (select distinct miner_id from daily_activity) m using (miner_id)
    where p.height <= 4000000
),
seed_latest as (
    select miner_id, raw_byte_power, quality_adj_power
    from seed_raw
    where rn = 1
),
-- Virtual anchor row: inject seed power as an entry on the day before the window opens.
-- The fill-forward carries these values into day 1, so the first real diff is correct.
seed_anchor as (
    select
        date_sub((select min(date) from daily_activity), interval 1 day) as date,
        s.miner_id,
        s.raw_byte_power,
        s.quality_adj_power
    from seed_latest s
),
-- Merge the anchor entries with actual in-window activity
all_activity as (
    select date, miner_id, raw_byte_power, quality_adj_power from daily_activity
    union all
    select date, miner_id, raw_byte_power, quality_adj_power from seed_anchor
),
date_spine  as (select distinct date    from all_activity),
miner_spine as (select distinct miner_id from all_activity),
-- All date × miner combinations (includes the anchor date)
all_combos as (
    select d.date, m.miner_id
    from date_spine d cross join miner_spine m
),
-- Fill-forward: carry each miner's last known power through subsequent days
filled as (
    select
        a.date,
        a.miner_id,
        last_value(aa.raw_byte_power    ignore nulls) over (
            partition by a.miner_id order by a.date
            rows between unbounded preceding and current row
        ) as raw_byte_power,
        last_value(aa.quality_adj_power ignore nulls) over (
            partition by a.miner_id order by a.date
            rows between unbounded preceding and current row
        ) as quality_adj_power
    from all_combos a
    left join all_activity aa on a.date = aa.date and a.miner_id = aa.miner_id
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
-- Exclude the synthetic anchor day; it exists only to seed the fill-forward
where date >= (select min(date) from daily_activity)
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

# Apply 7-day trailing rolling mean to smooth single-day spikes caused by
# timing/reporting artifacts in power_actor_claims (e.g. large power changes
# split across day boundaries). The chart subtitle reflects this smoothing.
df = (
    pl.DataFrame(data)
    .with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))
    .sort("date")
    .with_columns([
        pl.col("raw_power_added")
            .rolling_mean(window_size=7, min_samples=1)
            .alias("raw_power_added"),
        pl.col("quality_adjusted_power_added")
            .rolling_mean(window_size=7, min_samples=1)
            .alias("quality_adjusted_power_added"),
    ])
    .sort("date", descending=True)
)

df.write_json(f"public/{Path(__file__).stem}.json")
