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
-- NV25 epoch: 4878840 (~2025-04-14)
-- Grace period end: 4878840 + 90*2880 = 5138040 (~2025-07-13)
-- SECTOR_EXTENDED events give one row per sector extended (not per message).
with events as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        case
            when height < 4878840 then 'pre'
            when height < 5138040 then 'during'
            else 'post'
        end as grace_phase
    from `lily-data.lily.miner_sector_events`
    where height > 4000000
      and event = 'SECTOR_EXTENDED'
),
daily as (
    select
        date,
        count(*) as extensions_per_day,
        countif(grace_phase = 'pre')    as extensions_pre_grace,
        countif(grace_phase = 'during') as extensions_during_grace,
        countif(grace_phase = 'post')   as extensions_post_grace
    from events
    group by 1
)
select
    date,
    extensions_per_day,
    nullif(extensions_pre_grace,    0) as extensions_pre_grace,
    nullif(extensions_during_grace, 0) as extensions_during_grace,
    nullif(extensions_post_grace,   0) as extensions_post_grace,
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
