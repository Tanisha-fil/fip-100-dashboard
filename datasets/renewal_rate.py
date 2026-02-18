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
with events as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        event,
        case
            when height < 4878840 then 'pre'
            when height < 5138040 then 'during'
            else 'post'
        end as grace_phase
    from `lily-data.lily.miner_sector_events`
    where height > 4000000
      and event in ('SECTOR_EXPIRED', 'SECTOR_EXTENDED')
)
select
    date,
    countif(event = 'SECTOR_EXPIRED')  as expired_count,
    countif(event = 'SECTOR_EXTENDED') as extended_count,
    round(
        100.0 * countif(event = 'SECTOR_EXTENDED')
            / nullif(countif(event = 'SECTOR_EXPIRED') + countif(event = 'SECTOR_EXTENDED'), 0),
        2
    ) as renewal_rate_pct,
    -- Per-phase renewal rate (null outside each window for chart segmentation)
    round(
        100.0 * countif(event = 'SECTOR_EXTENDED' and grace_phase = 'pre')
            / nullif(countif(grace_phase = 'pre'), 0),
        2
    ) as renewal_rate_pre_pct,
    round(
        100.0 * countif(event = 'SECTOR_EXTENDED' and grace_phase = 'during')
            / nullif(countif(grace_phase = 'during'), 0),
        2
    ) as renewal_rate_during_pct,
    round(
        100.0 * countif(event = 'SECTOR_EXTENDED' and grace_phase = 'post')
            / nullif(countif(grace_phase = 'post'), 0),
        2
    ) as renewal_rate_post_pct
from events
group by 1
order by 1 desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
