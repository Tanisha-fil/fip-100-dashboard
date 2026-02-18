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

# Per-sector onboarding gas cost = avg PreCommitSectorBatch2 gas/sector
#                                 + avg ProveCommitSectors3 gas/sector
# Summing the two averages approximates the total two-message cost per sector.
# This is the gas-only component; post-nv25 the FIP-100 DailyFee adds a recurring
# fee on top (see sector_daily_fee_cost.py).
sql = """
with sector_msgs as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        pm.cid,
        pm.method,
        case
            when pm.method = 'PreCommitSectorBatch2'
                then array_length(json_extract_array(pm.params, '$.Sectors'))
            when pm.method = 'ProveCommitSectors3'
                then array_length(json_extract_array(pm.params, '$.Sectors'))
            else 1
        end as sector_count
    from `lily-data.lily.parsed_messages` pm
    where pm.height > 4000000
      and pm.method in ('PreCommitSectorBatch2', 'ProveCommitSectors3')
),

gas as (
    select
        cid,
        (cast(base_fee_burn as float64) + cast(over_estimation_burn as float64)) / 1e9 as fee_nanofil
    from `lily-data.lily.derived_gas_outputs`
    where height > 4000000
),

by_method as (
    select
        s.date,
        s.method,
        avg(safe_divide(g.fee_nanofil, s.sector_count)) as avg_gas_per_sector
    from sector_msgs s
    join gas g on s.cid = g.cid
    where s.sector_count > 0
    group by s.date, s.method
)

select
    date,
    sum(avg_gas_per_sector) as avg_sector_lifetime_cost_nanofil
from by_method
group by date
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
