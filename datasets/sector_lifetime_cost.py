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

# Approximate per-sector lifetime cost as:
# (onboarding gas cost per sector) + (daily_fee * avg_sector_lifetime_days)
# Since DailyFee from SectorOnChainInfo post-nv25 may not be in lily yet,
# we estimate from gas outputs alone as a lower bound.
sql = """
with sector_msgs as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        pm.cid,
        pm.method,
        case
            when pm.method = 'PreCommitSectorBatch2'
                then array_length(json_extract_array(pm.params, '$.Sectors'))
            when pm.method in ('ProveCommitSectors3', 'ProveReplicaUpdates3')
                then array_length(json_extract_array(pm.params, '$.Sectors'))
            when pm.method = 'ProveCommitAggregate'
                then cast(json_extract_scalar(pm.params, '$.AggregateSize') as int64)
            else 1
        end as sector_count
    from `lily-data.lily.parsed_messages` pm
    where pm.height > 4000000
      and pm.method in (
          'PreCommitSectorBatch2',
          'ProveCommitSectors3',
          'ProveCommitAggregate',
          'ProveReplicaUpdates3'
      )
),

gas as (
    select
        cid,
        (base_fee_burn + over_estimation_burn) / 1e18 as fee_fil
    from `lily-data.lily.derived_gas_outputs`
    where height > 4000000
)

select
    s.date,
    avg(safe_divide(g.fee_fil, s.sector_count)) as avg_sector_lifetime_cost_fil
from sector_msgs s
join gas g on s.cid = g.cid
where s.sector_count > 0
group by s.date
order by s.date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
