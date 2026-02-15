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
with sector_msgs as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        cid,
        method
    from `lily-data.lily.parsed_messages`
    where height > 4000000
      and method in (
          'PreCommitSectorBatch2',
          'ProveCommitSectors3',
          'ProveCommitAggregate',
          'ProveReplicaUpdates3'
      )
),

gas as (
    select
        cid,
        (cast(base_fee_burn as float64) + cast(over_estimation_burn as float64)) / 1e9 as total_gas_cost
    from `lily-data.lily.derived_gas_outputs`
    where height > 4000000
),

sectors_per_msg as (
    select
        pm.height,
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
)

select
    datetime_trunc(timestamp_seconds(((s.height * 30) + 1598306400)), day) as date,
    s.method,
    avg(safe_divide(g.total_gas_cost, s.sector_count)) as avg_gas_per_sector
from sectors_per_msg s
join gas g on s.cid = g.cid
where s.sector_count > 0
group by 1, 2
order by 1 desc, 2
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

# Pivot so each method becomes a column, one row per date
pivoted = df.pivot(
    values="avg_gas_per_sector",
    index="date",
    on="method",
    aggregate_function="mean",
).sort("date", descending=True)

pivoted.write_json(f"public/{Path(__file__).stem}.json")
