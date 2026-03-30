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

cid_col_query = """
select column_name
from `lily-data.lily.INFORMATION_SCHEMA.COLUMNS`
where table_name = 'miner_sector_events'
"""
cid_columns = {row.column_name for row in client.query(cid_col_query).result()}
event_cid_col = next(
    (c for c in ("message_cid", "msg_cid", "cid", "message") if c in cid_columns),
    None,
)

event_cte = ""
event_join = ""
sector_count_expr = "s.param_sector_count"

if event_cid_col:
    event_cte = f""",
event_sector_counts as (
    select
        datetime_trunc(timestamp_seconds(((mse.height * 30) + 1598306400)), day) as date,
        pm.cid,
        pm.method,
        count(*) as event_sector_count
    from `lily-data.lily.miner_sector_events` mse
    join `lily-data.lily.parsed_messages` pm
      on pm.cid = mse.{event_cid_col}
    where mse.height > 4000000
      and mse.event = 'SECTOR_ADDED'
      and pm.height > 4000000
      and pm.method in ('ProveCommitSectors3', 'ProveCommitAggregate', 'ProveReplicaUpdates3')
    group by 1, 2, 3
)"""
    event_join = """
left join event_sector_counts esc
  on s.date = esc.date
 and s.cid = esc.cid
 and s.method = esc.method
"""
    # Use decoded params when available; otherwise fallback to sector-added events per message.
    sector_count_expr = "coalesce(s.param_sector_count, esc.event_sector_count)"

sql = f"""
with gas as (
    select
        cid,
        (cast(base_fee_burn as float64) + cast(over_estimation_burn as float64)) / 1e9 as total_gas_cost
    from `lily-data.lily.derived_gas_outputs`
    where height > 4000000
),

sectors_per_msg as (
    select
        datetime_trunc(timestamp_seconds(((pm.height * 30) + 1598306400)), day) as date,
        pm.cid,
        pm.method,
        case
            when pm.method = 'PreCommitSectorBatch2'
                then array_length(json_extract_array(pm.params, '$.Sectors'))
            when pm.method = 'ProveCommitSectors3'
                then array_length(json_extract_array(pm.params, '$.SectorActivations'))
            when pm.method = 'ProveReplicaUpdates3'
                then coalesce(
                    array_length(json_extract_array(pm.params, '$.SectorUpdates')),
                    array_length(json_extract_array(pm.params, '$.Sectors')),
                    array_length(json_extract_array(pm.params, '$.SectorNumbers'))
                )
            when pm.method = 'ProveCommitAggregate'
                then array_length(json_extract_array(pm.params, '$.SectorNumbers'))
            else 1
        end as param_sector_count
    from `lily-data.lily.parsed_messages` pm
    where pm.height > 4000000
      and pm.method in (
          'PreCommitSectorBatch2',
          'ProveCommitSectors3',
          'ProveCommitAggregate',
          'ProveReplicaUpdates3'
      )
){event_cte},

combined as (
    select
        s.date,
        s.cid,
        s.method,
        {sector_count_expr} as sector_count
    from sectors_per_msg s
    {event_join}
)

select
    c.date,
    c.method,
    avg(safe_divide(g.total_gas_cost, c.sector_count)) as avg_gas_per_sector
from combined c
join gas g on c.cid = g.cid
where c.sector_count > 0
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
