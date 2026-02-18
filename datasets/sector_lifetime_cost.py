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
      and pm.method = 'ProveCommitSectors3'
    group by 1, 2, 3
)"""
    event_join = """
left join event_sector_counts esc
  on s.date = esc.date
 and s.cid = esc.cid
 and s.method = esc.method
"""
    sector_count_expr = "coalesce(s.param_sector_count, esc.event_sector_count)"

# Per-sector onboarding gas cost = avg PreCommitSectorBatch2 gas/sector
#                                 + avg ProveCommitSectors3 gas/sector
# Split by SP archetype proxied by batch size:
#   large SP  >= 10 sectors/batch
#   medium SP  4-9 sectors/batch
#   small SP   1-3 sectors/batch
sql = f"""
with sector_msgs as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        pm.cid,
        pm.method,
        case
            when pm.method = 'PreCommitSectorBatch2'
                then array_length(json_extract_array(pm.params, '$.Sectors'))
            when pm.method = 'ProveCommitSectors3'
                then coalesce(
                    array_length(json_extract_array(pm.params, '$.SectorNumbers')),
                    array_length(json_extract_array(pm.params, '$.SectorProofs')),
                    array_length(json_extract_array(pm.params, '$.Sectors'))
                )
            else 1
        end as param_sector_count
    from `lily-data.lily.parsed_messages` pm
    where pm.height > 4000000
      and pm.method in ('PreCommitSectorBatch2', 'ProveCommitSectors3')
){event_cte},
combined as (
    select
        s.date,
        s.cid,
        s.method,
        {sector_count_expr} as sector_count
    from sector_msgs s
    {event_join}
),
gas as (
    select
        cid,
        (cast(base_fee_burn as float64) + cast(over_estimation_burn as float64)) / 1e9 as fee_nanofil
    from `lily-data.lily.derived_gas_outputs`
    where height > 4000000
),
by_method_archetype as (
    select
        s.date,
        s.method,
        case
            when s.sector_count >= 10 then 'large_sp'
            when s.sector_count >= 4  then 'medium_sp'
            else 'small_sp'
        end as sp_archetype,
        avg(safe_divide(g.fee_nanofil, s.sector_count)) as avg_gas_per_sector
    from combined s
    join gas g on s.cid = g.cid
    where s.sector_count > 0
    group by s.date, s.method, sp_archetype
)
select
    date,
    sp_archetype,
    sum(avg_gas_per_sector) as avg_sector_lifetime_cost_nanofil
from by_method_archetype
group by date, sp_archetype
order by date desc, sp_archetype
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

pivoted = df.pivot(
    values="avg_sector_lifetime_cost_nanofil",
    index="date",
    on="sp_archetype",
    aggregate_function="mean",
).sort("date", descending=True)

pivoted.write_json(f"public/{Path(__file__).stem}.json")
