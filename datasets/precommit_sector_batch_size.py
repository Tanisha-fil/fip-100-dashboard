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
event_agg_expr = "a.param_agg_size"

if event_cid_col:
    event_cte = f""",
aggregate_event_sizes as (
    select
        datetime_trunc(timestamp_seconds(((mse.height * 30) + 1598306400)), day) as date,
        pm.cid,
        count(*) as event_agg_size
    from `lily-data.lily.miner_sector_events` mse
    join `lily-data.lily.parsed_messages` pm
      on pm.cid = mse.{event_cid_col}
    where mse.height > 4000000
      and mse.event = 'SECTOR_ADDED'
      and pm.height > 4000000
      and pm.method = 'ProveCommitAggregate'
    group by 1, 2
)"""
    event_join = """
left join aggregate_event_sizes aes
  on a.date = aes.date
 and a.cid = aes.cid
"""
    event_agg_expr = "coalesce(a.param_agg_size, aes.event_agg_size)"

sql = f"""
with precommit as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        array_length(json_extract_array(params, '$.Sectors')) as sector_count
    from `lily-data.lily.parsed_messages`
    where height > 4000000
      and method = 'PreCommitSectorBatch2'
),
aggregate_base as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        cid,
        array_length(json_extract_array(params, '$.SectorNumbers')) as param_agg_size
    from `lily-data.lily.parsed_messages`
    where height > 4000000
      and method = 'ProveCommitAggregate'
){event_cte},
aggregate as (
    select
        a.date,
        {event_agg_expr} as agg_size
    from aggregate_base a
    {event_join}
),
precommit_daily as (
    select
        date,
        avg(sector_count) as avg_sectors_per_message,
        approx_quantiles(sector_count, 2)[OFFSET(1)] as median_sectors_per_message,
        count(*) as messages
    from precommit
    group by date
),
aggregate_sized_daily as (
    select
        date,
        avg(agg_size) as avg_aggregate_size,
        approx_quantiles(agg_size, 2)[OFFSET(1)] as median_aggregate_size
    from aggregate
    where agg_size is not null and agg_size > 0
    group by date
),
aggregate_messages_daily as (
    select
        date,
        count(*) as aggregate_messages
    from aggregate
    group by date
)
select
    p.date,
    p.avg_sectors_per_message,
    p.median_sectors_per_message,
    p.messages,
    a.avg_aggregate_size,
    a.median_aggregate_size,
    m.aggregate_messages
from precommit_daily p
left join aggregate_sized_daily a on p.date = a.date
left join aggregate_messages_daily m on p.date = m.date
order by p.date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
