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
with precommit as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        array_length(json_extract_array(params, '$.Sectors')) as sector_count
    from `lily-data.lily.parsed_messages`
    where height > 4000000
      and method = 'PreCommitSectorBatch2'
),
aggregate as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        cast(json_extract_scalar(params, '$.AggregateSize') as int64) as agg_size
    from `lily-data.lily.parsed_messages`
    where height > 4000000
      and method = 'ProveCommitAggregate'
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
aggregate_daily as (
    select
        date,
        avg(agg_size) as avg_aggregate_size,
        approx_quantiles(agg_size, 2)[OFFSET(1)] as median_aggregate_size,
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
    a.aggregate_messages
from precommit_daily p
left join aggregate_daily a on p.date = a.date
order by p.date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
