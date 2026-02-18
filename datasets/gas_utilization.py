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
-- Theoretical max onboarding rate = Filecoin network gas capacity / avg gas per sector.
-- Network capacity: 10B gas/tipset * 2880 tipsets/day = 28.8T gas/day (constant).
-- Avg gas per sector derived from PreCommitSectorBatch2 gas_used / sector count.
with daily_gas as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        round(100.0 * sum(gas_used) / nullif(sum(gas_limit), 0), 4) as gas_utilization_pct
    from `lily-data.lily.derived_gas_outputs`
    where height > 4000000
    group by 1
),
sector_gas as (
    select
        datetime_trunc(timestamp_seconds(((pm.height * 30) + 1598306400)), day) as date,
        safe_divide(
            sum(cast(g.gas_used as float64)),
            sum(array_length(json_extract_array(pm.params, '$.Sectors')))
        ) as avg_gas_used_per_sector
    from `lily-data.lily.parsed_messages` pm
    join `lily-data.lily.derived_gas_outputs` g on pm.cid = g.cid
    where pm.height > 4000000
      and pm.method = 'PreCommitSectorBatch2'
      and array_length(json_extract_array(pm.params, '$.Sectors')) > 0
    group by 1
)
select
    d.date,
    d.gas_utilization_pct,
    round(safe_divide(28800000000000.0, s.avg_gas_used_per_sector), 0) as theoretical_max_sectors_per_day
from daily_gas d
left join sector_gas s on d.date = s.date
order by d.date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
