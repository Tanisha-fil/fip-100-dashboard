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

# nv25 height ~= (2025-04-07 - 2020-08-24) * 2880 epochs/day
# 2020-08-24 00:00 UTC = epoch 0 (genesis), but offset is 1598306400
# 2025-04-07 = approx height 4360320
sql = """
with gas as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        height,
        method,
        (cast(base_fee_burn as float64) + cast(over_estimation_burn as float64)) / 1e18 as fee_fil
    from `lily-data.lily.derived_gas_outputs`
    where height > 4000000
)

select
    date,
    sum(fee_fil) as total_fee_burn_fil,
    sum(case
        when method in (
            'PreCommitSectorBatch2',
            'ProveCommitSectors3',
            'ProveCommitAggregate',
            'ProveReplicaUpdates3',
            'PreCommitSector'
        ) then fee_fil
        else 0
    end) as sector_fee_burn_fil
from gas
group by date
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
