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

# nv25 ("Teep") activation epoch
NV25_EPOCH = 4878840

# For each post-nv25 sector, project total DailyFee over declared lifetime:
#   total_daily_fee_attoFIL = daily_fee * (expiration_epoch - activation_epoch) * 30 / 86400
# Convert attoFIL → nanoFIL by dividing by 1e9
sql = f"""
select
    date(timestamp_seconds(cast(activation_epoch * 30 + 1598306400 as int64))) as date,
    avg(
        safe_divide(
            cast(daily_fee as float64)
            * (cast(expiration_epoch as float64) - cast(activation_epoch as float64))
            * 30.0
            / 86400.0,
            1e9
        )
    ) as avg_daily_fee_lifetime_cost_nanofil,
    count(*) as sector_count
from `lily-data.lily.miner_sector_infos_v7`
where activation_epoch >= {NV25_EPOCH}
  and expiration_epoch > activation_epoch
  and daily_fee is not null
  and cast(daily_fee as float64) > 0
group by date
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
