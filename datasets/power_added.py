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
select
    datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
    sum(raw_byte_power / pow(1024,6)) as raw_power_added,
    sum(quality_adj_power / pow(1024,6)) as quality_adjusted_power_added,
from `lily-data.lily.power_actor_claims`
where height > 4000000
group by 1
order by 1 desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
