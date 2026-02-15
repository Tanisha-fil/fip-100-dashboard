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
with per_message as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        array_length(json_extract_array(params, '$.Sectors')) as sector_count
    from `lily-data.lily.parsed_messages`
    where height > 4000000
      and method = 'PreCommitSectorBatch2'
)

select
    date,
    countif(sector_count < 4) as small_batch_count,
    count(*) as total_batch_count,
    round(100.0 * countif(sector_count < 4) / count(*), 2) as small_batch_pct
from per_message
group by date
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
