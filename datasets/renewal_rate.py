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
    countif(event = 'SECTOR_EXPIRED') as expired_count,
    countif(event = 'SECTOR_EXTENDED') as extended_count,
    round(
        100.0 * countif(event = 'SECTOR_EXTENDED')
            / nullif(countif(event = 'SECTOR_EXPIRED') + countif(event = 'SECTOR_EXTENDED'), 0),
        2
    ) as renewal_rate_pct
from `lily-data.lily.sector_events`
where height > 4000000
  and event in ('SECTOR_EXPIRED', 'SECTOR_EXTENDED')
group by 1
order by 1 desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
