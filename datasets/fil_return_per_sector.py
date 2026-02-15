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
with daily_rewards as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        max(cast(total_mined_reward as float64)) as cumulative_mined_reward
    from `lily-data.lily.chain_rewards`
    where height > 4000000
    group by 1
),

daily_rewards_delta as (
    select
        date,
        cumulative_mined_reward - lag(cumulative_mined_reward) over (order by date) as daily_reward_fil
    from daily_rewards
),

daily_sectors as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        count(*) as sectors_added
    from `lily-data.lily.power_actor_claims`
    where height > 4000000
      and raw_byte_power > 0
    group by 1
)

select
    r.date,
    safe_divide(r.daily_reward_fil, s.sectors_added) as fil_return_per_sector
from daily_rewards_delta r
join daily_sectors s on r.date = s.date
where r.daily_reward_fil > 0
  and s.sectors_added > 0
order by r.date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
