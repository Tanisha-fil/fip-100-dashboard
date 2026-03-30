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

# DDO detection methodology adapted from Starboard's notebook.
#
# A sector is DDO (Direct Data Onboarding) if none of its Pieces notify the
# StorageMarket actor (f05). Non-DDO sectors have at least one piece with
# Notify[0].Address = 'f05' (the traditional StorageMarket path).
#
# Data source: lily-data.lily.parsed_messages, method = ProveCommitSectors3.
# Each element of params.SectorActivations represents one sector.
# CC sectors (Pieces = null) are counted as DDO since they don't go through f05.
#
# Chain-state philosophy (per Starboard): use miner_sector_events as primary
# source for counts where possible; params used here because DDO detection
# specifically requires inspecting the Notify field, which is only in params.

sql = """
with base as (
    select
        datetime_trunc(timestamp_seconds(((height * 30) + 1598306400)), day) as date,
        params
    from `lily-data.lily.parsed_messages`
    where height > 4000000
      and method = 'ProveCommitSectors3'
),
per_sector as (
    select
        date,
        -- A sector is non-DDO if at least one of its pieces calls the StorageMarket
        -- actor (f05) via Notify. CC sectors have Pieces=null and are counted as DDO.
        coalesce(
            (
                select countif(json_value(piece, '$.Notify[0].Address') = 'f05') > 0
                from unnest(json_query_array(sector, '$.Pieces')) as piece
            ),
            false
        ) as calls_f05
    from base,
    unnest(json_query_array(params, '$.SectorActivations')) as sector
)
select
    date,
    countif(not calls_f05) as ddo_sectors,
    countif(calls_f05) as non_ddo_sectors,
    count(*) as total_pcs3_sectors
from per_sector
group by date
order by date desc
"""

data = client.query(sql).to_arrow(create_bqstorage_client=False)

df = pl.DataFrame(data).with_columns(pl.col("date").dt.strftime("%Y-%m-%d"))

df.write_json(f"public/{Path(__file__).stem}.json")
