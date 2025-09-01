import os, io, gzip, json, hashlib
from datetime import datetime, timezone
import boto3
import polars as pl

BUCKET = os.getenv("S3_BUCKET")
ENV = os.getenv("ENV", "dev")

_session = boto3.session.Session()
_s3 = _session.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL") or None,
    aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    region_name=os.getenv("S3_REGION", "us-east-1"),
)


def write_jsonl_gz(records, vendor, api, endpoint, schema="v1"):
    now = datetime.now(timezone.utc)
    dt = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    batch_id = now.strftime("%Y%m%dT%H%M%SZ")

    prefix = (
        f"thirdparty/{vendor}/{api}/{ENV}/"
        f"zone=raw/endpoint={endpoint}/schema={schema}/dt={dt}/hour={hour}/"
    )

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        for r in records:
            r.setdefault("_ingested_at", now.isoformat())
            gz.write(json.dumps(r, separators=(',', ':')).encode() + b"\n")
    body = buf.getvalue()
    etag = hashlib.md5(body).hexdigest()

    data_key = f"{prefix}part={batch_id}-00001.jsonl.gz"
    meta_key = f"{prefix}part={batch_id}-00001.meta.json"

    _s3.put_object(
        Bucket=BUCKET,
        Key=data_key,
        Body=body,
        ContentType="application/json",
        ContentEncoding="gzip",
        Metadata={"zone": "raw", "endpoint": endpoint, "schema": schema},
    )
    meta = {
        "batch_id": batch_id,
        "endpoint": endpoint,
        "schema_version": schema,
        "record_count": len(records),
        "bytes_gz": len(body),
        "dt": dt,
        "hour": hour,
        "md5": etag,
    }
    _s3.put_object(
        Bucket=BUCKET,
        Key=meta_key,
        Body=json.dumps(meta).encode(),
        ContentType="application/json",
    )
    return {"data_key": data_key, "meta_key": meta_key}

def _list_ndjson_gz_keys(vendor: str, api: str, endpoint: str, date_str: str) -> list[str]:
    """List all .jsonl.gz parts for a given day under your raw layout."""
    prefix = (
        f"thirdparty/{vendor}/{api}/{ENV}/"
        f"zone=raw/endpoint={endpoint}/schema=v1/dt={date_str}/"
    )
    keys: list[str] = []
    token: str | None = None
    while True:
        if token is None:
            resp = _s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        else:
            resp = _s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, ContinuationToken=token)
        for obj in resp.get("Contents", []):
            k = obj["Key"]
            # keep only the data parts
            if k.endswith(".jsonl.gz") and "/part=" in k:
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def _load_ndjson_gz_as_polars(keys: list[str]) -> pl.DataFrame:
    """Fetch & decompress all keys, parse NDJSON lines, return a Polars DF."""
    rows: list[dict] = []
    for key in keys:
        obj = _s3.get_object(Bucket=BUCKET, Key=key)
        raw = obj["Body"].read()
        # try gzip; fall back to plain bytes just in case
        try:
            data = gzip.decompress(raw)
        except OSError:
            data = raw
        for line in data.splitlines():
            if line:
                rows.append(json.loads(line))
    if not rows:
        return pl.DataFrame()
    return pl.from_dicts(rows)