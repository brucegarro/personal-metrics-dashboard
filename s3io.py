import os, io, gzip, json, hashlib
from datetime import datetime, timezone
import boto3

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