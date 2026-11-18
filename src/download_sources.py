"""Baixa fontes Yellow Taxi da TLC preservando seus bytes originais.

Uso: python src/download_sources.py --months 2023-01
Os objetos finais são endereçados pelo conteúdo; active.json só é promovido após validação.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import time
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen
from uuid import uuid4

import pyarrow.parquet as pq

REQUIRED_COLUMNS = (
    "VendorID", "passenger_count", "total_amount",
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
)
SOURCE_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ALLOWED_MONTHS = {f"2023-{month:02d}" for month in range(1, 6)}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def validate_parquet(path: Path) -> dict:
    """Decodifica todas as colunas e lotes, além de verificar o rodapé."""
    # Gerencia o fechamento do arquivo mesmo se ParquetFile falhar ao ler bytes corrompidos.
    with path.open("rb") as stream, pq.ParquetFile(stream) as source:
        missing = sorted(set(REQUIRED_COLUMNS) - set(source.schema_arrow.names))
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        rows = sum(batch.num_rows for batch in source.iter_batches(batch_size=65536))
        if rows == 0 or rows != source.metadata.num_rows:
            raise ValueError("Empty Parquet or decoded row count differs from footer")
        return {
            "row_count": rows,
            "row_groups": source.metadata.num_row_groups,
            "columns": [{"name": field.name, "type": str(field.type)}
                        for field in source.schema_arrow],
            "validation": "all_columns_all_rows_decoded",
        }


def write_json_atomic(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.partial")
    try:
        temporary.write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n",
                             encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def download_month(month: str, root: Path, retries: int = 3) -> dict:
    if month not in ALLOWED_MONTHS:
        raise ValueError("Only January–May 2023 are in scope")
    run_id = uuid4().hex
    filename = f"yellow_tripdata_{month}.parquet"
    url = f"{SOURCE_BASE}/{filename}"
    staging = root / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    partial = staging / f"{filename}.{run_id}.partial"
    started_at = utc_now()
    try:
        # Busca a fonte novamente: uma URL estável pode receber bytes corrigidos.
        for attempt in range(1, retries + 1):
            try:
                request = Request(url, headers={"User-Agent": "ifood-case-data/1.0"})
                with urlopen(request, timeout=60) as response, partial.open("wb") as output:
                    content_length = response.headers.get("Content-Length")
                    etag = response.headers.get("ETag")
                    last_modified = response.headers.get("Last-Modified")
                    while block := response.read(1024 * 1024):
                        output.write(block)
                    output.flush()
                    os.fsync(output.fileno())
                if content_length is not None and partial.stat().st_size != int(content_length):
                    raise OSError("Downloaded size differs from Content-Length")
                break
            except (URLError, OSError, TimeoutError):
                if attempt == retries:
                    raise
                time.sleep(attempt)
        details = validate_parquet(partial)
        digest = sha256_file(partial)
        directory = root / month / digest
        directory.mkdir(parents=True, exist_ok=True)
        final = directory / filename
        manifest_path = directory / "manifest.json"
        reused = final.exists()
        if reused:
            if sha256_file(final) != digest:
                raise ValueError("Existing content-addressed object is corrupt")
        else:
            os.replace(partial, final)
        manifest = {
            "manifest_version": 1, "source_file_month": month,
            "source_file": filename, "source_url": url,
            "byte_count": final.stat().st_size, "sha256": digest,
            "download_started_at": started_at, "validated_at": utc_now(),
            "run_id": run_id, "status": "VALIDATED",
            "etag": etag, "last_modified": last_modified,
            **details,
        }
        # Preserva o primeiro manifesto de validação de cada versão imutável da fonte.
        if manifest_path.exists():
            saved = json.loads(manifest_path.read_text(encoding="utf-8"))
            for key in ("sha256", "byte_count", "row_count", "source_file_month"):
                if saved[key] != manifest[key]:
                    raise ValueError(f"Existing manifest mismatch: {key}")
            manifest = saved
        else:
            write_json_atomic(manifest_path, manifest)
        active = {
            "source_file_month": month, "sha256": digest,
            "manifest_relative_path": manifest_path.relative_to(root).as_posix(),
            "file_relative_path": final.relative_to(root).as_posix(),
            "activated_at": utc_now(), "validation_run_id": run_id,
        }
        write_json_atomic(root / month / "active.json", active)
        result = {"run_id": run_id, "status": "REUSED" if reused else "VALIDATED",
                  "source_file_month": month, "sha256": digest,
                  "byte_count": manifest["byte_count"], "row_count": manifest["row_count"],
                  "manifest": str(manifest_path), "file": str(final)}
        write_json_atomic(root / "runs" / f"{run_id}.json", result)
        return result
    except Exception as error:
        write_json_atomic(root / "runs" / f"{run_id}.json", {
            "run_id": run_id, "source_file_month": month, "source_url": url,
            "status": "FAILED", "failed_at": utc_now(),
            "error_type": type(error).__name__, "error": str(error),
        })
        raise
    finally:
        partial.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--months", nargs="+", default=["2023-01"],
                        choices=sorted(ALLOWED_MONTHS))
    parser.add_argument("--root", type=Path, default=Path("data/landing"))
    args = parser.parse_args()
    for month in args.months:
        print(json.dumps(download_month(month, args.root), ensure_ascii=False))


if __name__ == "__main__":
    main()
