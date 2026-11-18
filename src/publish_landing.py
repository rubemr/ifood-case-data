"""Publica uma versão validada da fonte em Unity Catalog Volumes usando a CLI com OAuth.

Os objetos de versão existentes são conferidos byte a byte e nunca sobrescritos.
O ponteiro ativo é publicado por último. Este script não lê credenciais.
"""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile

from download_sources import sha256_file, validate_parquet


def cli(*arguments):
    result = subprocess.run(["databricks", *arguments, "--profile", "ifood-case"],
                            check=True, capture_output=True, text=True, encoding="utf-8")
    return result.stdout


def publish(month: str, root: Path) -> dict:
    active_path = root / month / "active.json"
    active = json.loads(active_path.read_text(encoding="utf-8"))
    file = root / active["file_relative_path"]
    manifest_path = root / active["manifest_relative_path"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if sha256_file(file) != active["sha256"] or active["sha256"] != manifest["sha256"]:
        raise ValueError("Local file/active/manifest checksums disagree")
    details = validate_parquet(file)
    if details["row_count"] != manifest["row_count"] or file.stat().st_size != manifest["byte_count"]:
        raise ValueError("Local file and manifest counts disagree")
    remote_root = "dbfs:/Volumes/workspace/ifood_landing/raw"
    remote_version = f"{remote_root}/{month}/{active['sha256']}"
    cli("fs", "mkdir", remote_version)
    existing = json.loads(cli("fs", "ls", remote_version, "--output", "json"))
    names = {item.get("name", item.get("path", "")).rsplit("/", 1)[-1] for item in existing}
    Path(".tmp").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=".tmp") as folder:
        for local in (file, manifest_path):
            remote = f"{remote_version}/{local.name}"
            if local.name not in names:
                cli("fs", "cp", str(local), remote)
            returned = Path(folder) / local.name
            cli("fs", "cp", remote, str(returned))
            if sha256_file(local) != sha256_file(returned):
                raise ValueError(f"Remote checksum mismatch: {remote}")
    cli("fs", "cp", str(active_path), f"{remote_root}/{month}/active.json", "--overwrite")
    return {"source_file_month": month, "remote_file": f"{remote_version}/{file.name}",
            "remote_manifest": f"{remote_version}/manifest.json",
            "sha256": active["sha256"], "row_count": manifest["row_count"],
            "byte_count": manifest["byte_count"],
            "remote_verification": "downloaded_and_sha256_matched"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--month", default="2023-01", choices=[f"2023-{m:02d}" for m in range(1, 6)])
    parser.add_argument("--root", type=Path, default=Path("data/landing"))
    args = parser.parse_args()
    print(json.dumps(publish(args.month, args.root)))
