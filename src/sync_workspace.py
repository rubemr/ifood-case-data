"""Sincroniza apenas código-fonte, documentação e testes deste case como arquivos do workspace.

Não inclui dados, credenciais, caches, backups nem ambientes virtuais locais.
"""
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
TARGET = "/Shared/ifood-case-data"
DIRECTORIES = ("src", "analysis", "tests")
EXTENSIONS = {".py", ".md", ".json", ".csv", ".png"}


def main():
    files = [ROOT / name for name in ("README.md", "requirements.txt", ".gitignore")]
    if (ROOT / "requirements.lock.txt").exists():
        files.append(ROOT / "requirements.lock.txt")
    for directory in DIRECTORIES:
        files.extend(path for path in (ROOT / directory).rglob("*")
                     if path.is_file() and path.suffix in EXTENSIONS
                     and "__pycache__" not in path.parts)
    created = set()
    for path in sorted(files):
        remote = f"{TARGET}/{path.relative_to(ROOT).as_posix()}"
        parent = remote.rsplit("/", 1)[0]
        if parent not in created:
            subprocess.run(["databricks", "workspace", "mkdirs", parent,
                            "--profile", "ifood-case"], check=True)
            created.add(parent)
        subprocess.run(["databricks", "workspace", "import", remote,
                        "--format", "RAW", "--file", str(path), "--overwrite",
                        "--profile", "ifood-case"], check=True)
    # Mantém sincronizados os notebooks executáveis e seus arquivos de código-fonte originais.
    notebooks = [path for path in (ROOT / "src").glob("*.py")
                 if path.read_text(encoding="utf-8").startswith("# Databricks notebook source")]
    if notebooks:
        subprocess.run(["databricks", "workspace", "mkdirs", f"{TARGET}/notebooks",
                        "--profile", "ifood-case"], check=True)
    for path in notebooks:
        subprocess.run(["databricks", "workspace", "import", f"{TARGET}/notebooks/{path.stem}",
                        "--format", "SOURCE", "--language", "PYTHON", "--file", str(path),
                        "--overwrite", "--profile", "ifood-case"], check=True)
    print(f"Synced {len(files)} case files to {TARGET}")
    print(f"Synced {len(notebooks)} executable case notebooks")


if __name__ == "__main__":
    main()
