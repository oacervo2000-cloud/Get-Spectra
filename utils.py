#!/usr/bin/env python3
"""
utils.py — Utilitários compartilhados do spectra_downloader
============================================================
Funções usadas por todos os módulos de download:
  - Logging configurável (arquivo + console)
  - Carregamento da lista de alvos (lubin2010.csv)
  - Normalização de nomes de estrelas
  - Decorator de retry para downloads frágeis
  - Criação de diretórios
  - Atualização do INDEX_MASTER.csv
  - Resolução de nomes via SIMBAD (HD → nome canônico + coords)
"""

import os
import csv
import time
import logging
import functools
import json
import requests
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(log_file: str, level: int = logging.INFO) -> logging.Logger:
    """
    Configura o logger raiz com dois handlers:
      - FileHandler  → log_file (tudo, incluindo DEBUG)
      - StreamHandler → console (nível escolhido, padrão INFO)
    """
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    fmt = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Evita duplicação ao chamar setup_logging mais de uma vez
    if root.handlers:
        return root

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt, datefmt))

    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(logging.Formatter(fmt, datefmt))

    root.addHandler(fh)
    root.addHandler(ch)

    return root


# ---------------------------------------------------------------------------
# Carregamento de alvos
# ---------------------------------------------------------------------------

def load_targets(csv_path: str) -> pd.DataFrame:
    """
    Lê o arquivo CSV de alvos e retorna um DataFrame.
    Detecta automaticamente o separador (vírgula ou ponto-e-vírgula).
    Garante que as colunas HD e HIP estejam como strings limpas.
    """
    # Detecta separador lendo a primeira linha
    with open(csv_path, "r", encoding="utf-8") as f:
        first_line = f.readline()
    sep = ";" if first_line.count(";") > first_line.count(",") else ","

    df = pd.read_csv(csv_path, sep=sep, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    if "HD" not in df.columns:
        raise KeyError(
            f"Coluna 'HD' não encontrada. Colunas disponíveis: {list(df.columns)}\n"
            f"  Arquivo: {csv_path}\n"
            f"  Separador detectado: '{sep}'"
        )

    df["HD"]  = df["HD"].str.strip()
    df["HIP"] = df["HIP"].str.strip() if "HIP" in df.columns else ""
    return df


# ---------------------------------------------------------------------------
# Normalização de nomes
# ---------------------------------------------------------------------------

def hd_to_name(hd: str) -> str:
    """Converte número HD (string ou int) para nome canônico: 'HD 10307'."""
    return f"HD {str(hd).strip()}"


def hip_to_name(hip: str) -> str:
    """Converte número HIP para nome canônico: 'HIP 7918'."""
    return f"HIP {str(hip).strip()}"


def star_dir(spectra_root: str, instrument: str, hd: str) -> Path:
    """
    Retorna (e cria se necessário) o diretório de saída para uma estrela:
      <spectra_root>/<INSTRUMENT>/HD_<hd>/
    Exemplo: /home/rafael/SPECTRA/HARPS/HD_10307/
    """
    p = Path(spectra_root) / instrument.upper() / f"HD_{hd}"
    p.mkdir(parents=True, exist_ok=True)
    return p


def format_filename(hd: str, obs_date: str, source_url: str, dest_dir: Path) -> Path:
    """
    Formata o nome do espectro na padronização NomeDoObjeto_DataDaObservacao.fits.
    Utiliza tempo YYYYMMDD_HHMMSS quando possível. Se conflitar o nome, um sufixo numérico
    é incrementado. A idempotência é garantida checando o source_url no cache do INDEX_MASTER.
    """
    import re
    if source_url:
        cached_name = get_filename_from_index(source_url)
        if cached_name:
            return dest_dir / cached_name
            
    clean_name = f"HD{str(hd).strip()}"
    
    try:
        from astropy.time import Time
        try:
            val = float(obs_date)
            dt = Time(val, format='mjd').datetime
        except ValueError:
            dt = Time(obs_date).datetime
        date_str = dt.strftime("%Y%m%d_%H%M%S")
    except Exception:
        date_str = re.sub(r"[^0-9]", "", str(obs_date))
        if not date_str:
            date_str = "00000000"
            
    base_name = f"{clean_name}_{date_str}"
    target_path = dest_dir / f"{base_name}.fits"
    
    counter = 1
    while target_path.exists():
        target_path = dest_dir / f"{base_name}_{counter}.fits"
        counter += 1
        
    return target_path


def check_fits_snr_and_date(fits_path: Path):
    """
    Lê o cabeçalho FITS de maneira rápida para extrair SNR e a Data.
    Retorna (snr_value, date_obs_str).
    Útil para SOPHIE e módulos que omitem esses dados no JSON/HTML,
    ou validação a posteriori.
    """
    try:
        from astropy.io import fits
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with fits.open(fits_path, ignore_missing_end=True, memmap=True) as hdul:
                hdr = hdul[0].header
                
                snr = None
                for kw in ["SNR", "S_N", "SN", "SIGNAL", "SN_RATIO", "SIG2NOIS", "S_NOISE"]:
                    if kw in hdr:
                        try:
                            snr = float(hdr[kw])
                            break
                        except (ValueError, TypeError):
                            pass
                
                date_obs = hdr.get("DATE-OBS") or hdr.get("DATE") or hdr.get("OBS_DATE")
                return snr, date_obs
    except Exception as exc:
        log = logging.getLogger(__name__)
        log.debug("check_fits_snr_and_date falhou em %s: %s", fits_path.name, exc)
        return None, None


# ---------------------------------------------------------------------------
# Retry decorator
# ---------------------------------------------------------------------------

def with_retry(max_retries: int = 5, wait: int = 10, exceptions=(Exception,)):
    """
    Decorator que re-tenta a função em caso de exceção.

    Uso:
        @with_retry(max_retries=5, wait=10)
        def download(url, dest):
            ...
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            log = logging.getLogger(func.__module__)
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt == max_retries:
                        log.error("Falha definitiva após %d tentativas: %s", max_retries, exc)
                        raise
                    log.warning(
                        "Tentativa %d/%d falhou (%s). Aguardando %ds...",
                        attempt, max_retries, exc, wait
                    )
                    time.sleep(wait)
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# INDEX_MASTER.csv
# ---------------------------------------------------------------------------

INDEX_COLUMNS = [
    "timestamp",
    "instrument",
    "hd",
    "hip",
    "star_name",
    "filename",
    "filepath",
    "obs_date",
    "snr",
    "exp_time",
    "rv_kms",
    "source_url",
    "notes",
]

_INDEX_CACHE = None

def _load_index_cache() -> None:
    global _INDEX_CACHE
    _INDEX_CACHE = {}
    import config
    idx_file = Path(config.INDEX_FILE)
    if not idx_file.exists() or idx_file.stat().st_size == 0:
        return
    try:
        with open(idx_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                src = row.get("source_url")
                fname = row.get("filename")
                if src and fname:
                    _INDEX_CACHE[src] = fname
    except Exception as e:
        log = logging.getLogger(__name__)
        log.warning("Erro ao ler INDEX_MASTER.csv para cache: %s", e)


def get_filename_from_index(source_url: str) -> str | None:
    if _INDEX_CACHE is None:
        _load_index_cache()
    return _INDEX_CACHE.get(source_url)


def update_index(index_file: str, record: dict) -> None:
    """
    Adiciona uma linha ao INDEX_MASTER.csv.
    Cria o arquivo com cabeçalho se ainda não existir.
    O dict `record` pode ter qualquer subconjunto de INDEX_COLUMNS.
    """
    index_file = Path(index_file)
    index_file.parent.mkdir(parents=True, exist_ok=True)

    write_header = not index_file.exists() or index_file.stat().st_size == 0

    record.setdefault("timestamp", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))

    with open(index_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=INDEX_COLUMNS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(record)
        
    if _INDEX_CACHE is not None:
        src = record.get("source_url")
        fname = record.get("filename")
        if src and fname:
            _INDEX_CACHE[src] = fname


# ---------------------------------------------------------------------------
# Verificação de arquivo já baixado
# ---------------------------------------------------------------------------

def already_downloaded(dest_path: Path) -> bool:
    """Retorna True se o arquivo existir e tiver tamanho > 0."""
    return dest_path.exists() and dest_path.stat().st_size > 0


# ---------------------------------------------------------------------------
# Download simples com retry e User-Agent
# ---------------------------------------------------------------------------

USER_AGENT = "spectra_downloader/2.0 (gpefit.ura@iftm.edu.br; research)"


@with_retry(max_retries=5, wait=15, exceptions=(requests.RequestException, IOError))
def download_file(url: str, dest_path: Path, timeout: int = 60) -> bool:
    """
    Baixa `url` para `dest_path` usando requests.
    Retorna True em sucesso. Lança exceção em falha definitiva.
    """
    log = logging.getLogger(__name__)

    if already_downloaded(dest_path):
        log.debug("Já existe: %s — pulando.", dest_path.name)
        return False   # False = não baixou (já tinha)

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest_path.with_suffix(dest_path.suffix + ".tmp")

    try:
        with requests.get(url, stream=True, timeout=timeout, headers={"User-Agent": USER_AGENT}) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):   # 64 kB
                    if chunk:
                        f.write(chunk)
    except requests.RequestException as exc:
        if tmp.exists():
            tmp.unlink()
        raise exc

    tmp.rename(dest_path)
    log.info("Baixado: %s  (%d kB)", dest_path.name, dest_path.stat().st_size // 1024)
    return True   # True = novo arquivo baixado


# ---------------------------------------------------------------------------
# Resolução de nomes via SIMBAD TAP
# ---------------------------------------------------------------------------

_SIMBAD_TAP = "https://simbad.u-strasbg.fr/simbad/sim-tap/sync"


def simbad_resolve(name: str, timeout: int = 20) -> dict | None:
    """
    Consulta SIMBAD para obter: main_id, ra, dec, sp_type via ADQL.
    Retorna dict ou None em caso de falha.

    Exemplo de retorno:
        {"main_id": "HD  10307", "ra": 25.878, "dec": 16.998, "sp_type": "G2V"}
    """
    log = logging.getLogger(__name__)

    query = (
        f"SELECT TOP 1 main_id, ra, dec, sp_type "
        f"FROM basic "
        f"JOIN ident ON oidref = oid "
        f"WHERE id = '{name}'"
    )
    params = {
        "REQUEST": "doQuery",
        "LANG": "ADQL",
        "FORMAT": "json",
        "QUERY": query,
    }
    try:
        r = requests.get(_SIMBAD_TAP, params=params, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        cols = [c["name"] for c in data["metadata"]]
        rows = data["data"]
        if not rows:
            log.warning("SIMBAD: nenhum resultado para '%s'", name)
            return None
        return dict(zip(cols, rows[0]))
    except requests.RequestException as exc:
        log.warning("SIMBAD lookup falhou (Rede) para '%s': %s", name, exc)
        return None
    except ValueError as exc:
        log.warning("SIMBAD lookup falhou (JSON/Parsing) para '%s': %s", name, exc)
        return None


# ---------------------------------------------------------------------------
# Sumário de progresso
# ---------------------------------------------------------------------------

def print_summary(results: dict) -> None:
    """
    Imprime sumário de uma execução de módulo.
    `results` deve ter chave 'instrument' e listas 'downloaded', 'skipped', 'failed'.
    """
    inst  = results.get("instrument", "?")
    dl    = results.get("downloaded", [])
    skip  = results.get("skipped", [])
    fail  = results.get("failed", [])
    total = len(dl) + len(skip) + len(fail)

    GRN = "\033[92m"; YLW = "\033[93m"; RED = "\033[91m"
    BLD = "\033[1m";  RST = "\033[0m"

    print(f"\n{BLD}{'─'*55}{RST}")
    print(f"{BLD}  Resumo — {inst}{RST}")
    print(f"{'─'*55}")
    print(f"  {GRN}Novos downloads : {len(dl):>4}{RST}")
    print(f"  {YLW}Já existiam     : {len(skip):>4}{RST}")
    print(f"  {RED}Falhas          : {len(fail):>4}{RST}")
    print(f"  {'Total alvos    ':>18}: {total:>4}")
    print(f"{'─'*55}\n")

    if fail:
        print(f"{RED}  Estrelas com falha:{RST}")
        for s in fail:
            print(f"    • {s}")
        print()
