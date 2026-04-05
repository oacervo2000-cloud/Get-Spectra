#!/usr/bin/env python3
"""
koa.py — Download de espectros HIRES do KOA (Keck Observatory Archive)
=======================================================================
Fonte  : Keck Observatory Archive (https://koa.ipac.caltech.edu/)
Acesso : TAP público (sem autenticação para dados públicos, > 18 meses)
Dados  : HIRES @ Keck I, R ≈ 48 000–67 000

API KOA
-------
  TAP endpoint: https://koa.ipac.caltech.edu/TAP/sync
  Tabela:       koa_instrument_tab  (ou koa_sp.hits, koa.koa_sp)
  Formato:      JSON, CSV, VOTABLE

  Consulta ADQL recomendada:
    SELECT koaid, filehand, targname, ra, dec, date_obs,
           elaptime, progid, camera, ech_ang, xd_ang, slit, snr
    FROM koa.koa_sp
    WHERE targname LIKE 'HD %'
      AND instrume = 'HIRES'
      AND date_obs < NOW() - INTERVAL '18' MONTH   (dados públicos)
    ORDER BY date_obs

  Download dos FITS:
    URL base: https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA?filehand=<filehand>
    Ou via endpoint de download: https://koa.ipac.caltech.edu/KoaAPI/v2/download?koaid=<koaid>

Referências:
  - https://koa.ipac.caltech.edu/UserGuide/
  - Vogt et al. (1994) — HIRES, SPIE 2198, 362
"""

import json
import logging
import time
import requests
from pathlib import Path

import config
from utils import (
    USER_AGENT,
    download_file,
    star_dir,
    update_index,
    already_downloaded,
    print_summary,
    hd_to_name,
    with_retry,
    format_filename,
    check_fits_snr_and_date,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_TAP_URL      = "https://koa.ipac.caltech.edu/TAP/sync"
_DOWNLOAD_URL = "https://koa.ipac.caltech.edu/cgi-bin/getKOA/nph-getKOA"

# KOA API v2 REST (substitui o CGI legado nph-koaSearch que retorna 404)
_KOA_API_V2   = "https://koa.ipac.caltech.edu/KoaAPI/v2"
_ALT_DL_URL   = "https://koa.ipac.caltech.edu/KoaAPI/v2/download"

# Tabelas TAP — tentadas em ordem; 'instrume' e 'instrument' variam por tabela
_KOA_TAP_TABLES = [
    ("koa_tap",              "instrume"),   # tabela real (confirmada no schema TAP)
    ("koa.koa_obs",          "instrume"),   # alternativa com schema explícito
    ("koa.koa_hires",        "instrume"),   # tabela HIRES específica (se existir)
    ("koa_instrument_tab",   "instrument"), # nome alternativo legado
]


# ---------------------------------------------------------------------------
# Query TAP
# ---------------------------------------------------------------------------

@with_retry(max_retries=3, wait=10, exceptions=(requests.RequestException, IOError))
def _tap_query(adql: str, timeout: int = 60) -> list[dict]:
    """
    Executa query ADQL no TAP do KOA e retorna lista de dicts.

    O TAP do KOA (IPAC) não suporta FORMAT=json. Tenta em ordem:
    1. FORMAT=csv    — mais leve e simples de parsear
    2. FORMAT=ipac_table — formato nativo IPAC
    """
    import csv, io

    for fmt in ("csv", "ipac_table"):
        params = {
            "QUERY":   adql,
            "FORMAT":  fmt,
            "REQUEST": "doQuery",
            "LANG":    "ADQL",
        }
        
        log.debug("KOA TAP (%s)", fmt)

        try:
            r = requests.get(_TAP_URL, params=params, headers={"User-Agent": USER_AGENT, "Accept": "*/*"}, timeout=timeout)
            r.raise_for_status()
            body = r.text
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 400 and fmt == "csv":
                continue   # tenta próximo formato
            raise

        if fmt == "csv":
            # CSV: primeira linha = cabeçalho
            reader = csv.DictReader(io.StringIO(body))
            return [row for row in reader]

        # ipac_table: parsing posicional
        return _parse_ipac_table(body)

    return []


def _parse_ipac_table(text: str) -> list[dict]:
    """
    Faz parsing de uma tabela IPAC (formato fixo com | separando colunas).
    Retorna lista de dicts.
    """
    lines = [l for l in text.split("\n") if l.strip()]
    header_lines = [l for l in lines if l.startswith("|")]
    data_lines   = [l for l in lines if l.startswith(" ") and not l.startswith("\\")]

    if not header_lines:
        return []

    # Primeira linha de | é o cabeçalho de colunas
    cols = [c.strip() for c in header_lines[0].split("|") if c.strip()]

    rows = []
    for line in data_lines:
        parts = [p.strip() for p in line.split("|") if True]
        # Re-split alinhado por posição usando as posições dos pipes no header
        pipe_positions = [i for i, c in enumerate(header_lines[0]) if c == "|"]
        vals = []
        for i in range(len(pipe_positions) - 1):
            start = pipe_positions[i] + 1
            end   = pipe_positions[i + 1]
            vals.append(line[start:end].strip() if end <= len(line) else "")
        if len(vals) == len(cols):
            rows.append(dict(zip(cols, vals)))

    return rows


@with_retry(max_retries=3, wait=10, exceptions=(requests.RequestException, ValueError, IOError))
def _search_api_v2(target: str, timeout: int = 30) -> list[dict]:
    """
    Usa a KOA API v2 REST para buscar observações HIRES.
    Retorna JSON com lista de observações.
    """
    params = {
        "instrument": "HIRES",
        "target":     target,
        "format":     "json",
    }
    url = f"{_KOA_API_V2}/search"
    log.debug("KOA API v2: %s", url)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept":     "application/json",
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    # A resposta pode ser lista direta ou {"results": [...]}
    if isinstance(data, list):
        return data
    return data.get("results", data.get("data", []))


def _query_hires(star_name: str) -> list[dict]:
    """
    Consulta espectros HIRES para uma estrela.
    Tenta primeiro a Search API (IPAC), depois TAP.
    """
    hd_num = star_name.replace("HD ", "").replace("HD", "").strip()
    variants = [
        star_name,          # "HD 10307"
        f"HD{hd_num}",      # "HD10307"
        hd_num,             # "10307"
    ]

    # 1) Tenta KOA API v2
    for name in variants:
        try:
            rows = _search_api_v2(name)
            if rows:
                log.debug("KOA API v2: %d resultados para '%s'", len(rows), name)
                return rows
        except Exception as exc:
            log.debug("KOA API v2 falhou para '%s': %s", name, exc)

    # 2) Fallback: TAP com múltiplas tabelas e variantes de nome de coluna
    for (table, inst_col) in _KOA_TAP_TABLES:
        for name in variants[:2]:
            like_name = f"%{name}%"
            adql = (
                f"SELECT koaid, filehand, targname, ra, dec, "
                f"date_obs, elaptime, progid "
                f"FROM {table} "
                f"WHERE targname LIKE '{like_name}' "
                f"  AND {inst_col} = 'HIRES' "
                f"ORDER BY date_obs"
            )
            try:
                rows = _tap_query(adql)
                if rows:
                    log.debug("KOA TAP (%s): %d resultados", table, len(rows))
                    return rows
            except Exception as exc:
                log.debug("KOA TAP %s falhou: %s", table, exc)

    return []


# ---------------------------------------------------------------------------
# URL de download
# ---------------------------------------------------------------------------

def _build_download_url(row: dict) -> str | None:
    """
    Constrói a URL de download do FITS a partir de um row do TAP.
    Tenta: koaid → KoaAPI/v2, filehand → nph-getKOA.
    """
    koaid    = row.get("koaid", "")
    filehand = row.get("filehand", "")

    from requests.utils import quote

    if koaid:
        return f"{_ALT_DL_URL}?koaid={quote(str(koaid))}"
    elif filehand:
        return f"{_DOWNLOAD_URL}?filehand={quote(str(filehand))}"

    return None


# ---------------------------------------------------------------------------
# Download de uma estrela
# ---------------------------------------------------------------------------

def _download_star(hd: str, hip: str) -> dict:
    """
    Baixa todos os espectros HIRES de uma estrela.
    """
    star_name = hd_to_name(hd)
    downloaded, skipped, failed = [], [], []

    rows = _query_hires(star_name)

    if not rows:
        log.info("HIRES: nenhum espectro para %s", star_name)
        return {"downloaded": downloaded, "skipped": skipped, "failed": failed}

    log.info("HIRES: %d espectros encontrados para %s", len(rows), star_name)
    dest_root = star_dir(config.SPECTRA_ROOT, "HIRES", hd)

    for row in rows:
        koaid    = str(row.get("koaid", "")).strip()
        date_obs = str(row.get("date_obs", ""))

        dl_url = _build_download_url(row)
        if not dl_url:
            log.warning("Sem URL de download para KOAID %s", koaid)
            failed.append(koaid or "unknown")
            continue

        dest = format_filename(hd, date_obs, dl_url, dest_root)
        fname = dest.name

        if already_downloaded(dest):
            skipped.append(fname)
            continue

        try:
            new = download_file(dl_url, dest, timeout=config.REQUEST_TIMEOUT)
            if new:
                snr, fits_date = check_fits_snr_and_date(dest)
                if snr is not None and snr < config.MIN_SNR:
                    log.info("KOA: arquivo descartado por SNR baixo (%.1f < %s): %s", snr, config.MIN_SNR, fname)
                    dest.unlink()
                    skipped.append(fname)
                    continue

                downloaded.append(fname)
                update_index(config.INDEX_FILE, {
                    "instrument": "HIRES",
                    "hd":         hd,
                    "hip":        hip,
                    "star_name":  star_name,
                    "filename":   fname,
                    "filepath":   str(dest),
                    "obs_date":   fits_date or date_obs,
                    "snr":        snr,
                    "exp_time":   str(row.get("elaptime", "")),
                    "source_url": dl_url,
                    "notes":      f"progid={row.get('progid', '')} camera={row.get('camera', '')}",
                })
            else:
                skipped.append(fname)
        except Exception as exc:
            log.error("Falha ao baixar %s: %s", fname, exc)
            failed.append(fname)

        time.sleep(0.5)

    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


# ---------------------------------------------------------------------------
# Ponto de entrada público
# ---------------------------------------------------------------------------

def run(targets) -> dict:
    """
    Baixa espectros HIRES (KOA) para todas as estrelas.

    Parâmetros
    ----------
    targets : pandas.DataFrame com colunas HD, HIP

    Retorno
    -------
    dict com listas 'downloaded', 'skipped', 'failed'
    """
    log.info("═" * 55)
    log.info("Iniciando download HIRES/KOA  (%d estrelas)", len(targets))
    log.info("═" * 55)

    all_dl, all_skip, all_fail = [], [], []

    for _, row in targets.iterrows():
        hd  = str(row["HD"]).strip()
        hip = str(row["HIP"]).strip()
        star = f"HD {hd}"

        log.info("── %s ──", star)

        try:
            result = _download_star(hd, hip)
        except Exception as exc:
            log.error("Falha inesperada no processamento de %s: %s", star, exc)
            result = {"downloaded": [], "skipped": [], "failed": [star]}

        n_dl   = len(result["downloaded"])
        n_skip = len(result["skipped"])
        n_fail = len(result["failed"])

        log.info("   %s: %d novos | %d já existiam | %d falhas",
                 star, n_dl, n_skip, n_fail)

        all_dl   += [f"{star}/{f}" for f in result["downloaded"]]
        all_skip += [f"{star}/{f}" for f in result["skipped"]]
        all_fail += [f"{star}/{f}" for f in result["failed"]]

        if n_dl > 0 or n_fail > 0:
            time.sleep(config.SLEEP_BETWEEN)

    totals = {"instrument": "HIRES",
              "downloaded": all_dl,
              "skipped":    all_skip,
              "failed":     all_fail}
    print_summary(totals)
    return totals


# ---------------------------------------------------------------------------
# Execução direta
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent))

    import config
    from utils import setup_logging, load_targets

    setup_logging(config.LOG_FILE)
    targets = load_targets(config.TARGETS_FILE)
    run(targets)
