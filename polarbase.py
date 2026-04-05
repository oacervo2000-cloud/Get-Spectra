#!/usr/bin/env python3
"""
polarbase.py — Download de espectros NARVAL e ESPaDOnS do PolarBase
====================================================================
Fonte  : PolarBase — IRAP/OMP (https://polarbase.irap.omp.eu/)
Acesso : REST API pública (sem autenticação)
Dados  : NARVAL (Télescope Bernard Lyot, 2006–) e ESPaDOnS (CFHT, 2005–)
Resolving power: R ≈ 65 000

API PolarBase v2
----------------
  Base URL: https://polarbase.irap.omp.eu/api/v2/

  Endpoints relevantes:
    GET /targets/            → lista todos os alvos
    GET /targets/?name=X     → busca por nome
    GET /spectra/            → lista espectros (com filtros)
    GET /spectra/?target=ID  → espectros de um alvo específico

  Parâmetros de /spectra/:
    target   = ID numérico do alvo (do endpoint /targets/)
    inst     = "NARVAL" | "ESPaDOnS"
    type     = "S" (Stokes I) | "V" (Stokes V)
    page_size= número de resultados por página
    page     = paginação

  Cada espectro tem:
    id, target, obs_date, instrument, type, snr, rv, download_url (URL do FITS)

  ⚠ A API usa paginação (count, next, previous, results).
    Este módulo percorre todas as páginas automaticamente.

Referência: Petit et al. (2014), PASP 126, 469
           (https://doi.org/10.1086/675976)
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
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dependências do Requests tratarão o SSL automaticamente,
# e usaremos verify=False como fallback no _api_get se falhar.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_API_BASE   = "https://polarbase.irap.omp.eu/api/v2"
_PAGE_SIZE  = 200     # resultados por página (máximo aceito pela API)
_STOKES_I   = "S"     # espectros de intensidade (usados para S-index)
_STOKES_V   = "V"     # espectros de polarimetria (opcional)

# Instrumentos que queremos baixar (podem ser filtrados em config.py no futuro)
_INSTRUMENTS = ("NARVAL", "ESPaDOnS")


# ---------------------------------------------------------------------------
# Funções de acesso à API
# ---------------------------------------------------------------------------

@with_retry(max_retries=4, wait=10, exceptions=(requests.RequestException, ValueError, IOError))
def _api_get(endpoint: str, params: dict = None, timeout: int = 30) -> dict:
    """
    Faz GET em _API_BASE/<endpoint> passando `params` e retorna JSON como dict.
    """
    url = f"{_API_BASE}/{endpoint.lstrip('/')}/"
    log.debug("API GET: %s", url)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept":     "application/json",
    }
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.SSLError as exc:
        log.warning("PolarBase: SSL falhou (%s) — retentando sem verificação.", exc)
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        r = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)
        r.raise_for_status()
        return r.json()


def _find_target_id(hd: str) -> int | None:
    """
    Busca o ID interno do PolarBase para uma estrela pelo nome 'HD XXXXX'.
    Tenta também variantes (HD+XXXXX, sem espaço, etc.).
    Retorna o ID inteiro ou None se não encontrado.
    """
    variants = [
        f"HD {hd}",
        f"HD{hd}",
        f"HD  {hd}",
        hd_to_name(hd),
    ]
    for name in variants:
        try:
            data = _api_get("targets", {"name": name, "page_size": 10})
            results = data.get("results", [])
            if results:
                target_id = results[0]["id"]
                log.debug("PolarBase target: '%s' → ID %s", name, target_id)
                return target_id
        except Exception as exc:
            log.debug("Falha na busca por '%s': %s", name, exc)

    log.warning("PolarBase: alvo HD %s não encontrado.", hd)
    return None


def _list_spectra_for_target(target_id: int,
                              instruments: tuple = _INSTRUMENTS,
                              stokes_types: tuple = (_STOKES_I,)) -> list[dict]:
    """
    Retorna lista com todos os espectros (todas páginas) para um dado alvo.
    Cada item: dict com campos da API (id, obs_date, instrument, snr, rv, url_fits, ...).
    """
    all_spectra = []

    for inst in instruments:
        for stype in stokes_types:
            page = 1
            while True:
                params = {
                    "target":    target_id,
                    "inst":      inst,
                    "type":      stype,
                    "page_size": _PAGE_SIZE,
                    "page":      page,
                }
                try:
                    data = _api_get("spectra", params)
                except Exception as exc:
                    log.warning("Erro na listagem (inst=%s, type=%s, page=%d): %s",
                                inst, stype, page, exc)
                    break

                results = data.get("results", [])
                all_spectra.extend(results)

                if not data.get("next"):
                    break
                page += 1

    log.debug("PolarBase: %d espectros encontrados para ID %d", len(all_spectra), target_id)
    return all_spectra


# ---------------------------------------------------------------------------
# Download de uma estrela
# ---------------------------------------------------------------------------

def _download_star(hd: str, hip: str) -> dict:
    """
    Baixa todos os espectros PolarBase (NARVAL + ESPaDOnS) de uma estrela.
    """
    downloaded, skipped, failed = [], [], []

    target_id = _find_target_id(hd)
    if target_id is None:
        log.info("HD %s: sem dados no PolarBase.", hd)
        return {"downloaded": downloaded, "skipped": skipped, "failed": failed}

    spectra = _list_spectra_for_target(target_id)

    for spec in spectra:
        # Filtro de SNR
        snr_val = spec.get("snr")
        if snr_val is not None:
            try:
                if float(snr_val) < config.MIN_SNR:
                    log.info("HD %s ignorado: SNR %.1f < Mínimo (%s)", hd, float(snr_val), config.MIN_SNR)
                    # skipped.append("low_snr")  # Opicional: não poluir skip se for apenas ignorado
                    continue
            except ValueError:
                pass

        # Determina instrumento e pasta de destino
        inst     = str(spec.get("instrument", "POLARBASE")).upper()
        if "ESPADONS" in inst or "ESPADON" in inst:
            inst = "ESPaDOnS"
        elif "NARVAL" in inst:
            inst = "NARVAL"

        dest_root = star_dir(config.SPECTRA_ROOT, inst, hd)

        fits_url = spec.get("url_fits") or spec.get("download_url") or spec.get("fits_file")
        if not fits_url:
            log.warning("Espectro ID %s de HD %s sem URL de download.", spec.get("id"), hd)
            continue

        obs_date = str(spec.get("obs_date", ""))
        dest = format_filename(hd, obs_date, fits_url, dest_root)
        fname = dest.name

        if already_downloaded(dest):
            skipped.append(fname)
            continue

        try:
            new = download_file(fits_url, dest, timeout=config.REQUEST_TIMEOUT)
            if new:
                downloaded.append(fname)
                update_index(config.INDEX_FILE, {
                    "instrument": inst,
                    "hd":         hd,
                    "hip":        hip,
                    "star_name":  hd_to_name(hd),
                    "filename":   fname,
                    "filepath":   str(dest),
                    "obs_date":   spec.get("obs_date", ""),
                    "snr":        spec.get("snr", ""),
                    "rv_kms":     spec.get("rv", ""),
                    "source_url": fits_url,
                    "notes":      f"stokes={spec.get('type', '?')}",
                })
            else:
                skipped.append(fname)
        except Exception as exc:
            log.error("Falha ao baixar %s de HD %s: %s", fname, hd, exc)
            failed.append(fname)

        time.sleep(0.3)

    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


# ---------------------------------------------------------------------------
# Ponto de entrada público
# ---------------------------------------------------------------------------

def run(targets) -> dict:
    """
    Baixa espectros PolarBase (NARVAL + ESPaDOnS) para todas as estrelas.

    Parâmetros
    ----------
    targets : pandas.DataFrame com colunas HD, HIP

    Retorno
    -------
    dict com listas 'downloaded', 'skipped', 'failed'
    """
    log.info("═" * 55)
    log.info("Iniciando download PolarBase  (%d estrelas)", len(targets))
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

    totals = {"instrument": "POLARBASE (NARVAL+ESPaDOnS)",
              "downloaded": all_dl,
              "skipped":    all_skip,
              "failed":     all_fail}
    print_summary(totals)
    return totals


# ---------------------------------------------------------------------------
# Teste rápido da API (diagnóstico)
# ---------------------------------------------------------------------------

def check_api() -> bool:
    """
    Faz uma consulta simples à API PolarBase para verificar conectividade.
    Retorna True se OK, False se falha.
    """
    try:
        data = _api_get("targets", {"name": "HD 10307", "page_size": 1})
        count = data.get("count", 0)
        log.info("PolarBase API OK — HD 10307 → %d resultado(s)", count)
        return True
    except Exception as exc:
        log.error("PolarBase API inacessível: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Execução direta
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent))

    import config
    from utils import setup_logging, load_targets

    setup_logging(config.LOG_FILE)

    if "--check" in sys.argv:
        ok = check_api()
        sys.exit(0 if ok else 1)

    targets = load_targets(config.TARGETS_FILE)
    run(targets)
