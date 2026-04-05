#!/usr/bin/env python3
"""
sophie.py — Download de espectros SOPHIE e ELODIE do OHP
=========================================================
Fonte  : Observatoire de Haute-Provence (OHP)
URL    : http://atlas.obs-hp.fr/sophie/   /   http://atlas.obs-hp.fr/elodie/
Acesso : sem autenticação (dados públicos)

Mecanismo real do OHP Atlas
----------------------------
O servidor não retorna links HTML para .fits. Em vez disso, a URL de listagem
com o parâmetro &z=d|wg|e faz o CGI gerar uma página de texto com comandos wget:

    wget "http://atlas.obs-hp.fr/sophie/sophie.cgi?c=i&a=mime:application/fits&o=sophie:[s1d,12345]" -O 12345_s1d.fits

Este módulo extrai as URLs da parte entre 'wget "' e '" -O' e baixa cada arquivo.
Isso espelha exatamente o que o notebook original GET_SOPHIE_SPECTRA_V2.ipynb fazia.
"""

import logging
import re
import time
import requests
from pathlib import Path

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
import config

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URLs de listagem — geram página com comandos wget embutidos
# (mantidos idênticos ao notebook original, apenas parametrizados)
# ---------------------------------------------------------------------------

# URL exata copiada do notebook original GET_SOPHIE_SPECTRA_V2.ipynb
# IMPORTANTE: ?c=i e =mime não são codificados — o servidor OHP exige isso literalmente
# para gerar múltiplas linhas wget (uma por espectro encontrado).
# Codificar ? como %3F ou = como %3D quebra o template SQL e retorna só 1 linha.
_SOPHIE_LIST_TMPL = (
    "http://atlas.obs-hp.fr/sophie/sophie.cgi"
    "?n=sophies&c=o&o={obj}"
    "&of=1,leda,simbad"
    "&sql=view_head%20IS%20NOT%20NULL"
    "&a=t&z=d|wg|e&ob=ra,seq"
    "&d=[%27wget%20%22http%3A//atlas%2Eobs-hp%2Efr/sophie/sophie%2Ecgi"
    "?c=i%26a=mime%3Aapplication/fits%26o=sophie%3A[s1d,%27||seq||%27]"
    "%22%20-O%20%27||seq||%27_s1d%2Efits%27]"
    "&nra=l,simbad,d"
)

# ELODIE: arquivo histórico (1993–2006), R≈42000
# O CGI é /elodie.cgi com n=elodie (sem sql=view_head — ELODIE não tem esse campo)
_ELODIE_LIST_TMPL = (
    "http://atlas.obs-hp.fr/elodie/elodie.cgi"
    "?n=elodie&c=o&o={obj}"
    "&of=1,leda,simbad"
    "&a=t&z=d|wg|e&ob=ra,seq"
    "&d=[%27wget%20%22http%3A//atlas%2Eobs-hp%2Efr/elodie/elodie%2Ecgi"
    "?c=i%26a=mime%3Aapplication/fits%26o=elodie%3A[s1d,%27||seq||%27]"
    "%22%20-O%20%27||seq||%27_s1d%2Efits%27]"
    "&nra=l,simbad,d"
)

# Padrão para extrair URL entre 'wget "' e '" -O'
_WGET_URL_RE = re.compile(r'wget\s+"(http[^"]+)"')

# Estrelas com SOPHIE problemático (cap7 da tese)
_SOPHIE_PROBLEMATIC = {"9407", "68168", "135101", "138573", "164595", "217014"}


# ---------------------------------------------------------------------------
# Extração de URLs da página OHP
# ---------------------------------------------------------------------------

@with_retry(max_retries=4, wait=15, exceptions=(requests.RequestException, IOError))
def _fetch_page(url: str, timeout: int = 30) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    r.raise_for_status()
    r.encoding = "utf-8"
    return r.text


def _extract_wget_urls(page_text: str) -> list[str]:
    """
    Extrai todas as URLs de download a partir das linhas wget na página.
    O notebook original pulava os primeiros 3 matches (são do cabeçalho da página).
    """
    urls = _WGET_URL_RE.findall(page_text)
    # Pula primeiros 3 matches que são do cabeçalho/navegação da página OHP
    return urls[3:] if len(urls) > 3 else urls


# ---------------------------------------------------------------------------
# Download de uma estrela
# ---------------------------------------------------------------------------

def _download_star(hd: str, hip: str, instrument: str) -> dict:
    obj = f"HD{hd}"
    tmpl = _SOPHIE_LIST_TMPL if instrument.upper() == "SOPHIE" else _ELODIE_LIST_TMPL
    url  = tmpl.format(obj=obj)

    log.debug("Listando %s: %s", obj, url[:80])
    try:
        page = _fetch_page(url)
    except Exception as exc:
        log.warning("HD %s: falha ao acessar OHP: %s", hd, exc)
        return {"downloaded": [], "skipped": [], "failed": [obj]}

    dl_urls = _extract_wget_urls(page)
    log.debug("HD %s: %d URLs encontradas na página", hd, len(dl_urls))

    if not dl_urls:
        log.info("HD %s: nenhum espectro encontrado no %s", hd, instrument)
        return {"downloaded": [], "skipped": [], "failed": []}

    dest_root = star_dir(config.SPECTRA_ROOT, instrument, hd)
    downloaded, skipped, failed = [], [], []

    for i, fits_url in enumerate(dl_urls, start=1):
        seq_match = re.search(r'\[s1d,(\d+)\]', fits_url)
        seq = seq_match.group(1) if seq_match else str(i)
        
        # Pega a provável Path de um download passado baseado na URL
        temp_dest = format_filename(hd, seq, fits_url, dest_root)

        if already_downloaded(temp_dest):
            skipped.append(temp_dest.name)
            continue

        try:
            new = download_file(fits_url, temp_dest, timeout=config.REQUEST_TIMEOUT)
            if new:
                snr, fits_date = check_fits_snr_and_date(temp_dest)
                if snr is not None and snr < config.MIN_SNR:
                    log.info("%s: descartado pós-download por SNR baixo (%.1f < %s)", instrument, snr, config.MIN_SNR)
                    temp_dest.unlink()
                    skipped.append(temp_dest.name)
                    continue
                    
                # Renomeia para o layout oficial usando a data correta lida
                if fits_date:
                    final_dest = format_filename(hd, fits_date, fits_url, dest_root)
                    if final_dest != temp_dest:
                        temp_dest.rename(final_dest)
                else:
                    final_dest = temp_dest
                    
                fname = final_dest.name
                downloaded.append(fname)
                update_index(config.INDEX_FILE, {
                    "instrument": instrument.upper(),
                    "hd":         hd,
                    "hip":        hip,
                    "star_name":  hd_to_name(hd),
                    "filename":   fname,
                    "filepath":   str(final_dest),
                    "source_url": fits_url,
                    "snr":        snr,
                    "obs_date":   fits_date or seq,
                    "notes":      "PROBLEMATIC" if (hd in _SOPHIE_PROBLEMATIC
                                                    and instrument == "SOPHIE") else "",
                })
            else:
                skipped.append(temp_dest.name)
        except Exception as exc:
            log.error("Falha ao baixar %s: %s", temp_dest.name, exc)
            failed.append(temp_dest.name)

        time.sleep(0.5)

    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


# ---------------------------------------------------------------------------
# Ponto de entrada público
# ---------------------------------------------------------------------------

def run(targets, instrument: str = "SOPHIE") -> dict:
    instrument = instrument.upper()
    assert instrument in ("SOPHIE", "ELODIE")

    log.info("═" * 55)
    log.info("Iniciando download %s  (%d estrelas)", instrument, len(targets))
    log.info("═" * 55)

    all_dl, all_skip, all_fail = [], [], []

    for _, row in targets.iterrows():
        hd  = str(row["HD"]).strip()
        hip = str(row.get("HIP", "")).strip()
        star = f"HD {hd}"
        log.info("── %s ──", star)

        if instrument == "SOPHIE" and hd in _SOPHIE_PROBLEMATIC:
            log.warning("⚠  %s: dados SOPHIE problemáticos (cap7). Baixando mesmo assim.", star)

        try:
            result = _download_star(hd, hip, instrument)
        except Exception as exc:
            log.error("Falha inesperada no processamento de %s: %s", star, exc)
            result = {"downloaded": [], "skipped": [], "failed": [star]}
        n_dl, n_skip, n_fail = (len(result[k]) for k in ("downloaded","skipped","failed"))
        log.info("   %s: %d novos | %d já existiam | %d falhas", star, n_dl, n_skip, n_fail)

        all_dl   += [f"{star}/{f}" for f in result["downloaded"]]
        all_skip += [f"{star}/{f}" for f in result["skipped"]]
        all_fail += [f"{star}/{f}" for f in result["failed"]]

        if n_dl > 0 or n_fail > 0:
            time.sleep(config.SLEEP_BETWEEN)

    totals = {"instrument": instrument, "downloaded": all_dl,
              "skipped": all_skip, "failed": all_fail}
    print_summary(totals)
    return totals


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from utils import setup_logging, load_targets
    setup_logging(config.LOG_FILE)
    inst = sys.argv[1].upper() if len(sys.argv) > 1 else "SOPHIE"
    run(load_targets(config.TARGETS_FILE), instrument=inst)
