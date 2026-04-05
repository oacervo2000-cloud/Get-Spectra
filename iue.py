#!/usr/bin/env python3
"""
iue.py — Download de espectros IUE e HST/STIS do MAST
======================================================
Fonte  : MAST (Mikulski Archive for Space Telescopes)
         https://mast.stsci.edu/
Acesso : astroquery.mast (sem autenticação para dados públicos)
Dados  :
  • IUE (International Ultraviolet Explorer, 1978–1996)
      - Mg II h&k (2796/2803 Å) — indicador de atividade UV
      - R ≈ 10 000–20 000 (SWP e LWP)
  • HST/STIS (Space Telescope Imaging Spectrograph, 1997–)
      - UV e óptico de alta resolução, R até 110 000
      - Mg II, C IV, Ly-α

Estratégia:
  1. astroquery.mast.Observations.query_criteria() filtra por
     target_name, obs_collection (IUE ou HST), e wavelength_region (UV)
  2. astroquery.mast.Observations.get_product_list() obtém arquivos
  3. Filtra produtos por type='S' (ciência) e sufixo .fits
  4. astroquery.mast.Observations.download_products() faz o download

Referências:
  - https://astroquery.readthedocs.io/en/latest/mast/mast.html
  - Boggess et al. (1978) — IUE, Nature 275, 372
  - Woodgate et al. (1998) — HST/STIS, PASP 110, 1183
"""

import logging
import re
import time
from pathlib import Path

import config
from utils import (
    star_dir,
    update_index,
    already_downloaded,
    print_summary,
    hd_to_name,
    format_filename,
    check_fits_snr_and_date,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Coleções MAST a consultar
_COLLECTIONS = {
    "IUE":  {"obs_collection": "IUE"},
    "HST":  {"obs_collection": "HST",  "instrument_name": "STIS*"},
}

# Comprimentos de onda de interesse (UV para Mg II)
_WAVELENGTH_RANGE = (2500, 3200)   # Angstrom

# Sufixos de produto a baixar
_SCIENCE_EXTS = re.compile(r"\.(fits|fit)(\.gz)?$", re.IGNORECASE)

# Variantes de nome que o MAST usa para estrelas HD
def _name_variants(hd: str) -> list[str]:
    return [
        f"HD{hd}",
        f"HD {hd}",
        f"HD  {hd}",
    ]


# ---------------------------------------------------------------------------
# Download via astroquery.mast
# ---------------------------------------------------------------------------

def _get_mast():
    """Importa e retorna Observations do astroquery."""
    try:
        from astroquery.mast import Observations
        return Observations
    except ImportError:
        raise ImportError(
            "astroquery não instalado. Execute:\n"
            "  pip install astroquery"
        )


def _query_mast(Obs, target_name: str, collection: str, extra_filters: dict) -> object:
    """
    Consulta MAST por espectros UV de uma estrela.
    Retorna astropy Table ou None.
    """
    filters = {
        "target_name":     target_name,
        "obs_collection":  collection,
        "dataproduct_type": "spectrum",
        **extra_filters,
    }
    log.debug("MAST query: %s %s", collection, target_name)
    try:
        obs_table = Obs.query_criteria(**filters)
        if obs_table is None or len(obs_table) == 0:
            return None
        return obs_table
    except Exception as exc:
        log.debug("MAST query falhou para '%s' (%s): %s", target_name, collection, exc)
        return None


def _download_star(hd: str, hip: str) -> dict:
    """
    Baixa espectros IUE e HST/STIS de uma estrela via MAST.
    """
    downloaded, skipped, failed = [], [], []

    try:
        Obs = _get_mast()
    except ImportError as exc:
        log.error("%s", exc)
        return {"downloaded": downloaded, "skipped": skipped, "failed": failed}

    for coll_name, extra_filters in _COLLECTIONS.items():
        dest_root = star_dir(config.SPECTRA_ROOT, coll_name, hd)

        # Tenta variantes de nome
        obs_table = None
        for name in _name_variants(hd):
            obs_table = _query_mast(Obs, name, coll_name, extra_filters)
            if obs_table is not None and len(obs_table) > 0:
                log.debug("MAST %s: %d observações para '%s'", coll_name, len(obs_table), name)
                break

        if obs_table is None or len(obs_table) == 0:
            log.info("MAST %s: nenhuma observação para HD %s", coll_name, hd)
            continue

        # Obtém lista de produtos (arquivos FITS)
        try:
            products = Obs.get_product_list(obs_table)
        except Exception as exc:
            log.warning("get_product_list falhou para HD %s (%s): %s", hd, coll_name, exc)
            continue

        if products is None or len(products) == 0:
            continue

        # Filtra: apenas ciência, apenas FITS
        sci_products = Obs.filter_products(
            products,
            productType=["SCIENCE"],
        )
        # Filtra por extensão .fits
        if sci_products is not None and len(sci_products) > 0:
            mask = [bool(_SCIENCE_EXTS.search(str(fn))) for fn in sci_products["productFilename"]]
            sci_products = sci_products[mask]
        else:
            sci_products = products

        if len(sci_products) == 0:
            log.info("MAST %s: sem produtos de ciência para HD %s", coll_name, hd)
            continue

        log.info("MAST %s: %d arquivos para HD %s", coll_name, len(sci_products), hd)

        # Download
        try:
            manifest = Obs.download_products(
                sci_products,
                download_dir=str(dest_root),
                cache=True,
            )
        except Exception as exc:
            log.error("download_products falhou para HD %s (%s): %s", hd, coll_name, exc)
            failed.append(f"HD{hd}_{coll_name}")
            continue

        if manifest is None:
            failed.append(f"HD{hd}_{coll_name}")
            continue

        # Processa manifesto de download
        for row in manifest:
            local_path = Path(str(row.get("Local Path", "")))
            status     = str(row.get("Status", ""))
            original_fname = local_path.name if local_path.name else "unknown.fits"

            if status == "COMPLETE" and local_path.exists():
                snr, fits_date = check_fits_snr_and_date(local_path)
                if snr is not None and snr < config.MIN_SNR:
                    log.info("MAST: arquivo descartado por SNR baixo (%.1f < %s): %s", snr, config.MIN_SNR, original_fname)
                    local_path.unlink()
                    skipped.append(original_fname)
                    continue
                
                source_url = str(row.get("URL", ""))
                dest = format_filename(hd, fits_date or "", source_url, dest_root)
                fname = dest.name
                
                if dest != local_path:
                    local_path.rename(dest)

                downloaded.append(fname)
                update_index(config.INDEX_FILE, {
                    "instrument": coll_name,
                    "hd":         hd,
                    "hip":        hip,
                    "star_name":  hd_to_name(hd),
                    "filename":   fname,
                    "filepath":   str(dest),
                    "source_url": source_url,
                    "obs_date":   fits_date or "",
                    "snr":        snr,
                })
            elif status == "SKIPPED":
                # Se for SKIPPED porque já estava baixado pelo astroquery
                # Precisamos traduzir o nome se possível
                skipped.append(original_fname)
            else:
                log.warning("MAST download status '%s' para %s", status, original_fname)
                failed.append(original_fname)

        time.sleep(1.0)

    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


# ---------------------------------------------------------------------------
# Ponto de entrada público
# ---------------------------------------------------------------------------

def run(targets) -> dict:
    """
    Baixa espectros IUE e HST/STIS do MAST para todas as estrelas.

    Parâmetros
    ----------
    targets : pandas.DataFrame com colunas HD, HIP

    Retorno
    -------
    dict com listas 'downloaded', 'skipped', 'failed'
    """
    log.info("═" * 55)
    log.info("Iniciando download IUE/HST (MAST)  (%d estrelas)", len(targets))
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

    totals = {"instrument": "IUE/HST",
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
