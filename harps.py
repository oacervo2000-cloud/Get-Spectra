#!/usr/bin/env python3
"""
harps.py — Download de espectros HARPS do ESO Archive
======================================================
Fonte  : ESO Science Archive Facility (https://archive.eso.org/)
Acesso : ESO TAP (dados públicos) + autenticação ESO para download
         Credenciais: config.ESO_USERNAME / config.ESO_PASSWORD
Dados  : HARPS @ La Silla (3.6m), R ≈ 115 000
         Também pode baixar: UVES, FEROS, ESPRESSO (mesmo backend)

Dependências: astroquery >= 0.4.7

Estratégia:
  1. Autentica na API ESO com astroquery.eso.Eso
  2. Para cada estrela, faz query TAP no instrumento HARPS
  3. Filtra por produto 'SCIENCE' (não calibração)
  4. Baixa os arquivos .fits (espectro 1D e/ou 2D)

Referências:
  - https://astroquery.readthedocs.io/en/latest/eso/eso.html
  - https://archive.eso.org/tap_obs/tables
  - Mayor et al. (2003) — HARPS, The Messenger 114, 20

Nota sobre produtos HARPS:
  - *_e2ds_A.fits  — espectro 2D por ordem echelle (mais completo)
  - *_s1d_A.fits   — espectro 1D resampled (mais fácil de usar)
  - *_ccf_G2_A.fits — CCF para RV
  Recomendado para S-index: s1d (ou e2ds se quiser resolver as ordens)
"""

import logging
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
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Tipos de produto HARPS a baixar (pode restringir em config.py no futuro)
# Sufixos preferidos — em ordem de prioridade
_PREFERRED_SUFFIXES = ("_s1d_A.fits", "_e2ds_A.fits")

# Instrumento ESO padrão (pode ser sobrescrito ao chamar run())
_DEFAULT_INSTRUMENT = "HARPS"

# Período de embargo: espectros públicos após 1 ano da observação
# astroquery já trata isso automaticamente


# ---------------------------------------------------------------------------
# Autenticação e sessão ESO
# ---------------------------------------------------------------------------

def _get_eso_instance():
    """
    Cria e autentica uma instância de astroquery.eso.Eso.
    Usa as credenciais de config.py ou variáveis de ambiente.
    Se ESO_PASSWORD estiver vazio, pede interativamente via getpass.
    """
    try:
        from astroquery.eso import Eso
    except ImportError:
        raise ImportError(
            "astroquery não instalado. Execute:\n"
            "  pip install astroquery"
        )

    eso = Eso()
    eso.ROW_LIMIT = -1   # sem limite de linhas nas queries

    username = config.ESO_USERNAME
    password = config.ESO_PASSWORD

    if not username:
        raise ValueError("ESO_USERNAME não configurado em config.py")

    if not password:
        import getpass
        log.warning("ESO_PASSWORD não definido. Solicitando interativamente...")
        print(f"\n[ESO] Digite a senha para o usuário '{username}':")
        password = getpass.getpass(prompt="  Senha ESO: ")
        if not password:
            raise ValueError(
                "Senha ESO vazia. Configure antes de rodar:\n"
                "  export ESO_PASSWORD=suasenha\n"
                "  Ou edite config.py: ESO_PASSWORD = 'suasenha'"
            )

    # Nova API astroquery (>=0.4.7): login() não aceita password posicional.
    # A senha deve estar no keyring do sistema OU passada via mock do getpass.
    logged_in = False

    # Tentativa 1: keyring (armazena a senha para login() a recuperar)
    try:
        import keyring
        keyring.set_password("astroquery:www.eso.org", username, password)
        eso.login(username=username, store_password=False)
        logged_in = True
        log.info("ESO: autenticado via keyring como '%s'", username)
    except Exception as e1:
        log.debug("ESO keyring login falhou: %s", e1)

    # Tentativa 2: mock do getpass (intercepta o prompt de senha)
    if not logged_in:
        try:
            import unittest.mock
            with unittest.mock.patch("getpass.getpass", return_value=password):
                eso.login(username=username, store_password=False)
            logged_in = True
            log.info("ESO: autenticado via mock getpass como '%s'", username)
        except Exception as e2:
            log.debug("ESO mock login falhou: %s", e2)

    # Tentativa 3: API antiga (astroquery < 0.4.7) — login(username, password)
    if not logged_in:
        try:
            eso.login(username, password)
            logged_in = True
            log.info("ESO: autenticado via API antiga como '%s'", username)
        except Exception as e3:
            log.debug("ESO API antiga falhou: %s", e3)

    # Tentativa 4: bypass total — define auth diretamente na sessão requests.
    # Necessário nas versões de astroquery em que _login() mudou de assinatura.
    # Funciona para dados PÚBLICOS (>1 ano); dados proprietários precisam de login real.
    if not logged_in:
        try:
            eso._session.auth = (username, password)
            eso.USERNAME = username
            logged_in = True
            log.info("ESO: autenticação via session.auth como '%s' (login() indisponível nesta versão)", username)
        except Exception as e4:
            log.warning("ESO: session.auth também falhou: %s", e4)

    if not logged_in:
        raise RuntimeError(
            f"Login ESO falhou para '{username}' (todas as 4 tentativas).\n"
            "Verifique usuário/senha em https://www.eso.org/sso/\n"
            "Ou tente: python -c \"import keyring; "
            f"keyring.set_password('astroquery:www.eso.org','{username}','suasenha')\""
        )

    return eso


# ---------------------------------------------------------------------------
# Query de espectros via TAP
# ---------------------------------------------------------------------------

def _query_harps(eso, star_name: str, instrument: str = _DEFAULT_INSTRUMENT):
    """
    Consulta o arquivo ESO pelo nome da estrela e instrumento.
    Retorna uma astropy Table com os resultados (pode ser vazia).

    Colunas relevantes:
        dp_id        — identificador único do produto
        target       — nome do alvo
        dp_cat       — categoria (SCIENCE, CALIB, ...)
        dp_type      — tipo (SPECTRUM, ...)
        ins_id       — instrumento
        date_obs     — data de observação (UTC)
        exptime      — tempo de exposição (s)
        snr          — SNR estimado
        ob_name      — nome do OB
        access_url   — URL de download (autenticado)
    """
    log.debug("Consultando ESO TAP: %s / %s", instrument, star_name)

    try:
        # astroquery >= 0.4.7: query_instrument aceita column_filters como dict
        table = eso.query_instrument(
            instrument.lower(),
            column_filters={
                "target":  star_name,
                "dp_cat":  "SCIENCE",
                "dp_type": "SPECTRUM",
            }
        )
    except TypeError:
        # Versões mais antigas usam kwargs diretos
        try:
            table = eso.query_instrument(
                instrument.lower(),
                target=star_name,
                dp_cat="SCIENCE",
                dp_type="SPECTRUM",
            )
        except Exception as exc2:
            log.warning("query_instrument (modo antigo) falhou: %s. Tentando TAP.", exc2)
            table = _query_tap_fallback(star_name, instrument)
    except Exception as exc:
        log.warning("query_instrument falhou para %s: %s. Tentando TAP.", star_name, exc)
        table = _query_tap_fallback(star_name, instrument)

    if table is None or len(table) == 0:
        log.info("%s: nenhum espectro encontrado para '%s'", instrument, star_name)
        return None

    log.debug("%s: %d espectros encontrados para '%s'", instrument, len(table), star_name)
    return table


def _query_tap_fallback(star_name: str, instrument: str) -> object:
    """
    Fallback: consulta via TAP ADQL direto caso query_instrument falhe.
    Retorna astropy Table ou None.
    """
    import pyvo

    tap_url = "https://archive.eso.org/tap_obs"
    # Colunas corretas do ESO ObsCore (verificado contra schema do arquivo ESO)
    # instrument_name = nome do instrumento (ex: 'HARPS')
    # target_name     = nome do alvo
    # obs_collection  = coleção (ex: 'HARPS')
    adql = (
        f"SELECT dp_id, target_name, obs_collection, dataproduct_type, "
        f"instrument_name, t_min, exptime, snr, access_url "
        f"FROM ivoa.ObsCore "
        f"WHERE UPPER(instrument_name) LIKE UPPER('%{instrument}%') "
        f"  AND dataproduct_type = 'spectrum' "
        f"  AND target_name LIKE '%{star_name.replace(' ', '%')}%' "
        f"ORDER BY t_min"
    )
    try:
        svc = pyvo.dal.TAPService(tap_url)
        result = svc.search(adql)
        return result.to_table() if result else None
    except Exception as exc:
        log.warning("TAP fallback falhou: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Download de arquivos de uma estrela
# ---------------------------------------------------------------------------

def _download_star(eso, hd: str, hip: str, instrument: str) -> dict:
    """
    Baixa todos os espectros ESO de uma estrela para o instrumento selecionado.
    """
    star_name = hd_to_name(hd)  # "HD 10307"
    downloaded, skipped, failed = [], [], []

    table = _query_harps(eso, star_name, instrument)
    if table is None or len(table) == 0:
        # Tentar com variantes (algumas bases usam "HD10307" sem espaço)
        for variant in [f"HD{hd}", f"* {hd}", star_name.replace(" ", "")]:
            table = _query_harps(eso, variant, instrument)
            if table is not None and len(table) > 0:
                break

    if table is None or len(table) == 0:
        return {"downloaded": downloaded, "skipped": skipped, "failed": failed}

    dest_root = star_dir(config.SPECTRA_ROOT, instrument, hd)

    for row in table:
        snr_val = row.get("snr")
        if snr_val is not None and str(snr_val).strip() != "":
            try:
                if float(snr_val) < config.MIN_SNR:
                    log.info("%s: %s ignorado (SNR %.1f < %s)", instrument, row.get("dp_id", ""), float(snr_val), config.MIN_SNR)
                    continue
            except ValueError:
                pass

        dp_id      = str(row["dp_id"]) if "dp_id" in table.colnames else ""
        date_obs   = str(row.get("date_obs", ""))
        access_url = str(row.get("access_url", ""))

        dest = format_filename(hd, date_obs, access_url, dest_root)
        fname = dest.name

        if already_downloaded(dest):
            skipped.append(fname)
            continue

        # Download autenticado via astroquery
        try:
            data_files = eso.retrieve_data(
                [dp_id],
                destination=str(dest_root),
                continuation=True,
            )
            if data_files:
                original_file = Path(data_files[0])
                if dest != original_file:
                    original_file.rename(dest)

                downloaded.append(fname)
                update_index(config.INDEX_FILE, {
                    "instrument": instrument,
                    "hd":         hd,
                    "hip":        hip,
                    "star_name":  star_name,
                    "filename":   fname,
                    "filepath":   str(dest),
                    "obs_date":   date_obs,
                    "exp_time":   str(row.get("exptime", "")),
                    "snr":        str(row.get("snr", "")),
                    "source_url": access_url,
                })
            else:
                log.warning("retrieve_data retornou vazio para %s", dp_id)
                failed.append(fname)
        except Exception as exc:
            log.error("Falha ao baixar %s de HD %s: %s", dp_id, hd, exc)
            failed.append(fname)

        time.sleep(1.0)   # ESO pede cortesia

    return {"downloaded": downloaded, "skipped": skipped, "failed": failed}


# ---------------------------------------------------------------------------
# Ponto de entrada público
# ---------------------------------------------------------------------------

def run(targets, instrument: str = _DEFAULT_INSTRUMENT) -> dict:
    """
    Baixa espectros HARPS (ou UVES/FEROS/ESPRESSO) para todas as estrelas.

    Parâmetros
    ----------
    targets    : pandas.DataFrame com colunas HD, HIP
    instrument : "HARPS" | "UVES" | "FEROS" | "ESPRESSO"

    Retorno
    -------
    dict com listas 'downloaded', 'skipped', 'failed'
    """
    instrument = instrument.upper()

    log.info("═" * 55)
    log.info("Iniciando download %s  (%d estrelas)", instrument, len(targets))
    log.info("═" * 55)

    try:
        eso = _get_eso_instance()
    except (ImportError, ValueError) as exc:
        log.error("Não foi possível iniciar sessão ESO: %s", exc)
        return {"instrument": instrument, "downloaded": [], "skipped": [],
                "failed": [hd_to_name(r["HD"]) for _, r in targets.iterrows()]}

    all_dl, all_skip, all_fail = [], [], []

    for _, row in targets.iterrows():
        hd  = str(row["HD"]).strip()
        hip = str(row["HIP"]).strip()
        star = f"HD {hd}"

        log.info("── %s ──", star)

        try:
            result = _download_star(eso, hd, hip, instrument)
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

    totals = {"instrument": instrument,
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

    inst = sys.argv[1].upper() if len(sys.argv) > 1 else "HARPS"
    targets = load_targets(config.TARGETS_FILE)
    run(targets, instrument=inst)
