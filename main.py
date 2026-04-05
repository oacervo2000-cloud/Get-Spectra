#!/usr/bin/env python3
"""
main.py — Orquestrador do spectra_downloader
============================================
Baixa espectros de múltiplas bases de dados astronômicas para a lista
de alvos definida em config.TARGETS_FILE (lubin2010.csv).

Uso básico:
    python main.py                          # tudo ativo em config.py
    python main.py --only sophie            # apenas SOPHIE
    python main.py --only sophie polarbase  # múltiplos
    python main.py --skip harps             # tudo exceto HARPS
    python main.py --dry-run                # sem baixar, só lista

Outras amostras (múltiplos projetos):
    python main.py --targets outra_lista.csv
        → usa lista diferente, mesmos diretórios SPECTRA/<INST>/HD_XXXX/

    python main.py --targets outra_lista.csv --project amostra2
        → separa os espectros em SPECTRA/amostra2/<INST>/HD_XXXX/
        → index e log também ficam em subpasta separada

Rodar em background (tmux):
    tmux new -s download
    source ~/ativar_spectra.sh
    python main.py
    Ctrl+B, D   ← desanexa sem interromper
    tmux attach -t download   ← para reconectar

Módulos disponíveis (ativar/desativar em config.DOWNLOAD_MODULES):
    sophie     → SOPHIE (OHP)
    elodie     → ELODIE (OHP, legado)
    polarbase  → NARVAL + ESPaDOnS (PolarBase)
    harps      → HARPS (ESO Archive) — requer credenciais ESO
    uves       → UVES (ESO Archive)
    feros      → FEROS (ESO Archive)
    xshooter   → X-shooter (ESO Archive)
    hires      → HIRES (KOA)
    iue        → IUE + HST/STIS (MAST)
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Garante que o diretório do projeto está no PYTHONPATH
sys.path.insert(0, str(Path(__file__).parent))

import config
from utils import setup_logging, load_targets, print_summary

log = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Registro de módulos
# ---------------------------------------------------------------------------

def _build_module_registry() -> dict:
    """
    Retorna dict: nome → função run()
    Importações lazy para não falhar caso algum pacote opcional falte.
    """
    registry = {}

    try:
        import sophie
        registry["sophie"]  = lambda t: sophie.run(t, instrument="SOPHIE")
        registry["elodie"]  = lambda t: sophie.run(t, instrument="ELODIE")
    except ImportError as e:
        log.warning("Módulo sophie/elodie não disponível: %s", e)

    try:
        import polarbase
        registry["polarbase"] = polarbase.run
    except ImportError as e:
        log.warning("Módulo polarbase não disponível: %s", e)

    try:
        import harps
        registry["harps"]    = lambda t: harps.run(t, instrument="HARPS")
        registry["uves"]     = lambda t: harps.run(t, instrument="UVES")
        registry["feros"]    = lambda t: harps.run(t, instrument="FEROS")
        registry["xshooter"] = lambda t: harps.run(t, instrument="XSHOOTER")
    except ImportError as e:
        log.warning("Módulo harps não disponível: %s", e)

    try:
        import koa
        registry["hires"] = koa.run
    except ImportError as e:
        log.warning("Módulo koa/hires não disponível: %s", e)

    try:
        import iue
        registry["iue"] = iue.run
    except ImportError as e:
        log.warning("Módulo iue não disponível: %s", e)

    return registry


# ---------------------------------------------------------------------------
# Argparse
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="spectra_downloader — Acervo de espectros estelares",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--only", nargs="+", metavar="MODULO",
        help="Executa apenas estes módulos (ex: --only sophie harps)"
    )
    p.add_argument(
        "--skip", nargs="+", metavar="MODULO",
        help="Pula estes módulos"
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Não baixa nada, apenas lista o que seria executado"
    )
    p.add_argument(
        "--targets", default=None,
        help="CSV de alvos alternativo (padrão: config.TARGETS_FILE)"
    )
    p.add_argument(
        "--project", default=None, metavar="NOME",
        help=(
            "Nome do projeto/amostra (ex: --project amostra2). "
            "Cria subdiretório SPECTRA/<NOME>/<INST>/HD_XXXX/ isolando "
            "os espectros desta amostra. Sem --project, usa SPECTRA_ROOT direto."
        )
    )
    p.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Lógica principal
# ---------------------------------------------------------------------------

def select_modules(registry: dict, args) -> list[tuple[str, callable]]:
    """
    Retorna lista ordenada de (nome, função) a executar,
    respeitando config.DOWNLOAD_MODULES, --only e --skip.
    """
    # Ordem de execução preferida
    ORDER = ["sophie", "elodie", "polarbase", "harps", "hires", "iue",
             "uves", "feros", "xshooter"]

    # Filtra pela configuração do config.py
    active_in_config = {
        k for k, v in config.DOWNLOAD_MODULES.items() if v
    }

    selected = []
    for name in ORDER:
        if name not in registry:
            continue
        if name not in active_in_config:
            log.debug("Módulo '%s' desativado em config.DOWNLOAD_MODULES", name)
            continue
        if args.only and name not in [m.lower() for m in args.only]:
            continue
        if args.skip and name in [m.lower() for m in args.skip]:
            log.info("Pulando '%s' (--skip)", name)
            continue
        selected.append((name, registry[name]))

    return selected


def run_all(args):
    """Executa o pipeline completo."""
    # Configura logging
    setup_logging(config.LOG_FILE,
                  level=getattr(logging, args.log_level))

    # ── Ajusta SPECTRA_ROOT e paths de índice/log para o projeto ──────────
    if args.project:
        import os
        project_root = str(Path(config.SPECTRA_ROOT) / args.project)
        os.environ["SPECTRA_PROJECT_ROOT"] = project_root
        config.SPECTRA_ROOT  = project_root
        config.INDEX_FILE    = str(Path(project_root) / "INDEX_MASTER.csv")
        config.LOG_FILE      = str(
            Path(config.LOG_FILE).parent / f"download_{args.project}.log"
        )
        Path(project_root).mkdir(parents=True, exist_ok=True)

    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║         spectra_downloader — início                  ║")
    log.info("╠══════════════════════════════════════════════════════╣")
    log.info("║  Início  : %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
    log.info("║  Alvos   : %s", args.targets or config.TARGETS_FILE)
    log.info("║  Projeto : %s", args.project or "(padrão)")
    log.info("║  SPECTRA : %s", config.SPECTRA_ROOT)
    log.info("╚══════════════════════════════════════════════════════╝")

    # Carrega alvos
    targets_file = args.targets or config.TARGETS_FILE
    try:
        targets = load_targets(targets_file)
        log.info("Lista de alvos: %d estrelas carregadas de %s",
                 len(targets), targets_file)
    except Exception as exc:
        log.critical("Não foi possível carregar lista de alvos: %s", exc)
        sys.exit(1)

    # Registra módulos
    registry = _build_module_registry()
    modules  = select_modules(registry, args)

    if not modules:
        log.warning("Nenhum módulo selecionado. Verifique config.DOWNLOAD_MODULES e os argumentos.")
        sys.exit(0)

    log.info("Módulos a executar: %s", ", ".join(m for m, _ in modules))

    if args.dry_run:
        print("\n[DRY RUN] Seria executado:")
        for name, _ in modules:
            print(f"  • {name}  ({len(targets)} estrelas)")
        print()
        return

    # Executa cada módulo
    global_stats = {"downloaded": 0, "skipped": 0, "failed": 0}
    t0 = time.time()

    for i, (name, func) in enumerate(modules, 1):
        log.info("")
        log.info("┌─────────────────────────────────────────┐")
        log.info("│  Módulo %d/%d: %-28s │", i, len(modules), name.upper())
        log.info("└─────────────────────────────────────────┘")

        try:
            result = func(targets)
            global_stats["downloaded"] += len(result.get("downloaded", []))
            global_stats["skipped"]    += len(result.get("skipped",    []))
            global_stats["failed"]     += len(result.get("failed",     []))
        except Exception as exc:
            log.error("Módulo '%s' encerrou com erro: %s", name, exc, exc_info=True)
            global_stats["failed"] += len(targets)

        # Pausa entre módulos
        if i < len(modules):
            log.info("Aguardando %ds antes do próximo módulo...", config.SLEEP_BETWEEN)
            time.sleep(config.SLEEP_BETWEEN)

    # Resumo global
    elapsed = time.time() - t0
    log.info("")
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║                  RESUMO GLOBAL                       ║")
    log.info("╠══════════════════════════════════════════════════════╣")
    log.info("║  Novos downloads : %5d                              ║", global_stats["downloaded"])
    log.info("║  Já existiam     : %5d                              ║", global_stats["skipped"])
    log.info("║  Falhas          : %5d                              ║", global_stats["failed"])
    log.info("║  Tempo total     : %5.0f s  (%.1f min)              ║",
             elapsed, elapsed / 60)
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info("Índice mestre: %s", config.INDEX_FILE)
    log.info("Log completo : %s", config.LOG_FILE)

    sys.exit(0 if global_stats["failed"] == 0 else 1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    args = parse_args()
    run_all(args)
