"""
config.py — Configurações do spectra_downloader
================================================
Edite as variáveis abaixo conforme seu ambiente.
Este arquivo fica em ~/GET SPECTRA/spectra_downloader/config.py
"""

import os
from pathlib import Path

# ─── Diretório raiz para todos os espectros ────────────────────────────────
# Altere para onde quiser armazenar o acervo.
SPECTRA_ROOT = os.environ.get("SPECTRA_ROOT", str(Path.home() / "SPECTRA"))

# ─── Lista de alvos ────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
TARGETS_FILE = str(_HERE / "targets" / "lubin2010.csv")

# ─── Índice mestre (registra todos os espectros baixados) ──────────────────
INDEX_FILE = str(Path(SPECTRA_ROOT) / "INDEX_MASTER.csv")

# ─── Log ───────────────────────────────────────────────────────────────────
LOG_FILE = str(_HERE / "logs" / "download.log")

# ─── Credenciais ESO (necessário para HARPS) ───────────────────────────────
# Preencha aqui ou exporte as variáveis de ambiente antes de rodar:
#   export ESO_USERNAME=rrf
#   export ESO_PASSWORD=suasenha
ESO_USERNAME = os.environ.get("ESO_USERNAME", "rrf")
ESO_PASSWORD = os.environ.get("ESO_PASSWORD", "")   # nunca commitar senha aqui

# ─── Módulos ativos (True = será executado por main.py) ────────────────────
DOWNLOAD_MODULES = {
    "sophie":    True,
    "elodie":    False,   # ELODIE é legado; ativar se necessário
    "polarbase": True,
    "harps":     True,
    "hires":     True,
    "iue":       True,
    "uves":      True,
    "feros":     True,
    "xshooter":  True,
}

# ─── Parâmetros de download ────────────────────────────────────────────────
REQUEST_TIMEOUT  = 120    # segundos por arquivo
SLEEP_BETWEEN    = 5      # segundos entre estrelas (evitar sobrecarga nos servidores)
MIN_SNR          = 10     # Valor mínimo da relação Sinal-Ruído para aceitar o arquivo
