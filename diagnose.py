#!/usr/bin/env python3
"""
diagnose.py — Diagnóstico detalhado de cada módulo de download
===============================================================
Testa cada fonte de dados com UMA estrela (HD 10307) e imprime
o resultado detalhado. Use isto para descobrir exatamente o que
falhou antes de rodar main.py completo.

Uso:
    python diagnose.py            # testa tudo
    python diagnose.py sophie     # testa só SOPHIE
    python diagnose.py polarbase
    python diagnose.py harps
    python diagnose.py hires
    python diagnose.py iue
"""

import sys
import json
import re
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import config
from utils import USER_AGENT

GRN  = "\033[92m"; YLW = "\033[93m"; RED = "\033[91m"
BLD  = "\033[1m";  CYN = "\033[96m"; RST = "\033[0m"

TEST_STAR_HD  = "10307"
TEST_STAR_HIP = "7918"

# ─────────────────────────────────────────────────────────────────
def head(title):
    print(f"\n{BLD}{CYN}{'═'*55}{RST}")
    print(f"{BLD}{CYN}  {title}{RST}")
    print(f"{BLD}{CYN}{'═'*55}{RST}")

def ok(msg):   print(f"  {GRN}✔{RST}  {msg}")
def warn(msg): print(f"  {YLW}⚠{RST}  {msg}")
def fail(msg): print(f"  {RED}✘{RST}  {msg}")
def info(msg): print(f"     {msg}")

def get(url, params=None, timeout=20, full=False):
    # 'full' era usado para limitar read(8192) no urllib, em requests podemos usar iter_content ou apenas .content/.text porque a maioria não é enorme
    r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT, "Accept": "*/*"}, timeout=timeout, verify=False)
    # Usa text truncado se não for full (opcional, só para logging mesmo)
    body = r.text if full else r.text[:8192]
    return r.status_code, body

# ─────────────────────────────────────────────────────────────────
def test_sophie():
    head("SOPHIE (OHP)")
    # Testa a listagem com HD 10307
    url = (
        f"http://atlas.obs-hp.fr/sophie/sophie.cgi"
        f"?n=sophies&c=o&o=HD{TEST_STAR_HD}"
        f"&of=1,leda,simbad&sql=view_head%20IS%20NOT%20NULL"
        f"&a=t&z=d|wg|e&ob=ra,seq"
        f"&d=[%27wget%20%22http%3A//atlas%2Eobs-hp%2Efr/sophie/sophie%2Ecgi"
        f"%3Fc%3Di%26a%3Dmime%3Aapplication/fits%26o%3Dsophie%3A[s1d,%27||seq||%27]"
        f"%22%20-O%20%27||seq||%27_s1d%2Efits%27]&nra=l,simbad,d"
    )
    info(f"URL: {url[:80]}...")
    try:
        # full=True lê a página inteira — necessário para stars com muitos espectros
        status, body = get(url, timeout=30, full=True)
        ok(f"HTTP {status} — página recebida ({len(body)} bytes)")

        wget_urls = re.findall(r'wget\s+"(http[^"]+)"', body)
        info(f"Total de matches 'wget \"...\"' na página completa: {len(wget_urls)}")
        after_skip = wget_urls[3:] if len(wget_urls) > 3 else wget_urls
        if after_skip:
            ok(f"{len(after_skip)} URLs de download para HD {TEST_STAR_HD}")
            info(f"Exemplo: {after_skip[0][:80]}")
        else:
            warn("Nenhuma URL após pular cabeçalho. Trecho da página:")
            info(body[:600])
    except Exception as e:
        fail(f"Erro: {e}")

# ─────────────────────────────────────────────────────────────────
def test_elodie():
    head("ELODIE (OHP)")
    url = (
        f"http://atlas.obs-hp.fr/elodie/elodie.cgi"
        f"?n=elodie&c=o&o=HD{TEST_STAR_HD}"
        f"&of=1,leda,simbad&a=t&z=d|wg|e&ob=ra,seq"
        f"&d=[%27wget%20%22http%3A//atlas%2Eobs-hp%2Efr/elodie/elodie%2Ecgi"
        f"%3Fc%3Di%26a%3Dmime%3Aapplication/fits%26o%3Delodie%3A[s1d,%27||seq||%27]"
        f"%22%20-O%20%27||seq||%27_s1d%2Efits%27]&nra=l,simbad,d"
    )
    try:
        status, body = get(url, timeout=30)
        ok(f"HTTP {status}")
        wget_urls = re.findall(r'wget\s+"(http[^"]+)"', body)
        after_skip = wget_urls[3:] if len(wget_urls) > 3 else wget_urls
        if after_skip:
            ok(f"{len(after_skip)} URLs para HD {TEST_STAR_HD}")
        else:
            warn("Nenhuma URL (estrela pode não estar no ELODIE)")
    except Exception as e:
        fail(f"Erro: {e}")

# ─────────────────────────────────────────────────────────────────
def test_polarbase():
    head("PolarBase (IRAP)")

    # Testa SSL
    base = "https://polarbase.irap.omp.eu"
    try:
        status, body = get(base + "/", timeout=15)
        ok(f"SSL OK — HTTP {status}")
    except Exception as e:
        fail(f"SSL/conexão falhou: {e}")
        info("Execute: pip install --upgrade certifi")
        info("Ou: export REQUESTS_CA_BUNDLE=$(python -c 'import certifi; print(certifi.where())')")
        return

    # Testa endpoint de alvos
    for ep in [
        "/api/v2/targets/?name=HD+10307",
        "/api/v2/targets/?name=HD10307",
        "/api/v2/",
        "/api/",
    ]:
        try:
            status, body = get(base + ep, timeout=15)
            ok(f"Endpoint {ep} → HTTP {status}")
            if body.strip().startswith("{"):
                data = json.loads(body)
                info(f"  JSON keys: {list(data.keys())[:6]}")
                count = data.get("count", "?")
                info(f"  count = {count}")
            else:
                info(f"  Resposta (não-JSON): {body[:200]}")
            break
        except Exception as e:
            warn(f"Endpoint {ep}: {e}")

# ─────────────────────────────────────────────────────────────────
def test_harps():
    head("HARPS (ESO Archive)")

    # Verifica credenciais
    user = config.ESO_USERNAME
    pwd  = config.ESO_PASSWORD
    if not pwd:
        fail("ESO_PASSWORD está vazio em config.py / variável de ambiente!")
        info("Soluções:")
        info("  1. export ESO_PASSWORD=suasenha   (antes de rodar main.py)")
        info("  2. Ou edite config.py: ESO_PASSWORD = 'suasenha'")
        info("  3. Ou rode: python -c \"import keyring; keyring.set_password('astroquery:www.eso.org','rrf','suasenha')\"")
    else:
        ok(f"ESO_USERNAME = {user!r}  |  password definida ({len(pwd)} chars)")

    # Testa TAP sem autenticação (colunas padrão ObsCore — sem UPPER(), sem dp_id)
    # dp_id é extensão ESO; UPPER() não é suportado em todas as versões do tap_obs
    tap = "https://archive.eso.org/tap_obs/sync"
    adql = ("SELECT TOP 1 obs_id, target_name, instrument_name "
            "FROM ivoa.ObsCore "
            "WHERE instrument_name = 'HARPS' "
            "AND dataproduct_type = 'spectrum'")
    params = {"QUERY": adql, "FORMAT": "votable",
              "REQUEST": "doQuery", "LANG": "ADQL"}
    try:
        status, body = get(tap, params=params, timeout=30)
        if status == 200 and ("<VOTABLE" in body or "<TABLE" in body or "<TR>" in body):
            ok(f"ESO TAP acessível e retornou dados HARPS (HTTP {status})")
        elif status == 200:
            ok(f"ESO TAP acessível HTTP {status} — {len(body)} bytes")
            info(f"Trecho: {body[:300]}")
        else:
            fail(f"ESO TAP: HTTP {status}")
            info(f"Resposta: {body[:400]}")
    except Exception as e:
        fail(f"ESO TAP: {e}")

    # Testa astroquery
    try:
        from astroquery.eso import Eso
        ok("astroquery.eso importado com sucesso")
        if pwd:
            eso = Eso()
            # Tenta login com keyword args (nova API >= 0.4.7)
            # Se falhar, tenta bypass direto na session (dados públicos não precisam)
            login_ok = False
            # Tentativa A: keyring
            try:
                import keyring
                keyring.set_password("astroquery:www.eso.org", user, pwd)
                eso.login(username=user, store_password=False)
                ok(f"Login ESO bem-sucedido via keyring como '{user}'")
                login_ok = True
            except Exception as _e_kr:
                pass
            # Tentativa B: session.auth direto (bypass login, funciona para dados públicos)
            if not login_ok:
                try:
                    eso._session.auth = (user, pwd)
                    eso.USERNAME = user
                    ok(f"ESO: autenticação via session.auth como '{user}' (login() não disponível)")
                    login_ok = True
                except Exception as _e_sess:
                    warn(f"Login ESO: todas as tentativas falharam. Dados PÚBLICOS ainda acessíveis via TAP.")
                    info(f"Erro keyring: {_e_kr}")
    except ImportError:
        fail("astroquery não instalado: pip install astroquery")

# ─────────────────────────────────────────────────────────────────
def test_hires():
    head("HIRES (KOA)")
    tap = "https://koa.ipac.caltech.edu/TAP/sync"

    # 1) Descobre tabelas disponíveis no TAP
    info("Consultando tabelas disponíveis no KOA TAP...")
    try:
        status, body = get("https://koa.ipac.caltech.edu/TAP/tables", timeout=15)
        if "koa" in body.lower():
            tables = re.findall(r'<name>(koa[^<]+)</name>', body)
            if tables:
                ok(f"Tabelas KOA encontradas: {tables[:8]}")
            else:
                ok(f"TAP/tables: HTTP {status} ({len(body)} bytes)")
                info(f"Trecho: {body[:300]}")
        else:
            warn(f"TAP/tables: {body[:200]}")
    except Exception as e:
        fail(f"KOA TAP/tables: {e}")

    # 2) Testa KOA API v2
    api_url = f"https://koa.ipac.caltech.edu/KoaAPI/v2/search?instrument=HIRES&target=HD+{TEST_STAR_HD}&format=json"
    info(f"KOA API v2: {api_url}")
    try:
        status, body = get(api_url, timeout=30)
        ok(f"HTTP {status} — {len(body)} bytes")
        if body.strip().startswith("[") or '"results"' in body or '"data"' in body:
            try:
                data = json.loads(body)
                n = len(data) if isinstance(data, list) else len(data.get("results", data.get("data", [])))
                ok(f"{n} observações retornadas") if n > 0 else warn("0 observações (estrela pode não ter dados HIRES públicos)")
            except Exception:
                info(f"Body: {body[:300]}")
        else:
            warn(f"Resposta inesperada: {body[:300]}")
    except Exception as e:
        fail(f"KOA API v2: {e}")

    # 3) Testa TAP — KOA/IPAC não suporta FORMAT=json; usa FORMAT=csv
    for table, inst_col in [("koa_tap", "instrume"), ("koa.koa_obs", "instrume"), ("koa.koa_hires", "instrume")]:
        adql = (f"SELECT TOP 3 targname, date_obs FROM {table} "
                f"WHERE targname LIKE '%{TEST_STAR_HD}%' AND {inst_col}='HIRES'")
        params = {"QUERY": adql, "FORMAT": "csv",
                  "REQUEST": "doQuery", "LANG": "ADQL"}
        try:
            status, body = get(tap, params=params, timeout=20)
            if status == 200:
                lines = [l for l in body.strip().split("\n") if l and not l.startswith("#")]
                n = max(0, len(lines) - 1)   # subtrai linha de cabeçalho
                (ok if n > 0 else warn)(f"TAP {table}: {n} resultado(s)")
                if n > 0:
                    info(f"  {lines[1][:80]}")   # primeiro resultado
            else:
                warn(f"TAP {table}: HTTP {status} — {body[:200]}")
        except Exception as e:
            warn(f"TAP {table}: {e}")

# ─────────────────────────────────────────────────────────────────
def test_iue():
    head("IUE / HST (MAST)")
    try:
        from astroquery.mast import Observations
        ok("astroquery.mast importado")
        obs = Observations.query_criteria(
            target_name=f"HD{TEST_STAR_HD}",
            obs_collection="IUE",
            dataproduct_type="spectrum",
        )
        if obs and len(obs) > 0:
            ok(f"IUE: {len(obs)} observações para HD {TEST_STAR_HD}")
        else:
            warn(f"IUE: nenhuma observação para HD {TEST_STAR_HD} (pode não existir)")

        obs_hst = Observations.query_criteria(
            target_name=f"HD{TEST_STAR_HD}",
            obs_collection="HST",
            dataproduct_type="spectrum",
        )
        if obs_hst and len(obs_hst) > 0:
            ok(f"HST: {len(obs_hst)} observações para HD {TEST_STAR_HD}")
        else:
            warn("HST: nenhuma para esta estrela")
    except ImportError:
        fail("astroquery não instalado")
    except Exception as e:
        fail(f"MAST: {e}")

# ─────────────────────────────────────────────────────────────────
TESTS = {
    "sophie":    test_sophie,
    "elodie":    test_elodie,
    "polarbase": test_polarbase,
    "harps":     test_harps,
    "hires":     test_hires,
    "iue":       test_iue,
}

if __name__ == "__main__":
    requested = [a.lower() for a in sys.argv[1:]] if len(sys.argv) > 1 else list(TESTS)
    for name in requested:
        if name in TESTS:
            TESTS[name]()
        else:
            print(f"{RED}Módulo desconhecido: {name}{RST}. Opções: {', '.join(TESTS)}")
    print()
