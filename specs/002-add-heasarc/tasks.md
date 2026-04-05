# Tasks: 002-add-heasarc

**Branch**: `002-add-heasarc` | **Status**: Pending

## Phase 1: Setup
- [ ] T001 Inicializar script `heasarc.py` baseando-se no modelo existente (`iue.py` ou `koa.py`).

## Phase 2: Core Data Retrieval (US1)
- [ ] T002 Implementar `_query_heasarc()` com fallback cruzado no `simbad_resolve()` de `utils.py` chamando as missões *chanmaster* e *xmmmaster* via astroquery.
- [ ] T003 Iterar as linhas do Votable da mission interceptando metadados ou urls para execução e salvamento dos `.fits`. 

## Phase 3: Registration & Idempotency (US2)
- [ ] T004 Registrar `INDEX_MASTER.csv` corretamente interceptando `already_downloaded()` de maneira robusta.

## Phase 4: Integration
- [ ] T005 Adicionar `heasarc` no registry do orquestrador `main.py`.
- [ ] T006 Expor `"heasarc": True` em `config.py`.

## Phase 5: Testing & Polish
- [ ] T007 Validar execução via `python diagnose.py heasarc` e `python main.py --dry-run`.
