# Tasks: 001-add-xshooter

**Branch**: `001-add-xshooter` | **Status**: Pending

## Phase 1: Setup
- [x] T001 Inicializar revisão em `spectra_downloader/harps.py` para adequá-lo como um motor ESO generalista.

## Phase 2: Foundational
- [x] T002 Verificar hardcodes em `harps.py` assegurando que os módulos UVES, FEROS, e XSHOOTER farão `import harps` apontando o driver.

## Phase 3: [US1] Obtenção de Espectros do X-shooter
- [x] T003 [US1] Refatorar função interna de `harps.py` ou chamadas do `astroquery.eso` para interpolar as requisições de sub-sistemas UVES, FEROS e XSHOOTER.

## Phase 4: [US2] Tolerância e Aplicação de QoS e Padronização
- [x] T004 [P] [US2] Preservar a rotina de exclusão `MIN_SNR` durante o mapeamento de arquivos FITS recuperados no motor generalista ESO em `harps.py`.

## Phase 5: [US3] Registro Global na Orquestração Mestra
- [x] T005 [P] [US3] Registrar os aliases "xshooter", "uves", "feros" no despachante CLI principal de `spectra_downloader/main.py`.
- [x] T006 [P] [US3] Expor "XSHOOTER", "UVES", "FEROS" nas listas de alvos em `spectra_downloader/config.py`.

## Phase 6: Polish
- [x] T007 Validar via dry run todo o pipeline rodando e iterando sobre os novos links sem crash.
