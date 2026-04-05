# Implementation Plan: 002-add-heasarc

**Branch**: `002-add-heasarc` | **Date**: 2026-04-05 | **Spec**: [link](spec.md)
**Input**: Feature specification from `/specs/002-add-heasarc/spec.md`

## Summary

O foco principal é o setup do pipeline NASA Heasarc no nosso Data Lake, com uso do `astroquery.heasarc` suportando as missões XMM-Newton e Chandra. Inclui idempotência, renomeio, e skip checks baseados no `INDEX_MASTER`.

## Technical Context

**Language/Version**: Python 3.14 (baseado no ambiente local `uv`)
**Primary Dependencies**: `astroquery` (módulo `heasarc.Heasarc`), classe nativa local `utils.py` com o mapper target-to-coords do SIMBAD e construtores FITS.  
**Target Platform**: CLI (`main.py`) rodando cross-platform via UNIX/MacOS.  

## Project Structure

### Documentation (this feature)

```text
specs/002-add-heasarc/
├── plan.md
├── research.md
├── data-model.md
└── quickstart.md
```

### Source Code

- `spectra_downloader/heasarc.py` [NEW]
- `spectra_downloader/main.py` [MODIFY]
- `spectra_downloader/config.py` [MODIFY]
