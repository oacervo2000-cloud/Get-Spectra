# Phase 0: Research (HEASARC Integration)

- **Decision**: O módulo atuará como uma API pass-through de querying.
- **Rationale**: A classe `Heasarc` do module `astroquery` centraliza o envio de coordenadas e buscas em raio para missões "chanmaster" (Chandra) e "xmmmaster" (XMM-Newton). O `utils.name_to_coords` fornecerá `(ra, dec)` ou apenas submeteremos o nome.
- **Alternatives considered**: TAP querying genérico (rejeitado por conta da robustez da library nativa HEASARC do astroquery).
