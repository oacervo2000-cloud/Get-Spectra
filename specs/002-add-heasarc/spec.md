# Feature Specification: Integração HEASARC (Raios-X) e Expansão ESO

**Feature Branch**: `002-add-heasarc`  
**Created**: 2026-04-05  
**Status**: Draft  
**Input**: User description: "O repositório 'Get-Spectra' atuará como o Data Lake central definitivo para o projeto, devendo escalar no futuro para mais de 5000 alvos. A missão agora é expandir a cobertura espectral para altas energias e infravermelho. 1) Expansão ESO: Modifique a interface existente do ESO (atualmente no harps.py) para incluir o instrumento 'X-shooter', ativando também o UVES e o FEROS no config.py. 2) Novo Módulo HEASARC: Crie um módulo 'heasarc.py' utilizando o 'astroquery.heasarc' para buscar espectros de Raios-X calibrados (missões Chandra e XMM-Newton). O módulo deve resolver as coordenadas do alvo usando a nossa função SIMBAD já existente. 3) Ambos os desenvolvimentos devem aplicar a mesma nomenclatura padronizada e registrar as extrações no INDEX_MASTER.csv, garantindo a idempotência vital para quando operarmos com listas de milhares de estrelas."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Extração de Alta Energia HEASARC (Priority: P1)

Como astrônomo do projeto, eu quero utilizar um novo módulo (`heasarc.py`) construído no orquestrador Data Lake que conecte à base HEASARC da NASA empregando resoluções de coordenadas do SIMBAD para localizar arquivos públicos de missões de Raios-X como Chandra e XMM-Newton, para que possa cruzar os dados de raio-X com dados ópticos terrestres.

**Why this priority**: A inserção de dados multiespectrais em energias X-ray é estratégica e um avanço core do pipeline. O bloqueio geocêntrico/terrestre cessa aqui.

**Independent Test**: Invocar o pipeline informando `main.py --only heasarc` com nome de alvo conhecido por apresentar fluxo em Raios-X detectável e extrair o arquivo resultante sob os padrões em `INDEX_MASTER`.

**Acceptance Scenarios**:

1. **Given** um Target listado e o novo módulo HEASARC configurado, **When** processamos via SIMBAD as Ra/Dec, passamos ao `astroquery.heasarc`, **Then** espectros adequados devem ser filtrados e empurrados para parse.
2. **Given** um FITS resultante do HEASARC, **When** o processo o baixar para o disco nas pastar `SPECTRA_ROOT`, **Then** será formatado no layout idempotente, com registro exato no índice persistente (CSV).

---

### User Story 2 - Idempotência e Skalabilidade (Priority: P1)

Como engenheiro de dados lidando com +5000 alvos, eu quero garantir que caso a run falhe em 2000, e eu recomece a iteração, os módulos ESO revisados (X-shooter, UVES, FEROS) e HEASARC avaliem os downloads preexistentes nos discos através do `INDEX_MASTER.csv` e do utilitário padronizador antes de baixar tudo de novo, saltando os alvos.

**Why this priority**: Reprocessar milhares de GB não é viável via Python. O acervo precisa ser robusto e reentrante.

**Independent Test**: Executar uma dry run para os mesmos alvos preexistentes nos diretórios gerados confirmando log de SKIP em massa sem bytes gastos na banda.

**Acceptance Scenarios**:

1. **Given** que o utilitário já padronizou os arquivos do alvo 1 ao 50, **When** o pipeline for abortado e recomeçar, **Then** as validações internas saltarão downloads apontando idempotência ao longo de todos os instrumentos envolvidos (incluindo HEASARC e expansão ESO).

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: O sistema MUST implementar o módulo `heasarc.py` baseando-se no framework principal e exposto no `main.py`.
- **FR-002**: O módulo MUST utilizar `astroquery.heasarc` associado ao conversor de Ra/Dec dependente do objeto SIMBAD existente para contornar discrepâncias de parsing da ferramenta oficial.
- **FR-003**: O sistema MUST formatar e versionar a string de saída final dos FITS em aderência nativa ao formato Data-Timestamp (`_YYYYMMDD_HHMMSS`) e adicioná-la com sucesso ao banco `INDEX_MASTER.csv`.
- **FR-004**: O sistema MUST consultar e extrair espectros de missões como `Chandra` e `XMM-Newton` contidos ou mascaráveis no namespace do banco de dados oficial HEASARC da NASA.
- **FR-005**: (Legado Referência) Os dados da query ESO Archive via submodulo local suportarão explicitamente as flags "uves", "feros" e "xshooter" ativos nos settings centrais `config.py`.

### Key Entities

- **HEASARC Client**: Novo Driver abstrato acionado pela máquina geral utilizando coord maps.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Novo módulo HEASARC roda de ponta a ponta sem loops infinitos ou quebra de dependências em resoluções globais SIMBAD (taxa de erro sistêmica < 1%).
- **SC-002**: 100% de reuso idêntico do `format_filename` ou do layout em pastas `SPECTRA/<instrumento>/<target>/<target>_data.fits`.
- **SC-003**: Execuções interrompidas de múltiplos alvos (~100 testes rápidos) registram falhas e sucessos de indexação consistentes que são "skipped" em run subsequent.

## Assumptions

- O `astroquery.heasarc` expõe acesso irrestrito síncrono para links de download de missões de calibrações.
- Chandra e XMM-Newton são recuperáveis através da query generalista da API de missões.
- As integrações ESO requeridas nesta spec tecnicamente já constam implementadas (`001-add-xshooter`), ou serão mescladas sem impactos negativos nesta rotina de teste abrangente.
