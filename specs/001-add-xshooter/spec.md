# Feature Specification: Adição do Telescópio X-shooter ao ESO Archive

**Feature Branch**: `001-add-xshooter`  
**Created**: 2026-04-05  
**Status**: Draft  
**Input**: User description: "O nosso objetivo é expandir a obtenção de espectros baseados no solo para cobrir simultaneamente as linhas de H-alfa e do Triplete de Cálcio Infravermelho (Ca IRT). Para isso, precisamos de ampliar a nossa interface com o ESO Archive (atualmente gerida pelo 'harps.py'). 1) Adicione o instrumento 'X-shooter' aos alvos de download da classe ESO. 2) No arquivo 'main.py' e no registro de submódulos (onde o harps aponta para si mesmo e para uves/feros), garanta que o X-shooter seja devidamente registrado como um instrumento válido. 3) Modifique o arquivo de configuração para ativar definitivamente os downloads do UVES, FEROS e do novo X-shooter. O pipeline deve seguir respeitando estritamente o filtro de SNR mínimo e a formatação exata de nomenclatura que já implementámos anteriormente."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Obtenção de Espectros do X-shooter (Priority: P1)

Como astrônomo pesquisador, eu quero que o sistema consiga encontrar e baixar espectros do instrumento X-shooter através da interface do The ESO Science Archive Facility. Isso fará com que as análises científicas consigam explorar linhas de H-alfa e do Triplete de Cálcio Infravermelho (Ca IRT) na mesma infraestrutura sem precisar de rotinas apartadas.

**Why this priority**: A ampliação para novos instrumentos de amplo alcance infravermelho-espectroscópico é vital e o core desta feature.

**Independent Test**: Pode ser validado independentemente através da indicação de um target estelar conhecido que possua dados abertos do X-shooter, avaliando se os FITS chegam ao HD físico sob o padrão.

**Acceptance Scenarios**:

1. **Given** um Target validado com entradas no telescópio ESO, **When** o pipeline for configurado para baixar o `X-shooter`, **Then** o sistema acessará a interface, encontrará o produto reduzido e iniciará o download.

---

### User Story 2 - Tolerância e Aplicação de QoS e Padronização (Priority: P1)

Como engenheiro de dados, quero que os FITS do novo instrumento (e dos demais instrumentos da infraestrutura ESO como UVES e FEROS) passem pelo mesmo filtro limitador de SNR mínimo (Relação Sinal Ruído) e nomenclatura temporal (`NomeDaEstrela_DataTempo.fits`).

**Why this priority**: Evitar regressões na lógica de controle de qualidade recém adicionada ao sistema. Arquivos imprestáveis sujam as estatísticas.

**Independent Test**: Simulação de download de arquivo abaixo do SNR com validação de rejeição (arquivo descartado, sem gravação na index).

**Acceptance Scenarios**:

1. **Given** a configuração `MIN_SNR` setada no sistema, **When** os arquivos FITS (tanto UVES, FEROS, X-shooter ou HARPS) forem baixados, **Then** o cabeçalho validará o SNR e rejeitará arquivos ruidosos antes ou durante a indexação, com arquivo resultante renomeado usando o carimbo de data da observação.

---

### User Story 3 - Registro Global na Orquestração Mestra (Priority: P2)

Como mantenedor de software, eu quero expor os instrumentos `X-shooter`, `UVES` e `FEROS` no orquestrador principal para que usuários finais os habilitem ativarem diretamente pelo CLI/scripts ou Config do programa sem ter que mexer no core.

**Why this priority**: Permite que o pipeline execute e chame todos os módulos da série "ESO Archive" automaticamente ou sob demanda.

**Independent Test**: Rodar o main `--only x-shooter` / `--only uves`.

**Acceptance Scenarios**:

1. **Given** o arquivo de configuração e a orquestração `main.py`, **When** eu ligar as chaves de habilitar o UVES, FEROS e X-shooter, **Then** as rotinas devem executar como esperado em série.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: O sistema MUST suportar consultas e busca explícitas para pacotes de dados originados do instrumento `X-shooter` pertencente ao ESO Archive.
- **FR-002**: O sistema MUST registrar `X-shooter`, `UVES` e `FEROS` nas listas canônicas de instrumentos ativos globalmente (interfaces, `main.py` e logs).
- **FR-003**: Os novos instrumentos compartilhados (UVES, FEROS, X-shooter) MUST submeter seus arquivos ao formatador inteligente do sistema visando padronização idempotente `NomeDoAlvo_Data.fits`.
- **FR-004**: O sistema MUST usar o mecanismo validado de exclusão global utilizando o `config.MIN_SNR` descartando espectros corrompidos.
- **FR-005**: O sistema MUST carregar esses instrumentos por definitivo como ativos no pipeline através de update do diretório de configurações.

### Key Entities

- **ESO Request Handler**: O subsistema (atualmente encabeçado pelo `harps.py` e query ESO) deve ser polimórfico o suficiente para abarcar buscas para múltiplas strings de instrumentos.
- **Spectrum Payload**: FITS brutos das APIs externas mapeados para as linhas do `INDEX_MASTER`.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Espectros de targets listados com observações em X-shooter são baixados de ponta a ponta em testes end-to-end com 0% de exceções ou quebra.
- **SC-002**: Arquivos FITS de todos os instrumentos ESO (UVES, FEROS, X-shooter) são nomeados consistentemente (0% de desvios da string `Target_YYYYMMDD_HHMMSS.fits`).
- **SC-003**: Invocação da rotina geral ativada via config sem depender de alterações manuais diárias no código-fonte para baixar de X-shooter, validando 100% de flexibilidade do app.

## Assumptions

- O `astroquery.eso` ou a solução proprietária ESO implementada por `harps.py` possui compatibilidade de busca declarativa idêntica quando enviado o "instrument" como "X-shooter" (ou a string certa equivalente da API para ele, e a mesma coisa para UVES e FEROS).
- As calibrações científicas de cabeçalho (`DATE-OBS` e limite numérico SNR) estão localizadas nas mesmas chaves ou podem ser inferidas no header do FITS e/ou pela metadata fornecida pelo banco ESO Archive.
