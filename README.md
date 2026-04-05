# 🚗 Movida Scraper: Inteligência de Dados Automotivos

[![Python 3.14+](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![Architecture: Medalhão](https://img.shields.io/badge/Architecture-Medallion-orange.svg)](#-arquitetura-de-dados-medalhão)
[![Framework: ITIL 4](https://img.shields.io/badge/Framework-ITIL%204-green.svg)](#-alinhamento-itil-4)

O **Movida Scraper** é um serviço robusto de extração e refinamento de dados de veículos seminovos. Ele transforma o estoque público da Movida em um ativo de dados estruturado, geolocalizado e pronto para análise avançada de BI (Power BI, Tableau, etc).

---

## 🏗️ Arquitetura de Dados (Medalhão)

O serviço opera seguindo o padrão de camadas para garantir governança e qualidade:

```text
[ SITE MOVIDA ] ──( Scraper )──▶ [ BRONZE ] ──( Processador )──▶ [ SILVER ] ──( Enriquecimento )──▶ [ GOLD ]
      ▲                              │               │               │                │               │
      └─────────── RAW ──────────────┘               └──── CLEAN ────┘                └──── VALUE ────┘
```

1.  **🟫 Bronze (Raw)**: Captura fiel do HTML/Cards. Sem perdas. `data/bronze/`
2.  **🟨 Silver (Cleaned)**: Tipagem de dados (R$, KM), extração de atributos e remoção de lixo. `data/silver/`
3.  **🟩 Gold (Refined)**: Normalização de modelos, Geocodificação (Lat/Long) e KPIs de negócio. `data/gold/`

---

## 🚀 Início Rápido

### Requisitos
- Python 3.14+
- Google Chrome instalado
### Instalação
```bash
# Clone o repositório e entre na pasta
git clone https://github.com/nahuan891-dev/movida_project
cd movida-scraper

# Crie e ative o ambiente virtual (Windows)
python -m venv .venv
.\.venv\Scripts\activate

# Instale as dependências
pip install -r requirements.txt
```

---

## 🐳 Execução via Docker (Recomendado)

O projeto está totalmente conteinerizado, o que garante que o Selenium funcione perfeitamente sem depender do Chrome instalado na sua máquina local.

### Requisitos
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado.

### Comandos Docker
```bash
# 1. Construir a imagem do serviço
docker-compose build

# 2. Rodar uma coleta rápida (20 veículos por marca)
docker-compose up

# 3. Rodar a coleta completa (1000 veículos)
docker-compose run movida-scraper --max-cards 1000

# 4. Executar apenas o Health Check (Teste de Conectividade)
docker-compose run health-check
```
*Nota: Os dados coletados no Docker são persistidos automaticamente na sua pasta local `data/` através de volumes.*

---

## 🚀 Execução Comum (Local)

Para rodar diretamente no seu Windows (usando o `.venv`):

```powershell
# Coleta padrão (1000 veículos por marca, modo silencioso)
.\.venv\Scripts\python.exe main.py

# Coleta rápida para testes (5 veículos por marca, modo visível)
.\.venv\Scripts\python.exe main.py --max-cards 5 --interactive --verbose

# Apenas teste de diagnóstico de saúde
.\.venv\Scripts\python.exe main.py --test-only
```

---

## 🛠️ Referência Completa de Comandos (CLI)

Você pode customizar a execução usando estas flags (funciona tanto no Docker quanto no Local):

| Flag | Descrição | Padrão | Exemplo |
| :--- | :--- | :--- | :--- |
| `--max-cards` | Qtd de carros coletados por marca | `1000` | `--max-cards 2000` |
| `--timeout` | Tempo máximo (segundos) de espera por página | `300` | `--timeout 600` |
| `--output` | Nome do arquivo CSV final | `carros_movida.csv` | `--output base_abril.csv` |
| `--verbose` | Exibe logs detalhados de cada ação do scraper | `False` | `--verbose` |
| `--test-only` | Apenas testa se o site está acessível | `False` | `--test-only` |
| `--no-by-brand` | Coleta da página geral (mais lento/instável) | `False` | `--no-by-brand` |

### Como usar essas flags no Docker?
Basta adicioná-las ao final do comando `run`:
```bash
# Exemplo: Coletar 2000 carros com log detalhado no Docker
docker-compose run movida-scraper --max-cards 2000 --verbose
```

---

## 📊 Capacidades do Serviço

-   **Geolocalização Automática**: Converte a cidade do anúncio em coordenadas `Latitude/Longitude` via Nominatim API.
-   **Normalização de Modelos**: Corrige nomes como "Corolla Xei" vs "COROLLA XEi" para garantir agrupamentos corretos.
-   **Análise de SLA**: O script valida a qualidade dos dados ao final da execução (Ex: % de preços capturados).
-   **Power BI Ready**: Gera automaticamente o arquivo `data/gold_final.csv`, fonte direta para o Dashboard.

---

## 📈 Como Visualizar no Power BI

O projeto já inclui um painel pré-configurado para análise imediata:

1.  **Fonte de Dados**: O painel consome o arquivo **`data/gold_final.csv`**.
2.  **Abrindo o Relatório**: Abra o arquivo **`powerbi/BI_Movida.pbix`**.
3.  **Atualização de Caminho**:
    -   Se ao abrir o Power BI ele exibir erro de "Caminho não encontrado", vá em `Transformar Dados` -> `Configurações da Fonte de Dados`.
    -   Altere o caminho para o local exato onde o arquivo `gold_final.csv` está no seu computador.
    -   Clique em `Atualizar` para carregar os dados mais recentes coletados.

---

## 🔧 Configurações (`src/config.py`)

Você pode ajustar o comportamento do scraper sem mexer no código principal:
-   **Marcas**: Adicione ou remova marcas da lista `BRANDS`.
-   **Delays**: Controle o tempo de espera entre scrolls para evitar bloqueios.
-   **Bins**: Altere as faixas de preço e KM para os filtros do Power BI.

---

## 📋 Alinhamento ITIL 4

Este projeto não é apenas um script, mas um **Serviço de TI** alinhado às práticas do ITIL 4:
-   **Foco no Valor**: O output é projetado para a tomada de decisão do analista de mercado.
-   **Gestão de Incidentes**: Diagnóstico de saúde integrado via `ServiceHealth`.
-   **Melhoria Contínua**: Logs detalhados em `movida_scraper.log` para identificação de gargalos.

---
**Desenvolvido para transformar dados em decisões estratégicas.**
