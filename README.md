# 🚗 Movida Scraper: Inteligência de Dados Automotivos

[![Python 3.14+](https://img.shields.io/badge/python-3.14+-blue.svg)](https://www.python.org/downloads/)
[![Architecture: Medalhão](https://img.shields.io/badge/Architecture-Medallion-orange.svg)](#-arquitetura-de-dados-medalhão)
[![AI: Predictive](https://img.shields.io/badge/AI-Predictive-blueviolet.svg)](#-inteligência-artificial-preditiva)
[![Framework: ITIL 4](https://img.shields.io/badge/Framework-ITIL%204-green.svg)](#-alinhamento-itil-4)

O **Movida Scraper** é uma plataforma de inteligência competitiva de dados para o mercado automotivo. Ele automatiza a coleta, o refinamento e a **análise preditiva** do estoque da Movida, transformando dados brutos em insights estratégicos geolocalizados e projeções de mercado.

---

## 🏗️ Arquitetura de Dados (Medalhão)

O serviço opera seguindo o padrão de camadas para garantir governança e qualidade:

```text
[ SITE MOVIDA ] ──( Scraper )──▶ [ BRONZE ] ──( Processador )──▶ [ SILVER ] ──( Enriquecimento )──▶ [ GOLD ] ──( ML Engine )──▶ [ PREDICTIONS ]
      ▲                              │               │               │                │               │                │
      └─────────── RAW ──────────────┘               └──── CLEAN ────┘                └──── VALUE ────┘                └──── INSIGHTS ──┘
```

1.  **🟫 Bronze (Raw)**: Captura fiel do HTML/Cards. `data/bronze/`
2.  **🟨 Silver (Cleaned)**: Tipagem de dados (R$, KM) e extração de atributos. `data/silver/`
3.  **🟩 Gold (Refined)**: Normalização, Geocodificação e KPIs de negócio. `data/gold/`
4.  **🔮 Predictions (AI)**: Predição de demanda e saúde da frota via ML/DL. `data/gold_predictions.csv`

---

## 🧠 Inteligência Artificial Preditiva

O projeto conta com uma camada de ciência de dados avançada para responder às críticas de mercado e apoiar a tomada de decisão:

### 1. Previsão de Demanda (Deep Learning)
- **Modelo**: LSTM (Long Short-Term Memory) implementado em PyTorch.
- **Objetivo**: Prever a densidade de anúncios para o próximo período com base na série temporal coletada.
- **Técnica**: Normalização via MinMaxScaler e Windowing de 7 a 30 dias.

### 2. Saúde da Frota e Aging (Machine Learning)
- **Modelo**: XGBoost Regressor.
- **Objetivo**: Predizer a idade esperada do veículo com base em KM, Marca e Preço.
- **KPI - Fleet Health Score**: Diferença absoluta entre a idade real e a prevista. Desvios altos indicam veículos que precisam de manutenção urgente ou renovação imediata.

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

# Instale as dependências (Inclui XGBoost, PyTorch e Scikit-learn)
uv pip install -r requirements.txt
```

---

## 🐳 Execução via Docker (Recomendado)

O projeto está totalmente conteinerizado, o que garante que o Selenium e a IA funcionem perfeitamente.

### Comandos Docker
```bash
# Rodar a coleta completa + Treinamento da IA
docker-compose run movida-scraper --max-cards 1000
```
*A IA é disparada automaticamente após a consolidação da camada Gold.*

---

## 📊 Capacidades do Serviço

-   **Predição de Aging**: Identifica veículos com quilometragem fora do padrão para a idade.
-   **Deep Learning de Demanda**: Projeção de volume de estoque futuro por região.
-   **Geolocalização Automática**: Conversão de cidades em coordenadas geográficas.
-   **Análise de SLA**: Validação automática da integridade dos dados coletados.
-   **Data Science Ready**: Exporta `gold_predictions.csv` com métricas de R2 e Loss embutidas.

---

## 📈 Visualização no Power BI

O dashboard consome a camada preditiva para gerar insights de negócio:

1.  **Fonte de Dados**: Aponte para **`data/gold_predictions.csv`**.
2.  **KPIs Preditivos**: Use as colunas `Aging_Predicted` e `Predicted_Demand_Next_Period` para gráficos de tendência e saúde da frota.

---

## 📋 Alinhamento ITIL 4

-   **Foco no Valor**: Transforma relatórios estáticos em ferramentas preditivas.
-   **Gestão de Mudanças**: Modelos salvos em `data/models/` para garantir reprodutibilidade.
-   **Melhoria Contínua**: Métricas de treinamento logadas em cada execução.

---
**Transformando a coleta de dados em uma vantagem competitiva preditiva.**
