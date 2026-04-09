# Usar imagem base estável do Python
FROM python:3.12-slim

# Evitar que o Python gere arquivos .pyc e garantir logs em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalar dependências do sistema e Google Chrome
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    gnupg \
    ca-certificates \
    apt-transport-https \
    unzip \
    libgomp1 \
    --no-install-recommends \
    && curl -fSsL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor | tee /usr/share/keyrings/google-chrome.gpg > /dev/null \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Instalar o UV para gerenciamento rápido de pacotes
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Configurar diretório de trabalho
WORKDIR /app

# Copiar apenas o requirements primeiro para aproveitar o cache do Docker
COPY requirements.txt .

# Instalar dependências usando o UV (MUITO mais rápido)
# Note: Instalando torch via índice CPU para manter a imagem leve
RUN uv pip install --system --no-cache -r requirements.txt \
    && uv pip install --system torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu

# Copiar o restante do código do projeto
COPY . .

# Garantir que as pastas de dados e modelos existam
RUN mkdir -p data/bronze data/silver data/gold data/json data/db data/logos data/models

# Definir o ponto de entrada
ENTRYPOINT ["python", "main.py"]

# Comando padrão
CMD ["--max-cards", "100", "--no-by-brand"]
