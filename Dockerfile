# Usar imagem base estável do Python
FROM python:3.11-slim

# Evitar que o Python gere arquivos .pyc e garantir logs em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Instalar dependências do sistema e ferramentas para o Google Chrome
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    apt-transport-https \
    curl \
    unzip \
    --no-install-recommends

# Instalar o Google Chrome Estável
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Configurar diretório de trabalho
WORKDIR /app

# Copiar apenas o requirements primeiro para aproveitar o cache do Docker
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o restante do código do projeto
COPY . .

# Garantir que as pastas de dados existam
RUN mkdir -p data/bronze data/silver data/gold data/json data/db data/logos

# Definir o ponto de entrada
ENTRYPOINT ["python", "main.py"]

# Comando padrão (pode ser sobrescrito no docker-compose ou na linha de comando)
CMD ["--max-cards", "100", "--by-brand"]
