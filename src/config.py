"""
Configurações do projeto Movida Scraper
"""

# URLs
MOVIDA_URL = "https://www.seminovosmovida.com.br/busca"

# URLs específicas por marca
BRAND_URLS = {
    'volkswagen': 'https://www.seminovosmovida.com.br/busca/marcas-volkswagen',
    'fiat': 'https://www.seminovosmovida.com.br/busca/marcas-fiat',
    'volvo': 'https://www.seminovosmovida.com.br/busca/marcas-volvo',
    'caoa-chery': 'https://www.seminovosmovida.com.br/busca/marcas-caoa-chery',
    'renault': 'https://www.seminovosmovida.com.br/busca/marcas-renault',
    'hyundai': 'https://www.seminovosmovida.com.br/busca/marcas-hyundai',
    'toyota': 'https://www.seminovosmovida.com.br/busca/marcas-toyota',
    'jeep': 'https://www.seminovosmovida.com.br/busca/marcas-jeep',
    'chevrolet': 'https://www.seminovosmovida.com.br/busca/marcas-chevrolet',
    'citroen': 'https://www.seminovosmovida.com.br/busca/marcas-citroen',
    'peugeot': 'https://www.seminovosmovida.com.br/busca/marcas-peugeot',
    'nissan': 'https://www.seminovosmovida.com.br/busca/marcas-nissan',
    'audi': 'https://www.seminovosmovida.com.br/busca/marcas-audi',
    'bmw': 'https://www.seminovosmovida.com.br/busca/marcas-bmw',
    'ford': 'https://www.seminovosmovida.com.br/busca/marcas-ford',
    'honda': 'https://www.seminovosmovida.com.br/busca/marcas-honda',
    'mini': 'https://www.seminovosmovida.com.br/busca/marcas-mini',
    'mercedes-benz': 'https://www.seminovosmovida.com.br/busca/marcas-mercedes-benz'
}

# Configurações de scraping
MAX_CARDS = 1000
SCROLL_TIMEOUT = 300  # segundos
SCROLL_STEP = 500  # pixels por scroll
SCROLL_ATTEMPTS_MAX = 50

# Timing (in seconds) - Otimizado para performance
INITIAL_PAGE_LOAD_DELAY = 3  # Reduzido de 5
POST_SCROLL_DELAY = 0.2      # Reduzido de 0.5
BOTTOM_SCROLL_DELAY = 1      # Reduzido de 2
BRAND_PAGE_LOAD_DELAY = 2    # Reduzido de 5
GEOCODING_RATE_LIMIT = 1  # seconds between API calls

# Scroll behavior
SCROLLS_PER_BATCH = 10

# Brands
BRANDS = [
    'volkswagen', 'fiat', 'renault', 'hyundai', 'toyota', 'jeep',
    'chevrolet', 'citroen', 'peugeot', 'nissan', 'audi', 'volvo',
    'caoa-chery', 'bmw', 'ford', 'honda', 'mini', 'mercedes-benz'
]

# Configurações de dados
DATA_COLUMNS = [
    'id', 'marca', 'modelo', 'versao', 'ano_fabricacao', 'ano_modelo',
    'km', 'localizacao', 'preco_bruto', 'condicoes', 'texto_completo',
    'data_coleta', 'preco', 'faixa_preco', 'faixa_km', 'idade_veiculo'
]

# Faixas de preço
PRECO_BINS = [0, 30000, 50000, 80000, 120000, float('inf')]
PRECO_LABELS = ['Até 30k', '30k-50k', '50k-80k', '80k-120k', '120k+']

# Faixas de quilometragem
KM_BINS = [0, 30000, 60000, 100000, 150000, float('inf')]
KM_LABELS = ['Até 30k km', '30k-60k km', '60k-100k km', '100k-150k km', '150k+ km']

# Configurações de output
OUTPUT_DIR = "data"
DEFAULT_FILENAME = "carros_movida.csv"
ENCODING = 'utf-8-sig'

# Estados e cidades para normalização de localização
BRAZIL_STATES = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG',
    'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
]

MAJOR_CITIES = [
    'Rio de Janeiro', 'São Paulo', 'Belo Horizonte', 'Brasília', 'Salvador',
    'Fortaleza', 'Curitiba', 'Manaus', 'Recife', 'Goiânia', 'Belém', 'Porto Alegre',
    'Guarulhos', 'Campinas', 'São Luís', 'São Gonçalo', 'Maceió', 'Duque de Caxias',
    'Natal', 'Teresina', 'São Bernardo do Campo', 'São Bernardo', 'Nova Iguaçu',
    'Campo Grande', 'Osasco', 'Santo André', 'João Pessoa', 'Jaboatão dos Guararapes',
    'Contagem', 'São José dos Campos', 'São José', 'Uberlândia', 'Sorocaba', 'Ribeirão Preto',
    'Cuiabá', 'Aracaju', 'Feira de Santana', 'Joinville', 'Juiz de Fora', 'Londrina',
    'Aparecida de Goiânia', 'Ananindeua', 'Porto Velho', 'Serra', 'Niterói', 'Belford Roxo',
    'Caxias do Sul', 'Campos dos Goytacazes', 'Macapá', 'Florianópolis', 'Vila Velha',
    'Mauá', 'São João de Meriti', 'São José do Rio Preto', 'Mogi das Cruzes', 'Betim',
    'Santos', 'Diadema', 'Maringá', 'Jundiaí', 'Campina Grande', 'Montes Claros',
    'Rio Branco', 'Piracicaba', 'Carapicuíba', 'Olinda', 'Anápolis', 'Cariacica',
    'Bauru', 'Itaquaquecetuba', 'São Vicente', 'Vitória', 'Pelotas', 'Caucaia',
    'Canoas', 'Caruaru', 'Franca', 'Ponta Grossa', 'Blumenau', 'Petrolina', 'Paulista',
    'Vitória da Conquista', 'Cascavel', 'Santarém', 'Uberaba', 'Petrópolis', 'Mogi Guaçu',
    'Várzea Grande', 'Lauro de Freitas', 'São José', 'Limeira'
]

# Diretórios das camadas de dados
BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"
GOLD_DIR = "data/gold"
JSON_DIR = "data/json"
DB_DIR = "data/db"

# Arquivos de Automação (Fixo)
GOLD_FINAL_CSV = "data/gold_final.csv"
GOLD_FINAL_JSON = "data/gold_final.json"
SQLITE_DB_PATH = "data/db/movida_vendas.db"

# Prefixos para arquivos das camadas
BRONZE_PREFIX = "bronze"
SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"
JSON_PREFIX = "gold"

# Logging
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Selenium
CHROME_OPTIONS = [
    '--headless',  # executar sem interface gráfica
    '--no-sandbox',
    '--disable-dev-shm-usage',
    '--disable-gpu',
    '--window-size=1920,1080'
]