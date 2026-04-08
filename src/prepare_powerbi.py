"""
Script auxiliar para preparar dados para Power BI
Gera arquivos de configuração e metadados
"""
from platform import processor

from geopy.geocoders import Nominatim
from geopy.exc import GeopyError
import time
import logging

import pandas as pd
import json
from pathlib import Path

from concurrent.futures import ThreadPoolExecutor, as_completed

GEO_CACHE_FILE = Path("data/geo_cache.json")

def carregar_cache_geo():
    if GEO_CACHE_FILE.exists():
        try:
            with open(GEO_CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Erro ao carregar cache: {e}")
    return {}

def salvar_cache_geo(cache):
    try:
        GEO_CACHE_FILE.parent.mkdir(exist_ok=True)
        with open(GEO_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logging.error(f"Erro ao salvar cache: {e}")

def adicionar_coordenadas(df, coluna_localizacao, max_workers=1):
    geolocator = Nominatim(user_agent="movida_scraper_v2")
    logger_geo = logging.getLogger("geocoder")
    
    # 1. Carregar cache persistente
    cache_geo = carregar_cache_geo()
    
    # 2. Identificar localizações únicas que NÃO estão no cache
    todas_locs = [loc for loc in df[coluna_localizacao].unique() if pd.notna(loc) and str(loc).strip()]
    locs_para_consultar = [loc for loc in todas_locs if loc not in cache_geo]
    
    if not locs_para_consultar:
        print(f"✅ Todas as {len(todas_locs)} localizações já estão no cache. Nenhuma consulta necessária!")
    else:
        print(f"🌍 {len(cache_geo)} locs no cache. Consultando {len(locs_para_consultar)} novas localizações...")
        
        for i, loc in enumerate(locs_para_consultar):
            try:
                # Tentar busca direta
                query = f"{loc}, Brazil"
                location = geolocator.geocode(query, addressdetails=True, timeout=10)
                
                if not location:
                    # Fallback: cidade apenas
                    city_only = loc.split(',')[0].strip()
                    location = geolocator.geocode(f"{city_only}, Brazil", addressdetails=True, timeout=10)

                if location:
                    addr = location.raw.get('address', {})
                    # Tentar várias chaves possíveis para cidade
                    city = addr.get('city') or addr.get('town') or addr.get('village') or addr.get('suburb') or addr.get('municipality')
                    state_sigla = loc.split(',')[-1].strip() if ',' in loc else (addr.get('state_district') or addr.get('state') or 'BR')
                    
                    # FALLBACK: Se city for None, usa o nome da própria busca (limpo)
                    if not city:
                        city = loc.split(',')[0].strip()

                    norm = f"{city}, {state_sigla}"
                    
                    cache_geo[loc] = {
                        'lat': location.latitude,
                        'lon': location.longitude,
                        'normalized': norm
                    }
                    print(f"📍 [{i+1}/{len(locs_para_consultar)}] Geocodificado: {loc}")
                else:
                    print(f"⚠️  [{i+1}/{len(locs_para_consultar)}] Não encontrado: {loc}")
                
                # Salvar a cada 5 consultas para não perder progresso se cair
                if (i + 1) % 5 == 0:
                    salvar_cache_geo(cache_geo)
                
                # RATE LIMIT SUPER CONSERVADOR: Nominatim exige 1s, vamos usar 2.5s para garantir
                time.sleep(2.5)
                
            except Exception as e:
                print(f"❌ Erro em {loc}: {e}")
                time.sleep(2) # Pausa maior em caso de erro

        # Salvar cache final
        salvar_cache_geo(cache_geo)

    # 3. Aplicar cache ao DataFrame
    lats, lons, norms = [], [], []
    for loc in df[coluna_localizacao]:
        res = cache_geo.get(loc)
        if res:
            lats.append(res['lat'])
            lons.append(res['lon'])
            norms.append(res['normalized'])
        else:
            lats.append(None)
            lons.append(None)
            norms.append(loc)
            
    df['latitude'] = lats
    df['longitude'] = lons
    df[coluna_localizacao] = norms
    
    return df


def criar_metadata_powerbi(csv_path: str, output_dir: str = "powerbi"):
    """
    Cria arquivo de metadados para Power BI
    """
    # Carregar dados
    df = pd.read_csv(csv_path)

    # Estatísticas básicas
    metadata = {
        "dataset_info": {
            "nome": "Carros Movida - Seminovos",
            "total_registros": len(df),
            "data_coleta": df['data_coleta'].iloc[0] if 'data_coleta' in df.columns else None,
            "fonte": "https://www.seminovosmovida.com.br"
        },
        "colunas": {
            col: {
                "tipo": str(df[col].dtype),
                "valores_unicos": df[col].nunique(),
                "valores_nulos": df[col].isnull().sum(),
                "exemplo": str(df[col].dropna().iloc[0]) if len(df[col].dropna()) > 0 else None
            } for col in df.columns
        },
        "estatisticas": {
            "preco": {
                "media": df['preco'].mean(),
                "mediana": df['preco'].median(),
                "min": df['preco'].min(),
                "max": df['preco'].max(),
                "desvio_padrao": df['preco'].std()
            } if 'preco' in df.columns else None,
            "km": {
                "media": df['km'].mean(),
                "mediana": df['km'].median(),
                "min": df['km'].min(),
                "max": df['km'].max()
            } if 'km' in df.columns else None,
            "marcas": df['marca'].value_counts().to_dict() if 'marca' in df.columns else None
        },
        "insights_recomendados": [
            "Preço médio por marca",
            "Distribuição geográfica",
            "Correlação preço vs quilometragem",
            "Faixas de preço mais populares",
            "Idade média dos veículos por marca"
        ]
    }

    # Criar diretório
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Salvar metadata
    with open(output_path / "metadata.json", 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

    print(f"✅ Metadata salva em: {output_path / 'metadata.json'}")
    return metadata

def criar_template_visualizacoes(output_dir: str = "powerbi"):
    """
    Cria template de visualizações recomendadas
    """
    template = {
        "paginas": [
            {
                "nome": "Visão Geral",
                "visualizacoes": [
                    {
                        "tipo": "card",
                        "titulo": "Total de Carros",
                        "campo": "id",
                        "agregacao": "count"
                    },
                    {
                        "tipo": "card",
                        "titulo": "Preço Médio",
                        "campo": "preco",
                        "agregacao": "average",
                        "formato": "R$ #,##0.00"
                    },
                    {
                        "tipo": "card",
                        "titulo": "KM Médio",
                        "campo": "km",
                        "agregacao": "average",
                        "formato": "#,##0"
                    },
                    {
                        "tipo": "barras",
                        "titulo": "Carros por Marca",
                        "eixo_x": "marca",
                        "valores": "id",
                        "agregacao": "count"
                    },
                    {
                        "tipo": "mapa",
                        "titulo": "Distribuição Geográfica",
                        "localizacao": "localizacao",
                        "tamanho": "preco"
                    }
                ]
            },
            {
                "nome": "Análise de Preços",
                "visualizacoes": [
                    {
                        "tipo": "histograma",
                        "titulo": "Distribuição de Preços",
                        "campo": "preco",
                        "bins": 20
                    },
                    {
                        "tipo": "scatter",
                        "titulo": "Preço vs KM",
                        "eixo_x": "km",
                        "eixo_y": "preco",
                        "tamanho": "idade_veiculo"
                    },
                    {
                        "tipo": "boxplot",
                        "titulo": "Preços por Marca",
                        "categoria": "marca",
                        "valores": "preco"
                    }
                ]
            }
        ],
        "segmentacoes": [
            {
                "campo": "marca",
                "titulo": "Filtrar por Marca"
            },
            {
                "campo": "faixa_preco",
                "titulo": "Faixa de Preço"
            },
            {
                "campo": "faixa_km",
                "titulo": "Faixa de KM"
            }
        ]
    }

    # Criar diretório
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Salvar template
    with open(output_path / "template_visualizacoes.json", 'w', encoding='utf-8') as f:
        json.dump(template, f, indent=2, ensure_ascii=False)

    print(f"✅ Template salvo em: {output_path / 'template_visualizacoes.json'}")
    return template

if __name__ == "__main__":
    # Gerar arquivos para Power BI
    csv_path = "data/carros_powerbi.csv"
    # No main, após carregar o df:

    df = processor.process_car_data(dados)
    df = adicionar_coordenadas(df, 'localizacao')
    df.to_csv('data/carros_powerbi_com_coords.csv', index=False)
    if Path(csv_path).exists():
        print("🔄 Gerando arquivos auxiliares para Power BI...")

        # Criar metadata
        metadata = criar_metadata_powerbi(csv_path)

        # Criar template
        template = criar_template_visualizacoes()

        print("\n✅ Arquivos gerados com sucesso!")
        print(f"📁 Diretório: powerbi/")
        print(f"📄 metadata.json: Informações sobre o dataset")
        print(f"📄 template_visualizacoes.json: Sugestões de visualizações")

    else:
        print(f"❌ Arquivo CSV não encontrado: {csv_path}")
        print("Execute primeiro: python main.py --max-cards 100 --output carros_powerbi.csv")