"""
Utilitários diversos para o projeto
"""

import logging
import sys
import re
from pathlib import Path
from typing import Dict, Any, Optional

import pandas as pd

from .config import LOG_LEVEL, LOG_FORMAT

# Mapeamento manual de lojas/shoppings para endereços reais
SHOPPING_MAPPING = {
    "Auto Shopping Shopping Car": {"cidade": "Rio de Janeiro", "estado": "RJ", "bairro": "Barra da Tijuca"},
    "Auto Shopping Portal": {"cidade": "Contagem", "estado": "MG", "bairro": "Babita Camargo"},
    "Auto Shopping Autonomistas": {"cidade": "Osasco", "estado": "SP", "bairro": "Vila Yara"},
    "Auto Shopping Raposo": {"cidade": "São Paulo", "estado": "SP", "bairro": "Butantã"},
    "Auto Shopping Bandeirantes": {"cidade": "São Paulo", "estado": "SP", "bairro": "Limão"},
    "Auto Shopping Mundo Car": {"cidade": "São José", "estado": "SC", "bairro": "Kobrasol"},
    "Auto Shopping Natal": {"cidade": "Natal", "estado": "RN", "bairro": "Candelária"},
    "Auto Shopping Campina": {"cidade": "Campina Grande", "estado": "PB", "bairro": "Catolé"},
    "Auto Shopping Lauro de Freitas": {"cidade": "Lauro de Freitas", "estado": "BA", "bairro": "Estrada do Coco"},
    "Auto Shopping Tamboré": {"cidade": "Barueri", "estado": "SP", "bairro": "Alphaville"},
    "Auto Shopping Arena Motors": {"cidade": "São Paulo", "estado": "SP", "bairro": "Vila Guilherme"},
    "Auto Shopping Passeio das Águas": {"cidade": "Goiânia", "estado": "GO", "bairro": "Residencial Humaitá"},
    "Auto Shopping Show Auto Mall": {"cidade": "Contagem", "estado": "MG", "bairro": "Cidade Industrial"},
    "Auto Shopping Rodrigues": {"cidade": "Salvador", "estado": "BA", "bairro": "Paralela"},
    "Auto Shopping Fortaleza": {"cidade": "Fortaleza", "estado": "CE", "bairro": "Papicu"},
    "Auto Shopping Fórmula":{"cidade": "Várzea Grande", "estado": "MT", "bairro": "Centro"}
}

# Termos que devem ser removidos da string de localização (lixo do scraping)
JUNK_WORDS = [
    r"BAIXO\s*KM", r"SEMINOVOS", r"MOVIDA", r"OFERTA", r"ESPECIAL", 
    r"IPVA\s*\d*\s*PAGO", r"ÚNICO\s*DONO", r"LICENCIADO"
]

def limpar_string_localizacao(texto: str) -> str:
    """
    Remove termos inúteis e limpa a string de localização
    """
    if not texto or not isinstance(texto, str):
        return texto
        
    res = texto
    for junk in JUNK_WORDS:
        res = re.sub(junk, "", res, flags=re.IGNORECASE)
    
    # Remover espaços múltiplos e pontuação solta no fim
    res = re.sub(r'\s+', ' ', res).strip()
    res = re.sub(r'[•\s,-]+$', '', res).strip()
    
    return res

def traduzir_auto_shopping(texto: str) -> Optional[Dict[str, str]]:
    """
    Tenta traduzir um nome de shopping/loja para um endereço real
    
    Args:
        texto: Nome da localização extraído (ex: 'Auto Shopping Portal, MG')
        
    Returns:
        Dicionário com cidade, estado e bairro ou None se não encontrar
    """
    if not texto or not isinstance(texto, str):
        return None
        
    # Limpar o texto para busca (remover estado se houver no fim)
    nome_limpo = re.sub(r',\s*[A-Z]{2}$', '', texto).strip()
    
    # Busca exata ou por início
    for shopping, info in SHOPPING_MAPPING.items():
        if shopping.lower() in nome_limpo.lower():
            return info
            
    return None

def setup_logging(level: str = LOG_LEVEL, format_str: str = LOG_FORMAT) -> None:
    """Configura o sistema de logging"""
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Nível de log inválido: {level}')

    logging.basicConfig(
        level=numeric_level,
        format=format_str,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('movida_scraper.log', encoding='utf-8')
        ]
    )
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

def ensure_directories(*dirs: str) -> None:
    """Garante que os diretórios existam"""
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

def format_currency(value: float) -> str:
    """Formata valor como moeda brasileira"""
    if pd.isna(value):
        return "N/A"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def format_number(value: float, decimals: int = 0) -> str:
    """Formata número com separadores de milhar"""
    if pd.isna(value):
        return "N/A"
    return f"{value:,.{decimals}f}"

def validate_data_quality(df: pd.DataFrame) -> Dict[str, Any]:
    """Valida qualidade dos dados coletados"""
    quality = {
        'total_registros': len(df),
        'registros_com_preco': df['preco'].notna().sum(),
        'registros_com_km': df['km'].notna().sum(),
        'registros_com_ano': df['ano_fabricacao'].notna().sum(),
        'registros_com_localizacao': df['localizacao'].notna().sum(),
        'marcas_unicas': df['marca'].nunique(),
        'duplicatas': df.duplicated(subset=['modelo', 'versao', 'ano_fabricacao']).sum()
    }
    quality['perc_preco'] = (quality['registros_com_preco'] / quality['total_registros']) * 100 if quality['total_registros'] > 0 else 0
    quality['perc_km'] = (quality['registros_com_km'] / quality['total_registros']) * 100 if quality['total_registros'] > 0 else 0
    quality['perc_ano'] = (quality['registros_com_ano'] / quality['total_registros']) * 100 if quality['total_registros'] > 0 else 0
    quality['perc_local'] = (quality['registros_com_localizacao'] / quality['total_registros']) * 100 if quality['total_registros'] > 0 else 0
    return quality

def print_data_summary(df: pd.DataFrame) -> None:
    """Imprime resumo estatístico dos dados"""
    print("\n" + "="*50)
    print("📊 RESUMO DOS DADOS COLETADOS")
    print("="*50)
    print(f"📈 Total de carros: {len(df):,}")
    if 'preco' in df.columns:
        print(f"💰 Preço médio: {format_currency(df['preco'].mean())}")
    if 'marca' in df.columns:
        marcas_top = df['marca'].value_counts().head(5)
        print(f"🏷️ Top 5 marcas: {', '.join(marcas_top.index)}")
    if 'localizacao' in df.columns:
        locais_top = df['localizacao'].value_counts().head(3)
        print(f"📍 Top 3 localizações: {', '.join(locais_top.index)}")
    print("="*50)
