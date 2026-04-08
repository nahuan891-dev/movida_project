"""
Pacote Movida Scraper
Coletor de dados de carros seminovos da Movida
"""

__version__ = "1.0.0"
__author__ = "Movida Scraper Team"
__description__ = "Scraper para coletar dados de carros da Movida"

from .config import *
from .scraper import MovidaScraper
from .data_processor import DataProcessor
from .utils import setup_logging, print_data_summary, validate_data_quality

__all__ = [
    'MovidaScraper',
    'DataProcessor',
    'setup_logging',
    'print_data_summary',
    'validate_data_quality'
]