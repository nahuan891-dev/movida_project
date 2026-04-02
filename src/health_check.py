"""
Módulo de Health Check para o Serviço Movida Scraper
Prática ITIL 4: Monitoramento e Gestão de Eventos
"""

import logging
import requests
from selenium.webdriver.common.by import By
from .config import MOVIDA_URL, INITIAL_PAGE_LOAD_DELAY
from .scraper import MovidaScraper

logger = logging.getLogger(__name__)

class ServiceHealth:
    def __init__(self, scraper: MovidaScraper):
        self.scraper = scraper

    def check_connectivity(self) -> bool:
        """Valida se o site está online (Status 200)"""
        try:
            response = requests.get(MOVIDA_URL, timeout=10)
            if response.status_code == 200:
                logger.info("✅ Conectividade: Site online (HTTP 200)")
                return True
            else:
                logger.error(f"❌ Conectividade: Site retornou status {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Conectividade: Erro ao acessar site: {e}")
            return False

    def check_site_structure(self) -> bool:
        """Valida se o XPath dos cards ainda é válido (Evita falhas silenciosas)"""
        try:
            self.scraper.driver = self.scraper._setup_driver()
            self.scraper.driver.get(MOVIDA_URL)
            import time
            time.sleep(INITIAL_PAGE_LOAD_DELAY)
            
            # Tenta encontrar pelo menos um card
            cards = self.scraper.driver.find_elements(By.XPATH, "//a/card")
            if len(cards) > 0:
                logger.info(f"✅ Estrutura: XPath válido ({len(cards)} cards detectados)")
                return True
            else:
                logger.warning("❌ Estrutura: Nenhum card detectado. O site pode ter mudado.")
                return False
        except Exception as e:
            logger.error(f"❌ Estrutura: Erro ao validar elementos: {e}")
            return False
        finally:
            if self.scraper.driver:
                self.scraper.driver.quit()

    def run_full_diagnostic(self) -> bool:
        """Executa diagnóstico completo antes de iniciar o pipeline"""
        print("\n🔍 Executando Diagnóstico de Saúde do Serviço...")
        conn = self.check_connectivity()
        if not conn: return False
        
        struct = self.check_site_structure()
        return struct
