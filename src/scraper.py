"""
Módulo de scraping para coletar dados de carros da Movida
"""

import time
import logging
import requests
from pathlib import Path
from typing import List, Optional, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from .config import (
    MOVIDA_URL, MAX_CARDS, SCROLL_TIMEOUT, SCROLL_STEP,
    SCROLL_ATTEMPTS_MAX, CHROME_OPTIONS, BRANDS, BRAND_URLS,
    INITIAL_PAGE_LOAD_DELAY, POST_SCROLL_DELAY, BOTTOM_SCROLL_DELAY,
    BRAND_PAGE_LOAD_DELAY, SCROLLS_PER_BATCH
)

logger = logging.getLogger(__name__)


class MovidaScraper:
    """Classe para scraping de dados da Movida"""

    def __init__(self, headless: bool = False):
        """
        Inicializa o scraper

        Args:
            headless: Se True, executa o navegador em modo headless
        """
        self.driver = None
        self.headless = headless
        self.cards_xpath = "//a/card"
        self.logos_dir = Path("data/logos")
        self.logos_dir.mkdir(exist_ok=True, parents=True)

    def _save_brand_logo(self, logo_url: str, brand_idx: int) -> None:
        """
        Salva a logo da marca

        Args:
            logo_url: URL da logo
            brand_idx: Índice da marca
        """
        try:
            response = requests.get(logo_url, timeout=10)
            if response.status_code == 200:
                # Extrair extensão do arquivo
                content_type = response.headers.get('content-type', '')
                if 'png' in content_type:
                    ext = 'png'
                elif 'jpg' in content_type or 'jpeg' in content_type:
                    ext = 'jpg'
                else:
                    ext = 'png'  # padrão

                filename = f"marca_{brand_idx}.{ext}"
                filepath = self.logos_dir / filename

                with open(filepath, 'wb') as f:
                    f.write(response.content)

                logger.info(f"Logo salva: {filepath}")
            else:
                logger.warning(f"Erro ao baixar logo da marca {brand_idx}: {response.status_code}")
        except Exception as e:
            logger.warning(f"Erro ao salvar logo da marca {brand_idx}: {e}")

    def _setup_driver(self) -> webdriver.Chrome:
        """Configura e retorna o driver do Chrome"""
        options = webdriver.ChromeOptions()

        if self.headless:
            for option in CHROME_OPTIONS:
                options.add_argument(option)

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        logger.info("Driver Chrome configurado com sucesso")
        return driver

    def _scroll_smooth(self, height_step: int = SCROLL_STEP) -> None:
        """Realiza scroll suave em incrementos"""
        current_height = self.driver.execute_script("return window.pageYOffset;")
        new_height = current_height + height_step
        self.driver.execute_script(f"window.scrollTo(0, {new_height});")

    def _collect_cards_optimized(self, max_items: int = MAX_CARDS,
                                timeout: int = SCROLL_TIMEOUT) -> List:
        """
        Coleta cards com scroll otimizado e processamento em lote

        Args:
            max_items: Número máximo de cards a coletar
            timeout: Timeout em segundos

        Returns:
            Lista de elementos WebElement dos cards
        """
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException

        start_time = time.time()
        cards = []
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        no_new_cards_count = 0

        logger.info("Iniciando coleta otimizada de cards")

        while (len(cards) < max_items and
               (time.time() - start_time) < timeout and
               scroll_attempts < SCROLL_ATTEMPTS_MAX and
               no_new_cards_count < 3):  # Parar se não há novos cards por 3 scrolls

            # Scroll direto para o bottom (mais eficiente)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Aguardar carregamento dinâmico com timeout reduzido
            try:
                WebDriverWait(self.driver, 2).until(
                    lambda driver: driver.execute_script("return document.body.scrollHeight") > last_height
                )
            except TimeoutException:
                pass  # Continua mesmo se não detectar mudança de altura

            # Atualizar altura e buscar cards
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            current_cards = self.driver.find_elements(By.XPATH, self.cards_xpath)

            # Verificar se encontrou novos cards
            if len(current_cards) > len(cards):
                cards = current_cards
                no_new_cards_count = 0
                logger.info(f"Scroll {scroll_attempts + 1}: {len(cards)} cards")
            else:
                no_new_cards_count += 1

            last_height = new_height
            scroll_attempts += 1

            # Pequena pausa apenas se necessário
            if scroll_attempts % 5 == 0:
                time.sleep(0.5)

        collected = cards[:max_items]
        logger.info(f"Coleta otimizada finalizada. Total: {len(collected)} cards em {time.time() - start_time:.1f}s")
        return collected

    def _extract_cards_batch(self, cards: List) -> List[List[str]]:
        """
        Extrai dados de texto de múltiplos cards em lote, incluindo flag de compra online

        Args:
            cards: Lista de elementos WebElement

        Returns:
            Lista de listas com dados dos cards
        """
        dados = []
        batch_size = 50  # Processar em lotes para melhor performance

        for i in range(0, len(cards), batch_size):
            batch = cards[i:i + batch_size]
            batch_data = []

            for card in batch:
                try:
                    texto = card.text.split('\n')
                    
                    # Verificar flag de Compra Online (Selo/Botão)
                    compra_online = "Não"
                    try:
                        # Busca por texto específico ou presença de botão de reserva
                        card_html = card.get_attribute('innerHTML').upper()
                        if "RESERVAR ONLINE" in card_html or "COMPRA ONLINE" in card_html:
                            compra_online = "Sim"
                    except:
                        pass
                    
                    # Adicionar flag ao final dos dados do card
                    texto.append(f"COMPRA_ONLINE: {compra_online}")
                    batch_data.append(texto)
                except Exception as e:
                    logger.warning(f"Erro ao processar card: {e}")
                    continue

            dados.extend(batch_data)

            if (i + batch_size) % 200 == 0:
                logger.info(f"Processados {i + batch_size} cards")

        return dados

    def scrape_cars(self, max_items: int = MAX_CARDS,
                   timeout: int = SCROLL_TIMEOUT) -> List[List[str]]:
        """
        Método principal para coletar dados dos carros

        Args:
            max_items: Número máximo de cards a coletar
            timeout: Timeout em segundos

        Returns:
            Lista de listas com dados dos cards
        """
        try:
            logger.info("Iniciando scraping da Movida")
            self.driver = self._setup_driver()
            self.driver.get(MOVIDA_URL)

            # Aguardar carregamento inicial
            time.sleep(INITIAL_PAGE_LOAD_DELAY)
            logger.info("Página carregada, iniciando coleta")

            # Coletar cards com método otimizado
            cards = self._collect_cards_optimized(max_items, timeout)

            # Extrair dados em lote
            dados = self._extract_cards_batch(cards)

            logger.info(f"Scraping concluído. Total de dados: {len(dados)}")
            return dados

        except Exception as e:
            logger.error(f"Erro durante scraping: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Driver fechado")

    def _scrape_single_brand(self, brand: str, max_items: int, timeout: int, save_logo: bool = True) -> List[List[str]]:
        """
        Scraping otimizado para uma única marca

        Args:
            brand: Nome da marca
            max_items: Máximo de items por marca
            timeout: Timeout em segundos
            save_logo: Se True, tenta salvar a logo da marca

        Returns:
            Lista de dados da marca
        """
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException, NoSuchElementException

        driver = None
        try:
            driver = self._setup_driver()
            marca_url = BRAND_URLS.get(brand)
            if not marca_url:
                logger.warning(f"URL não encontrada para marca: {brand}")
                return []

            driver.get(marca_url)

            # Aguardar carregamento com timeout reduzido
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, self.cards_xpath))
                )
            except TimeoutException:
                logger.warning(f"Timeout aguardando cards da marca {brand}")
                return []

            # Tentar salvar logo da marca
            if save_logo:
                try:
                    # Encontrar índice da marca para o nome do arquivo
                    try:
                        brand_idx = BRANDS.index(brand) + 1
                    except ValueError:
                        brand_idx = 99
                    
                    logo_selectors = [
                        f"img[alt*='{brand}']",
                        f"img[src*='{brand}']",
                        ".brand-logo img",
                        ".logo img"
                    ]

                    logo_url = None
                    for selector in logo_selectors:
                        try:
                            logo_element = driver.find_element(By.CSS_SELECTOR, selector)
                            logo_url = logo_element.get_attribute("src")
                            if logo_url: break
                        except: continue

                    if logo_url:
                        self._save_brand_logo(logo_url, brand_idx)
                except Exception as e:
                    logger.debug(f"Erro ao salvar logo da marca {brand}: {e}")

            # Scroll otimizado para esta marca
            cards = self._collect_cards_for_driver(driver, max_items, timeout)

            # Extrair dados em lote
            dados_marca = self._extract_cards_batch(cards)

            logger.info(f"Marca {brand}: {len(dados_marca)} cards coletados")
            return dados_marca

        except Exception as e:
            logger.warning(f"Erro ao processar marca {brand}: {e}")
            return []
        finally:
            if driver:
                driver.quit()

    def _collect_cards_for_driver(self, driver, max_items: int, timeout: int) -> List:
        """
        Coleta cards usando um driver específico (para paralelização)
        """
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException

        start_time = time.time()
        cards = []
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        no_new_cards_count = 0

        while (len(cards) < max_items and
               (time.time() - start_time) < timeout and
               scroll_attempts < SCROLL_ATTEMPTS_MAX and
               no_new_cards_count < 3):

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            try:
                WebDriverWait(driver, 1).until(
                    lambda d: d.execute_script("return document.body.scrollHeight") > last_height
                )
            except TimeoutException:
                pass

            new_height = driver.execute_script("return document.body.scrollHeight")
            current_cards = driver.find_elements(By.XPATH, self.cards_xpath)

            if len(current_cards) > len(cards):
                cards = current_cards
                no_new_cards_count = 0
            else:
                no_new_cards_count += 1

            last_height = new_height
            scroll_attempts += 1

        return cards[:max_items]

    def scrape_cars_simple(self, max_items: int = 5, timeout: int = 30) -> List[List[str]]:
        """
        Versão simplificada do scraping para debug
        """
        logger.info("Iniciando scraping simplificado...")
        try:
            self.driver = self._setup_driver()
            self.driver.get(MOVIDA_URL)

            # Aguardar carregamento mínimo
            time.sleep(3)
            logger.info(f"Página carregada: {self.driver.title}")

            # Tentar encontrar cards com diferentes abordagens
            cards = self.driver.find_elements(By.XPATH, self.cards_xpath)
            logger.info(f"Cards encontrados com XPath: {len(cards)}")

            if len(cards) == 0:
                # Tentar outros seletores
                cards = self.driver.find_elements(By.CSS_SELECTOR, "a")
                cards = [c for c in cards if "card" in c.get_attribute("class") or "car" in c.get_attribute("class")]
                logger.info(f"Cards encontrados com CSS alternativo: {len(cards)}")

            # Limitar a poucos cards para teste
            cards = cards[:max_items]

            dados = []
            for i, card in enumerate(cards):
                try:
                    texto = card.text.split('\n')
                    dados.append(texto)
                    logger.info(f"Card {i+1}: {len(texto)} linhas")
                except Exception as e:
                    logger.warning(f"Erro processando card {i}: {e}")

            logger.info(f"Scraping simplificado concluído: {len(dados)} dados")
            return dados

        except Exception as e:
            logger.error(f"Erro no scraping simplificado: {e}")
            return []
        finally:
            if self.driver:
                self.driver.quit()

    def scrape_cars_by_brand(self, max_items_per_brand: int = 100, timeout: int = 60) -> List[List[str]]:
        """
        Scraping organizado por marcas para maior diversidade e eficiência

        Args:
            max_items_per_brand: Máximo de items por marca
            timeout: Timeout em segundos por marca

        Returns:
            Lista consolidada de dados de todas as marcas
        """
        # Lista de todas as marcas disponíveis no site
        marcas = BRANDS

        try:
            logger.info("Iniciando scraping da Movida por marcas")
            self.driver = self._setup_driver()
            self.driver.get(MOVIDA_URL)

            # Aguardar carregamento inicial
            time.sleep(INITIAL_PAGE_LOAD_DELAY)
            logger.info("Página carregada, iniciando coleta por marcas")

            dados_totais = []

            for marca_idx, marca in enumerate(marcas, 1):
                logger.info(f"Coletando dados para marca {marca_idx}: {marca}")

                # Navegar diretamente para a página da marca usando URL específica
                marca_url = BRAND_URLS.get(marca)
                if not marca_url:
                    logger.warning(f"URL não encontrada para marca: {marca}")
                    continue
                    
                try:
                    self.driver.get(marca_url)
                    time.sleep(BRAND_PAGE_LOAD_DELAY)  # Aguardar carregamento da página da marca
                    logger.info(f"Navegou para: {marca_url}")
                except Exception as e:
                    logger.warning(f"Erro ao navegar para marca {marca}: {e}")
                    continue

                # Tentar salvar logo da marca (se disponível na página)
                try:
                    # Procurar por logos na página da marca
                    logo_selectors = [
                        "img[alt*='" + marca + "']",
                        "img[src*='" + marca + "']",
                        ".brand-logo img",
                        ".logo img"
                    ]

                    logo_url = None
                    for selector in logo_selectors:
                        try:
                            logo_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                            logo_url = logo_element.get_attribute("src")
                            if logo_url:
                                break
                        except:
                            continue

                    if logo_url:
                        self._save_brand_logo(logo_url, marca_idx)
                        logger.info(f"Logo salva para marca {marca}")
                    else:
                        logger.warning(f"Logo não encontrada para marca {marca}")

                except Exception as e:
                    logger.warning(f"Erro ao salvar logo da marca {marca}: {e}")

                # Coletar cards para esta marca
                cards = self._collect_cards_optimized(max_items_per_brand, timeout)
                dados_marca = self._extract_cards_batch(cards)
                
                dados_totais.extend(dados_marca)
                logger.info(f"Marca {marca} ({marca_idx}): {len(dados_marca)} cards coletados")

                # Se já atingiu o limite total solicitado (aproximadamente)
                # Note: max_items_per_brand já é um limite por marca, mas se quisermos
                # um limite global, teríamos que passar aqui. 
                # Por enquanto, mantemos por marca como solicitado.

            logger.info(f"Scraping por marcas concluído. Total de dados: {len(dados_totais)}")
            return dados_totais

        except Exception as e:
            logger.error(f"Erro durante scraping por marcas: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Driver fechado")

    def scrape_cars_parallel(self, max_items_per_brand: int = 100, timeout: int = 60, max_workers: int = 4) -> List[List[str]]:
        """
        Scraping paralelo por marcas para máxima performance

        Args:
            max_items_per_brand: Máximo de items por marca
            timeout: Timeout em segundos por marca
            max_workers: Número de instâncias simultâneas do navegador

        Returns:
            Lista consolidada de dados de todas as marcas
        """
        marcas = BRANDS
        dados_totais = []

        logger.info(f"Iniciando scraping paralelo com {max_workers} workers para {len(marcas)} marcas")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Mapear tarefas para cada marca
            futures = {
                executor.submit(self._scrape_single_brand, marca, max_items_per_brand, timeout): marca 
                for marca in marcas
            }

            for future in as_completed(futures):
                marca = futures[future]
                try:
                    dados_marca = future.result()
                    if dados_marca:
                        dados_totais.extend(dados_marca)
                        logger.info(f"Tarefa concluída para {marca}: {len(dados_marca)} cards")
                    else:
                        logger.warning(f"Nenhum dado retornado para {marca}")
                except Exception as e:
                    logger.error(f"Erro na thread da marca {marca}: {e}")

        logger.info(f"Scraping paralelo concluído. Total: {len(dados_totais)} cards")
        return dados_totais

    def get_page_info(self) -> dict:
        """
        Coleta informações básicas da página sem scroll completo

        Returns:
            Dicionário com informações da página
        """
        try:
            self.driver = self._setup_driver()
            self.driver.get(MOVIDA_URL)
            time.sleep(5)

            # Contar cards iniciais
            initial_cards = self.driver.find_elements(By.XPATH, self.cards_xpath)

            # Tentar scroll básico
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)
            after_scroll_cards = self.driver.find_elements(By.XPATH, self.cards_xpath)

            info = {
                'url': MOVIDA_URL,
                'cards_iniciais': len(initial_cards),
                'cards_apos_scroll': len(after_scroll_cards),
                'timestamp': time.time()
            }

            logger.info(f"Info da página: {info}")
            return info

        except Exception as e:
            logger.error(f"Erro ao coletar info da página: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()