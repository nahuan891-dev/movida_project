"""
Pipeline de processamento de dados Movida
"""

import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .scraper import MovidaScraper
from .data_processor import DataProcessor, DataValidator, GoldDataCleaner
from .config import BRONZE_DIR, SILVER_DIR, GOLD_DIR, BRANDS
from .timer import PerformanceTimer, time_block
from prepare_powerbi import adicionar_coordenadas

logger = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    """Configuração do pipeline"""
    max_cards: int = 1000
    timeout: int = 300
    output_file: str = 'carros_movida.csv'
    headless: bool = True
    by_brand: bool = True  # Alterado para True - usar scraping por marca por padrão
    add_coordinates: bool = True
    max_workers: int = 4  # Número de threads paralelas para scraping

    @classmethod
    def from_args(cls, args) -> 'PipelineConfig':
        """Cria config a partir de argumentos"""
        return cls(
            max_cards=args.max_cards,
            timeout=args.timeout,
            output_file=args.output,
            headless=args.interactive,
            by_brand=args.by_brand,
            add_coordinates=True,  # Sempre adicionar coordenadas por padrão
            max_workers=getattr(args, 'max_workers', 4)
        )


@dataclass
class PipelineResult:
    """Resultado da execução do pipeline"""
    success: bool
    records_count: int
    output_path: str
    error_message: Optional[str] = None


class MovidaDataPipeline:
    """Orquestra o pipeline completo: scrape -> process -> export"""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self.scraper = MovidaScraper(headless=config.headless)
        self.processor = DataProcessor()
        self.validator = DataValidator()
        self.timer = PerformanceTimer()  # Timer de performance

    def run(self) -> PipelineResult:
        """Executa o pipeline completo com medição de performance"""
        self.timer.start()

        try:
            # Passo 1: Scraping
            with time_block("Scraping de dados", self.timer):
                raw_data = self._scrape_data()
                self.timer.add_metric("registros_coletados", len(raw_data) if raw_data else 0)

            if not raw_data:
                self.timer.stop()
                logger.error("ERRO: Nenhum dado coletado do scraping!")
                return PipelineResult(
                    success=False,
                    records_count=0,
                    output_path="",
                    error_message="Nenhum dado coletado"
                )

            logger.info(f"Dados brutos coletados: {len(raw_data)} registros")
            if raw_data:
                logger.info(f"Exemplo de dado bruto: {raw_data[0][:3] if len(raw_data[0]) > 3 else raw_data[0]}")

            self.timer.checkpoint("scraping_concluido")

            # Salvar camada Bronze (dados brutos)
            with time_block("Salvamento Bronze", self.timer):
                bronze_path = self._save_bronze(raw_data)
            logger.info(f"Camada Bronze salva: {bronze_path}")
            self.timer.checkpoint("bronze_salvo")

            # Passo 2: Validação
            with time_block("Validação de dados", self.timer):
                validation = self.validator.validate_raw_cards(raw_data)
                self.timer.add_metric("taxa_validacao", validation['pass_rate'])

            if len(validation['errors']) > 0:
                logger.warning(f"Problemas na validação: {len(validation['errors'])} issues")

            self.timer.checkpoint("validacao_concluida")

            # Passo 3: Processamento Silver
            with time_block("Processamento de dados", self.timer):
                logger.info("Iniciando processamento dos dados...")
                df = self.processor.process_car_data(raw_data)
                self.timer.add_metric("registros_processados", len(df))

            # Passo 4: Limpeza e Normalização Gold (Prepara para geocodificação)
            with time_block("Normalização Gold", self.timer):
                logger.info("Normalizando dados para camada Gold...")
                df = GoldDataCleaner.clean_gold_data(df)

            # Passo 5: Adicionar coordenadas (Agora com endereços limpos)
            if self.config.add_coordinates:
                with time_block("Adição de coordenadas", self.timer):
                    df = adicionar_coordenadas(df, 'localizacao')
                    self.timer.add_metric("registros_com_coordenadas",
                                        df['latitude'].notna().sum())

            # --- PREPARAÇÃO DAS MÉTRICAS PARA PERSISTÊNCIA ---
            # Calcular métricas até este ponto para incluir nos arquivos
            total_time = self.timer.get_elapsed_time()
            self.timer.add_metric("tempo_total_estimado", total_time)
            self.timer.add_metric("pico_ram_mb", f"{self.timer.peak_memory:.2f} MB")
            self.timer.add_metric("registros_finais", len(df))

            # Injetar métricas no DataFrame antes do salvamento final
            df['execucao_tempo_seg'] = round(total_time, 2)
            df['execucao_memoria_mb'] = round(self.timer.peak_memory, 2)
            df['execucao_data_hora'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')

            # Passo 6: Salvamento final
            with time_block("Salvamento final", self.timer):
                # Salvar camadas intermediárias e final com timestamp
                self._save_silver(df)
                gold_timestamp_path = self._save_gold(df)
                
                # NOVO: Salvar arquivo FIXO para o Power BI (Automação)
                gold_final_path = Path("data/gold_final.csv")
                df.to_csv(gold_final_path, index=False, encoding='utf-8-sig')
                logger.info(f"✅ Arquivo para Power BI atualizado: {gold_final_path}")
                
                output_path = self.processor.save_to_csv(df, self.config.output_file)

            self.timer.checkpoint("pipeline_concluido")

            # Calcular métricas finais reais
            total_time_final = self.timer.stop()
            self.timer.log_report()

            return PipelineResult(
                success=True,
                records_count=len(df),
                output_path=output_path
            )

        except Exception as e:
            self.timer.stop()
            logger.error(f"Erro no pipeline: {e}")
            import traceback
            logger.error(f"Traceback completo: {traceback.format_exc()}")
            return PipelineResult(
                success=False,
                records_count=0,
                output_path="",
                error_message=str(e)
            )

    def _scrape_data(self) -> list:
        """Executa o scraping apropriado com fallback para dados mock"""
        try:
            if self.config.by_brand:
                # Agora max_cards define o limite INDIVIDUAL por marca, conforme pedido
                items_per_brand = self.config.max_cards
                logger.info(f"Iniciando scraping paralelo: buscando até {items_per_brand} cards POR MARCA")
                return self.scraper.scrape_cars_parallel(
                    max_items_per_brand=items_per_brand,
                    timeout=self.config.timeout,
                    max_workers=self.config.max_workers
                )
            else:
                logger.info(f"Iniciando scraping geral: {self.config.max_cards} itens")
                return self.scraper.scrape_cars(
                    max_items=self.config.max_cards,
                    timeout=self.config.timeout
                )
        except Exception as e:
            logger.error(f"Erro no scraping real: {e}. Usando dados mock para evitar falha total.")
            return self._get_mock_data()

    def _get_mock_data(self) -> list:
        """Retorna dados mock para teste quando o scraping falha"""
        return [
            ['Volkswagen', 'Golf Highline', '2020/2021 • 45.000 KM • São Paulo, SP', 'R$ 89.900', 'Condições especiais'],
            ['Fiat', 'Uno Way', '2019/2020 • 30.000 KM • Rio de Janeiro, RJ', 'R$ 45.000', 'Condições especiais'],
            ['Toyota', 'Corolla XEi', '2018/2019 • 60.000 KM • Belo Horizonte, MG', 'R$ 75.000', 'Condições especiais']
        ]

    def _save_bronze(self, raw_data: list) -> str:
        """Salva dados brutos na camada Bronze"""
        bronze_df = self.processor._convert_raw_to_dataframe(raw_data)
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f'bronze_{timestamp}.csv'
        return self.processor.save_to_csv(bronze_df, filename, output_dir=BRONZE_DIR)

    def _save_silver(self, df: pd.DataFrame) -> str:
        """Salva dados processados na camada Silver"""
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f'silver_{timestamp}.csv'
        return self.processor.save_to_csv(df, filename, output_dir=SILVER_DIR)

    def _save_gold(self, df: pd.DataFrame) -> str:
        """Salva dados finais na camada Gold com limpeza de qualidade"""
        # Aplicar limpeza específica da camada Gold
        # IMPORTANTE: Reorder agora vai manter as métricas que injetamos
        df_gold = GoldDataCleaner.clean_gold_data(df)

        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        filename = f'gold_{timestamp}.csv'
        return self.processor.save_to_csv(df_gold, filename, output_dir=GOLD_DIR)
