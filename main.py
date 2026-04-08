#!/usr/bin/env python3
"""
Movida Scraper - Coletor de dados de carros seminovos
Arquivo principal para execução do scraper
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from src import pipeline

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.config import BRANDS
from src.prepare_powerbi import adicionar_coordenadas
from src.scraper import MovidaScraper
from src.data_processor import DataProcessor
from src.pipeline import MovidaDataPipeline, PipelineConfig
from src.utils import setup_logging, print_data_summary, validate_data_quality
from src.health_check import ServiceHealth


def main():
    """Função principal"""
    # ... (parser definitions remain the same)
    parser = argparse.ArgumentParser(
        description="Movida Scraper - Coleta dados de carros seminovos",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

# Coletar 1000 carros por marca (padrão)
python main.py

# Coletar apenas 100 carros por marca
python main.py --max-cards 100

# Executar em modo headless (sem interface)
python main.py --interactive

# Salvar com nome personalizado
python main.py --output meus_carros.csv

# Apenas testar conectividade
python main.py --test-only

# Coletar da página geral (não recomendado)
python main.py --no-by-brand
        """
    )

    parser.add_argument(
        '--max-cards',
        type=int,
        default=1000,
        help='Número máximo de carros a coletar (padrão: 1000)'
    )

    parser.add_argument(
        '--timeout',
        type=int,
        default=1800,
        help='Timeout em segundos para coleta (padrão: 1800)'
    )

    parser.add_argument(
        '--output',
        type=str,
        default='carros_movida.csv',
        help='Nome do arquivo de saída (padrão: carros_movida.csv)'
    )

    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Executar em modo headless (sem interface gráfica)'
    )

    parser.add_argument(
        '--test-only',
        action='store_true',
        help='Apenas testar conectividade, não coletar dados completos'
    )

    parser.add_argument(
        '--by-brand',
        action='store_true',
        default=True,  # Agora é True por padrão
        help='Coletar carros organizados por marca (padrão: True)'
    )

    parser.add_argument(
        '--no-by-brand',
        action='store_false',
        dest='by_brand',
        help='Coletar da página geral (não recomendado)'
    )

    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Log detalhado (DEBUG)'
    )

    args = parser.parse_args()

    # Configurar logging
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(log_level)

    print("🚗 Movida Scraper v1.0.0")
    print("=" * 50)

    try:
        if args.test_only:
            # Apenas testar conectividade
            print("🧪 Executando teste de conectividade...")
            scraper = MovidaScraper(headless=not args.interactive)
            info = scraper.get_page_info()

            print("✅ Teste concluído!")
            print(f"📊 Cards iniciais: {info['cards_iniciais']}")
            print(f"📈 Cards após scroll: {info['cards_apos_scroll']}")
            return

        # Executar coleta completa
        print(f"🎯 Iniciando coleta de até {args.max_cards} carros...")
        print(f"⏱️  Timeout: {args.timeout} segundos")
        print(f"📁 Arquivo de saída: {args.output}")
        print()

        # Passo ITIL: Health Check (Gestão de Eventos)
        health = ServiceHealth(MovidaScraper(headless=True))
        if not health.run_full_diagnostic():
            print("\n⚠️  AVISO: Diagnóstico de saúde detectou problemas.")
            cont = input("Deseja continuar mesmo assim? (s/N): ")
            if cont.lower() != 's':
                print("🛑 Execução cancelada pelo usuário (Prevenção de Incidente).")
                return 0

        # Criar configuração e executar pipeline
        config = PipelineConfig.from_args(args)
        pipeline = MovidaDataPipeline(config)
        result = pipeline.run()

        if not result.success:
            print(f"❌ Erro no pipeline: {result.error_message}")
            return 1

        # Carregar dados para estatísticas
        df = pd.read_csv(result.output_path)

        # Estatísticas finais
        print_data_summary(df)

        # Validação de qualidade (SLA - Gestão de Nível de Serviço)
        quality = validate_data_quality(df)
        print("\n📋 Relatório de SLA do Serviço:")
        
        sla_status = "✅ DENTRO DO SLA" if quality['perc_preco'] >= 90 else "⚠️  SLA VIOLADO (Dados Incompletos)"
        print(f"📊 Status do Serviço: {sla_status}")
        
        print(f"💰 Preenchimento de Preços: {quality['perc_preco']:.1f}% (Meta: 90%)")
        print(f"📍 Preenchimento de Localização: {quality['perc_local']:.1f}%")
        print(f"🔄 Registros duplicados: {quality['duplicatas']}")
        print(f"📊 Total de registros: {result.records_count}")

        # Mostrar caminhos das camadas
        print("\n📁 Arquivos das camadas salvos:")
        print(f"🟫 Bronze: data/bronze/bronze_*.csv")
        print(f"🟨 Silver: data/silver/silver_*.csv")
        print(f"🟨 Gold:   data/gold/gold_*.csv")
        print(f"\n🚀 AUTOMATIZAÇÃO POWER BI:")
        print(f"📍 Use este arquivo como fonte: data/gold_final.csv")
        print(f"💡 (No Power BI, basta clicar em 'Atualizar' agora!)")

    except KeyboardInterrupt:
        print("\n⏹️  Execução interrompida pelo usuário")
    except Exception as e:
        print(f"\n❌ Erro durante execução: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
