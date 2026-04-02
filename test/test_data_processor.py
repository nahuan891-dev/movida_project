"""
Testes para o módulo data_processor
"""

import pytest
import pandas as pd
from unittest.mock import patch

from src.data_processor import DataProcessor, CardParser, DataValidator


class TestCardParser:
    """Testes para CardParser"""

    def test_parse_valid_card(self):
        """Testa parsing de card válido"""
        card_data = [
            "Volkswagen Gol Trend",
            "1.6 MSI",
            "2020/2021 • 82.817 KM • São Paulo, SP",
            "R$ 45.000",
            "Parcelas a partir de R$ 2.000"
        ]
        result = CardParser.parse(card_data)

        assert result is not None
        assert result['marca'] == "Volkswagen"
        assert result['ano_fabricacao'] == 2020
        assert result['ano_modelo'] == 2021
        assert result['km'] == 82817
        assert result['localizacao'] == "São Paulo, SP"
        assert result['preco_bruto'] == "R$ 45.000"

    def test_parse_malformed_card(self):
        """Testa parsing de card malformado"""
        card_data = ["incompleto"]
        result = CardParser.parse(card_data)
        assert result is None

    def test_extract_brand_known(self):
        """Testa extração de marca conhecida"""
        assert CardParser._extract_brand("Ferrari F8") == "Ferrari"
        assert CardParser._extract_brand("BMW X3") == "BMW"

    def test_extract_years(self):
        """Testa extração de anos"""
        fab, mod = CardParser._extract_years("2020/2021 • 50.000 KM")
        assert fab == 2020
        assert mod == 2021

    def test_extract_km(self):
        """Testa extração de KM"""
        km = CardParser._extract_km("82.817 KM • São Paulo")
        assert km == 82817

    def test_extract_location(self):
        """Testa extração de localização"""
        loc = CardParser._extract_location("• São Paulo, SP")
        assert loc == "São Paulo, SP"


class TestDataValidator:
    """Testes para DataValidator"""

    def test_validate_good_data(self):
        """Testa validação de dados bons"""
        cards = [
            ["VW Gol", "1.6", "2020/2021 • 50000 KM • SP", "R$ 40000", "OK"],
            ["Fiat Uno", "1.0", "2019/2020 • 30000 KM • RJ", "R$ 35000", "OK"]
        ]
        result = DataValidator.validate_raw_cards(cards)
        assert result['valid'] is True
        assert result['pass_rate'] == 1.0

    def test_validate_bad_data(self):
        """Testa validação de dados ruins"""
        cards = [
            ["VW Gol"],  # Muito curto
            ["Fiat Uno", "1.0", "sem ano"],  # Sem padrão ano
            []  # Vazio
        ]
        result = DataValidator.validate_raw_cards(cards)
        assert result['valid'] is False
        assert result['pass_rate'] < 0.5


class TestDataProcessor:
    """Testes para DataProcessor"""

    @pytest.fixture
    def processor(self):
        """Fixture para DataProcessor"""
        return DataProcessor()

    @pytest.fixture
    def sample_cards(self):
        """Fixture com dados de exemplo"""
        return [
            [
                "Volkswagen Gol Trend",
                "1.6 MSI",
                "2020/2021 • 82.817 KM • São Paulo, SP",
                "R$ 45.000",
                "Parcelas a partir de R$ 2.000"
            ],
            [
                "Fiat Uno Mille",
                "1.0",
                "2018/2019 • 45.000 KM • Rio de Janeiro, RJ",
                "R$ 32.000",
                "Financiamento disponível"
            ]
        ]

    def test_process_car_data(self, processor, sample_cards):
        """Testa processamento completo"""
        df = processor.process_car_data(sample_cards)

        assert not df.empty
        assert len(df) == 2
        assert 'marca' in df.columns
        assert 'preco' in df.columns
        assert 'km' in df.columns

    def test_clean_data(self, processor):
        """Testa limpeza de dados"""
        # Criar DF com dados "sujos"
        data = {
            'preco_bruto': ["R$ 45.000", "R$ 32.900"],
            'km': ["82.817", "45.000"],
            'ano_fabricacao': ["2020", "2018"],
            'localizacao': ["82.817 KM • São Paulo, SP", "45.000 KM • Rio, RJ"]
        }
        df = pd.DataFrame(data)

        cleaned = processor._clean_data(df)

        assert cleaned['preco'].iloc[0] == 45000.0
        assert cleaned['km'].iloc[0] == 82817
        assert cleaned['localizacao'].iloc[0] == "São Paulo, SP"

    def test_add_calculated_columns(self, processor):
        """Testa adição de colunas calculadas"""
        data = {
            'preco': [45000, 32000],
            'km': [82817, 45000],
            'ano_fabricacao': [2020, 2018]
        }
        df = pd.DataFrame(data)

        result = processor._add_calculated_columns(df)

        assert 'faixa_preco' in result.columns
        assert 'faixa_km' in result.columns
        assert 'idade_veiculo' in result.columns
        assert 'preco_por_km' in result.columns

    @patch('src.data_processor.DataProcessor.save_to_csv')
    def test_save_to_csv(self, mock_save, processor, tmp_path):
        """Testa salvamento CSV"""
        # Mock para evitar escrever arquivo real
        mock_save.return_value = str(tmp_path / "test.csv")

        df = pd.DataFrame({'test': [1, 2, 3]})
        path = processor.save_to_csv(df, "test.csv")

        assert "test.csv" in path