"""
Módulo para processamento e salvamento dos dados coletados
"""

import os
import re
import logging
from typing import List, Optional, Dict, Any, Tuple
from pathlib import Path

import pandas as pd

from .config import (
    DATA_COLUMNS, PRECO_BINS, PRECO_LABELS, KM_BINS, KM_LABELS,
    OUTPUT_DIR, DEFAULT_FILENAME, ENCODING,
    BRONZE_DIR, SILVER_DIR, GOLD_DIR,
    BRONZE_PREFIX, SILVER_PREFIX, GOLD_PREFIX,
    BRAZIL_STATES, MAJOR_CITIES
)

logger = logging.getLogger(__name__)


class GoldDataCleaner:
    """Classe para limpeza e melhoria da qualidade dos dados na camada Gold"""

    @staticmethod
    def clean_gold_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e corrige dados da camada Gold para melhor qualidade

        Args:
            df: DataFrame da camada Silver com coordenadas

        Returns:
            DataFrame limpo e corrigido para camada Gold
        """
        logger.info(f"Iniciando limpeza da camada Gold. Registros: {len(df)}")

        # Criar cópia para não modificar o original
        df_clean = df.copy()

        # 1. Extrair localização robusta do texto_completo se necessário
        df_clean = GoldDataCleaner._extract_location_from_text(df_clean)

        # 2. Corrigir coluna 'modelo' - remover duplicação da marca
        df_clean = GoldDataCleaner._fix_model_column(df_clean)

        # 3. Corrigir coluna 'versao' - extrair apenas a versão específica
        df_clean = GoldDataCleaner._fix_version_column(df_clean)

        # 4. Limpar coluna 'texto_completo' - remover quebras de linha e formatação
        df_clean = GoldDataCleaner._clean_text_completo(df_clean)

        # 5. Normalizar preco_bruto para padrão 'R$ 00.000'
        df_clean = GoldDataCleaner._normalize_preco_bruto(df_clean)

        # 6. Adicionar 'R$' às faixas de preço
        df_clean = GoldDataCleaner._format_price_tier(df_clean)

        # 7. Tratar valores nulos de latitude/longitude
        df_clean = GoldDataCleaner._handle_missing_coords(df_clean)

        # 8. Reordenar colunas para melhor organização
        df_clean = GoldDataCleaner._reorder_columns(df_clean)

        # 9. Validar dados finais
        df_clean = GoldDataCleaner._validate_gold_data(df_clean)

        logger.info(f"Limpeza da camada Gold concluída. Registros finais: {len(df_clean)}")
        return df_clean

    @staticmethod
    def _extract_location_from_text(df: pd.DataFrame) -> pd.DataFrame:
        """
        Extrai a localização da coluna texto_completo usando Regex 
        e aplica mapeamento de shoppings e divisão de bairro com limpeza de lixo.
        """
        from .utils import traduzir_auto_shopping, limpar_string_localizacao
        
        if 'texto_completo' not in df.columns:
            return df

        def parse_loc(row):
            texto = str(row['texto_completo'])
            
            # 1. Regex para pegar entre KM e R$
            match = re.search(r'KM\s+(.*?)\s+R\$', texto, re.IGNORECASE)
            loc_raw = match.group(1).strip() if match else str(row.get('localizacao', ''))
            
            if not loc_raw or loc_raw.lower() == 'none':
                return row.get('localizacao')

            # 2. Limpar lixo (BAIXO KM, IPVA PAGO, etc)
            loc_limpa = limpar_string_localizacao(loc_raw)

            # 3. Tentar traduzir se for Shopping
            info_shopping = traduzir_auto_shopping(loc_limpa)
            if info_shopping:
                return f"{info_shopping['cidade']}, {info_shopping['estado']}"

            # 4. Tratar caso Cidade Bairro, UF
            if ',' in loc_limpa:
                parts = loc_limpa.rsplit(',', 1)
                address_part = parts[0].strip()
                state = parts[1].strip().upper()
                
                # Tentar identificar a cidade no início
                for city in MAJOR_CITIES:
                    if address_part.lower().startswith(city.lower()):
                        return f"{city}, {state}"

                return f"{address_part}, {state}"
            
            return loc_limpa

        df['localizacao'] = df.apply(parse_loc, axis=1)
        
        # Chamar a normalização padrão para garantir consistência
        df = GoldDataCleaner._normalize_locations(df)
        
        return df

    @staticmethod
    def _normalize_preco_bruto(df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte a coluna preco_bruto em um valor numérico (float)
        removendo 'R$', pontos de milhar e tratando a vírgula decimal.
        """
        if 'preco_bruto' not in df.columns:
            return df

        def clean_price_to_numeric(price_val):
            if pd.isna(price_val):
                return None
            
            # Se já for número, apenas retorna
            if isinstance(price_val, (int, float)):
                return float(price_val)
                
            # Se for string, limpa
            price_str = str(price_val)
            # 1. Remover R$ e espaços
            price_str = re.sub(r'R\$\s*', '', price_str)
            # 2. Remover pontos de milhar
            price_str = price_str.replace('.', '')
            # 3. Trocar vírgula decimal por ponto (se houver)
            price_str = price_str.replace(',', '.')
            
            # 4. Extrair apenas a parte numérica (caso haja lixo no fim)
            match = re.search(r'(\d+\.?\d*)', price_str)
            if match:
                return float(match.group(1))
            
            return None

        df['preco_bruto'] = df['preco_bruto'].apply(clean_price_to_numeric)
        logger.info("Coluna 'preco_bruto' convertida para numérico")
        return df

    @staticmethod
    def _format_price_tier(df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona o símbolo R$ às etiquetas de faixa de preço"""
        if 'faixa_preco' not in df.columns:
            return df
        
        # Converter para string para manipulação
        df['faixa_preco'] = df['faixa_preco'].astype(str)
        
        def format_tier(tier):
            if pd.isna(tier) or tier == 'nan':
                return tier
            # Se já tem R$, não faz nada
            if 'R$' in tier:
                return tier
            
            # Formata padrões como "50k-80k" -> "R$ 50k - R$ 80k"
            # Formata padrões como "Até 30k" -> "Até R$ 30k"
            # Primeiro o "Até" para evitar duplicação pelo regex seguinte
            if tier.startswith('Até '):
                tier = tier.replace('Até ', 'Até R$ ')
            else:
                # regex para adicionar R$ em todas as ocorrências de \d+k
                tier = re.sub(r'(\d+k)', r'R$ \1', tier)
            
            tier = tier.replace('-', ' - ')
            return tier

        df['faixa_preco'] = df['faixa_preco'].apply(format_tier)
        logger.info("Coluna 'faixa_preco' formatada com R$")
        return df

    @staticmethod
    def _handle_missing_coords(df: pd.DataFrame) -> pd.DataFrame:
        """Trata valores nulos de latitude e longitude"""
        if 'latitude' not in df.columns or 'longitude' not in df.columns:
            return df
        
        # Preencher nulos com 0 ou valor padrão se necessário para o Power BI
        # Aqui vamos apenas registrar e garantir que sejam tipos consistentes
        null_count = df['latitude'].isnull().sum()
        if null_count > 0:
            logger.info(f"Tratando {null_count} registros sem coordenadas")
            # Opcional: remover se forem essenciais para o mapa, ou manter para filtros
            # Por enquanto, vamos manter para não perder dados de preço/modelo
            pass
            
        return df

    @staticmethod
    def _normalize_locations(df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza a coluna localização para o formato 'Cidade, Estado'"""
        if 'localizacao' not in df.columns:
            return df

        def normalize_loc(loc_str):
            if pd.isna(loc_str) or not isinstance(loc_str, str):
                return loc_str
            
            loc_str = loc_str.strip()
            if not loc_str:
                return loc_str

            # Tentar extrair "Cidade, Estado"
            if ',' in loc_str:
                parts = loc_str.rsplit(',', 1)
                city_part = parts[0].strip()
                state_part = parts[1].strip().upper()

                # Verificar se o estado é válido
                if state_part not in BRAZIL_STATES:
                    # Tentar encontrar o estado se ele estiver grudado ou errado
                    for state in BRAZIL_STATES:
                        if state_part.endswith(state):
                            state_part = state
                            break

                # 1. Tentar encontrar cidade usando MAJOR_CITIES (correspondência exata ou início)
                for city in MAJOR_CITIES:
                    if city_part.lower().startswith(city.lower()) or city.lower() in city_part.lower():
                        return f"{city}, {state_part}"
                
                # 2. Se não encontrou nas major cities, tenta limpar o city_part
                # Remover prefixos comuns do scraping da Movida
                city_part = re.sub(r'^(Auto\s+Shopping|Shopping|Auto)\s+', '', city_part, flags=re.IGNORECASE)
                
                # Remover sufixos de endereços/bairros
                city_part = re.sub(r'\s+(Av\.|Rua|Bairro|Vila|Jardim|Miguel|Pereira).*$', '', city_part, flags=re.IGNORECASE)
                
                return f"{city_part.strip()}, {state_part}"
            
            return loc_str

        initial_locs = df['localizacao'].copy()
        df['localizacao'] = df['localizacao'].apply(normalize_loc)
        
        changed = (initial_locs != df['localizacao']).sum()
        logger.info(f"Localizações normalizadas: {changed} registros alterados")
        
        return df

    @staticmethod
    def _fix_model_column(df: pd.DataFrame) -> pd.DataFrame:
        """Corrige a coluna modelo removendo duplicação da marca"""
        if 'modelo' not in df.columns or 'marca' not in df.columns:
            return df

        def extract_model(row):
            marca = str(row['marca']).lower().strip()
            modelo_atual = str(row['modelo']).strip()

            # Se o modelo começa com a marca, remover a marca
            if modelo_atual.lower().startswith(marca):
                # Remover a marca e espaços extras
                modelo_limpo = modelo_atual[len(marca):].strip()
                # Se ainda tiver a marca no início (caso insensitive), tentar novamente
                if modelo_limpo.lower().startswith(marca):
                    modelo_limpo = modelo_limpo[len(marca):].strip()
                return modelo_limpo if modelo_limpo else modelo_atual
            else:
                return modelo_atual

        df['modelo'] = df.apply(extract_model, axis=1)
        logger.info("Coluna 'modelo' corrigida")
        return df

    @staticmethod
    def _fix_version_column(df: pd.DataFrame) -> pd.DataFrame:
        """Corrige a coluna versao extraindo apenas a versão específica"""
        if 'versao' not in df.columns or 'modelo' not in df.columns:
            return df

        def extract_version(row):
            modelo = str(row['modelo']).strip()
            versao_atual = str(row['versao']).strip()

            # Se a versão contém o modelo, extrair apenas a parte específica
            if modelo and versao_atual.lower().startswith(modelo.lower()):
                versao_limpa = versao_atual[len(modelo):].strip()
                # Remover caracteres especiais do início
                versao_limpa = versao_limpa.lstrip(' -•').strip()
                return versao_limpa if versao_limpa else versao_atual
            else:
                return versao_atual

        df['versao'] = df.apply(extract_version, axis=1)
        logger.info("Coluna 'versao' corrigida")
        return df

    @staticmethod
    def _clean_text_completo(df: pd.DataFrame) -> pd.DataFrame:
        """Limpa a coluna texto_completo removendo quebras de linha e formatação"""
        if 'texto_completo' not in df.columns:
            return df

        def clean_text(text):
            if pd.isna(text):
                return text

            # Converter para string se não for
            text = str(text)

            # Remover quebras de linha e múltiplos espaços
            text = re.sub(r'\n+', ' ', text)
            text = re.sub(r'\s+', ' ', text)

            # Remover formatação especial como •
            text = re.sub(r'•', '', text)

            return text.strip()

        df['texto_completo'] = df['texto_completo'].apply(clean_text)
        logger.info("Coluna 'texto_completo' limpa")
        return df

    @staticmethod
    def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Reordena as colunas para a sequência exata solicitada pelo usuário"""

        # Ordem desejada das colunas (Exatamente como pedido + compra_online + métricas)
        desired_order = [
            'id', 'marca', 'modelo', 'versao', 'ano_fabricacao', 'ano_modelo',
            'km', 'localizacao', 'compra_online', 'preco_bruto', 'preco', 'faixa_preco', 'faixa_km',
            'idade_veiculo', 'preco_por_km', 'latitude', 'longitude',
            'texto_completo', 'data_coleta',
            'execucao_tempo_seg', 'execucao_memoria_mb', 'execucao_data_hora'
        ]

        # Garantir que todas as colunas existem (adicionar como nulo se faltar)
        for col in desired_order:
            if col not in df.columns:
                df[col] = None

        # Reordenar mantendo apenas as solicitadas nessa ordem
        df = df[desired_order]
        logger.info("Colunas reordenadas conforme solicitação")
        return df

    @staticmethod
    def _validate_gold_data(df: pd.DataFrame) -> pd.DataFrame:
        """Valida a qualidade dos dados finais da camada Gold"""

        # Remover registros com dados críticos faltando
        critical_columns = ['marca', 'modelo', 'preco']
        initial_count = len(df)

        for col in critical_columns:
            if col in df.columns:
                df = df[df[col].notna() & (df[col] != '')]

        final_count = len(df)

        if initial_count != final_count:
            logger.warning(f"Removidos {initial_count - final_count} registros com dados críticos faltando")

        # Validar coordenadas
        if 'latitude' in df.columns and 'longitude' in df.columns:
            valid_coords = df['latitude'].notna() & df['longitude'].notna()
            coords_count = valid_coords.sum()
            logger.info(f"Registros com coordenadas válidas: {coords_count}/{len(df)}")

        # Verificar se existem arquivos Gold anteriores para validação cruzada
        try:
            gold_files = sorted(Path(GOLD_DIR).glob('gold_*.csv'))
            if gold_files:
                latest_gold = gold_files[-1]
                logger.info(f"Validando localizações contra arquivo de referência: {latest_gold}")
                GoldDataCleaner._check_locations_consistency(df, latest_gold)
        except Exception as e:
            logger.warning(f"Não foi possível realizar a validação cruzada de localizações: {e}")

        return df

    @staticmethod
    def _check_locations_consistency(df: pd.DataFrame, reference_file: Path) -> None:
        """Verifica se as localizações no DF atual existem no arquivo de referência"""
        try:
            ref_df = pd.read_csv(reference_file)
            if 'localizacao' not in ref_df.columns:
                return

            valid_refs = set(ref_df['localizacao'].unique())
            current_locs = set(df['localizacao'].unique())
            
            new_locs = current_locs - valid_refs
            
            if new_locs:
                logger.info(f"Identificadas {len(new_locs)} novas localizações não presentes no histórico")
                for loc in list(new_locs)[:5]:
                    logger.info(f"Nova localização: {loc}")
            else:
                logger.info("Todas as localizações já foram vistas em coletas anteriores")
                
        except Exception as e:
            logger.error(f"Erro ao checar consistência de localizações: {e}")


class CardParser:
    """Encapsula lógica de parsing de dados dos cards"""

    # Estrutura esperada dos cards
    CARD_STRUCTURE = {
        'marca_modelo': 0,
        'versao': 1,
        'info_line': 2,
        'preco': 3,
        'condicoes': 4
    }

    # Padrões regex para extração
    REGEX_PATTERNS = {
        'ano': re.compile(r'(?P<ano_fab>\d{4})/(?P<ano_mod>\d{4})'),
        'km': re.compile(r'(?P<km>\d+(?:\.\d+)?)\s*KM'),
        'location': re.compile(r'•\s*(?P<local>.+?)(?:\s*,\s*(?P<state>[A-Z]{2}))?$'),
        'price': re.compile(r'R\$\s*(?P<value>[\d.,]+)'),
    }

    KNOWN_BRANDS = [
        'Ferrari', 'Audi', 'BMW', 'Mercedes', 'Porsche', 'Lamborghini',
        'Volkswagen', 'Fiat', 'Renault', 'Hyundai', 'Toyota', 'Jeep',
        'Chevrolet', 'Citroen', 'Peugeot', 'Nissan', 'Volvo', 'Ford',
        'Honda', 'Mini', 'Mitsubishi', 'Caoa Chery'
    ]

    @staticmethod
    def parse(card_data: List[str]) -> Optional[Dict[str, Any]]:
        """
        Parseia dados do card buscando padrões em todas as linhas (mais robusto)
        """
        if len(card_data) < 3:
            return None

        try:
            texto_unido = " ".join(card_data)
            
            # 1. Extrair Marca (sempre a primeira parte do título)
            marca_modelo_raw = card_data[0]
            marca = CardParser._extract_brand(marca_modelo_raw)
            
            # 2. Buscar ano, km e localização em todas as linhas
            ano_fab, ano_mod = None, None
            km = None
            local = None
            
            for line in card_data:
                # Tentar Anos
                if not ano_fab:
                    af, am = CardParser._extract_years(line)
                    if af: ano_fab, ano_mod = af, am
                
                # Tentar KM
                if km is None:
                    k = CardParser._extract_km(line)
                    if k is not None: km = k
                
                # Tentar Localização (geralmente segue o padrão • ou tem a sigla do estado no fim)
                if not local:
                    l = CardParser._extract_location(line)
                    if l: local = l

            # 3. Extrair Preço Bruto
            preco_bruto = ''
            for line in card_data:
                if 'R$' in line and not preco_bruto:
                    # Pegar apenas a parte que contém o preço principal
                    match = re.search(r'R\$\s*[\d\.,]+', line)
                    if match:
                        preco_bruto = match.group(0)
                        break

            # 4. Condições
            condicoes = ''
            for line in card_data:
                if 'parcela' in line.lower() or 'condição' in line.lower() or 'entrada' in line.lower():
                    condicoes = line.strip()
                    break

            return {
                'marca': marca,
                'modelo': marca_modelo_raw,
                'versao': card_data[1] if len(card_data) > 1 else '',
                'ano_fabricacao': ano_fab,
                'ano_modelo': ano_mod,
                'km': km,
                'localizacao': local,
                'preco_bruto': preco_bruto,
                'condicoes': condicoes,
                'texto_completo': ' '.join(card_data).replace('\n', ' ')
            }
        except Exception as e:
            logger.warning(f"Erro ao parsear card: {e}")
            return None

    @staticmethod
    def _validate_structure(card_data: List[str]) -> bool:
        """Valida se o card tem estrutura mínima"""
        return len(card_data) >= len(CardParser.CARD_STRUCTURE) - 1  # Pelo menos 4 campos

    @staticmethod
    def _extract_brand(marca_modelo_str: str) -> Optional[str]:
        """Extrai marca do string marca/modelo"""
        for brand in CardParser.KNOWN_BRANDS:
            if marca_modelo_str.upper().startswith(brand.upper()):
                return brand
        # Fallback: primeira palavra
        return marca_modelo_str.split()[0] if marca_modelo_str else None

    @staticmethod
    def _extract_years(info_line: str) -> Tuple[Optional[int], Optional[int]]:
        """Extrai anos de fabricação e modelo"""
        match = CardParser.REGEX_PATTERNS['ano'].search(info_line)
        if match:
            return int(match.group('ano_fab')), int(match.group('ano_mod'))
        return None, None

    @staticmethod
    def _extract_km(info_line: str) -> Optional[int]:
        """Extrai quilometragem"""
        match = CardParser.REGEX_PATTERNS['km'].search(info_line)
        if match:
            return int(match.group('km').replace('.', ''))
        return None

    @staticmethod
    def _extract_location(info_line: str) -> Optional[str]:
        """Extrai localização"""
        match = CardParser.REGEX_PATTERNS['location'].search(info_line)
        if match:
            local = match.group('local').strip()
            state = match.group('state')
            if state:
                return f"{local}, {state}"
            return local
        return None


class DataProcessor:
    """Classe para processar dados dos carros"""

    def __init__(self, output_dir: str = OUTPUT_DIR):
        """
        Inicializa o processador de dados

        Args:
            output_dir: Diretório para salvar os arquivos
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        logger.info(f"DataProcessor inicializado - Dir: {self.output_dir}")

    def _extract_car_info(self, card_data: List[str]) -> Optional[Dict[str, Any]]:
        """
        Extrai informações estruturadas de um card

        Args:
            card_data: Lista com dados do card

        Returns:
            Dicionário com informações extraídas ou None se inválido
        """
        return CardParser.parse(card_data)

    def process_car_data(self, dados_cards: List[List[str]]) -> pd.DataFrame:
        """
        Processa lista de dados dos cards e retorna DataFrame estruturado

        Args:
            dados_cards: Lista de listas com dados dos cards

        Returns:
            DataFrame pandas com dados processados
        """
        logger.info(f"Processando {len(dados_cards)} cards")

        # Validar dados de entrada
        validation = DataValidator.validate_raw_cards(dados_cards)
        if not validation['valid']:
            logger.warning(f"Qualidade dos dados baixa: {validation['pass_rate']:.1%} válidos")
            for issue in validation['issues'][:5]:
                logger.warning(f"Problema: {issue}")

        registros = []
        for i, card in enumerate(dados_cards):
            try:
                registro = self._extract_car_info(card)
                if registro:  # Só adiciona se conseguiu extrair dados
                    registro['id'] = i + 1
                    registro['data_coleta'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                    registros.append(registro)

                if (i + 1) % 100 == 0:
                    logger.info(f"Processados {i + 1} registros")

            except Exception as e:
                logger.warning(f"Erro ao processar card {i}: {e}")
                continue

        df = pd.DataFrame(registros)

        if df.empty:
            logger.warning("Nenhum dado válido foi processado")
            return df

        # Limpar e converter dados
        df = self._clean_data(df)

        # Adicionar colunas calculadas
        df = self._add_calculated_columns(df)

        logger.info(f"Processamento concluído. Total registros: {len(df)}")
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e converte tipos de dados"""
        logger.info(f"Iniciando limpeza. Colunas: {list(df.columns)}")
        logger.info(f"Tipos iniciais: {df.dtypes.to_dict()}")

        # Preço - regex melhorado para capturar todos os formatos
        try:
            df['preco'] = (
                df['preco_bruto']
                .str.replace(r'R\$\s*', '', regex=True)  # Remove R$ e espaços
                .str.replace(r'\.', '', regex=True)      # Remove pontos de milhar
                .str.replace(',', '.', regex=False)      # Vírgula para ponto decimal
                .str.extract(r'(\d+\.?\d*)')             # Extrai número
                .astype(float)
            )
            logger.info(f"Preço convertido: {df['preco'].dtype}")
        except Exception as e:
            logger.error(f"Erro na conversão de preço: {e}")

        # KM
        try:
            df['km'] = pd.to_numeric(df['km'], errors='coerce')
            logger.info(f"KM convertido: {df['km'].dtype}")
        except Exception as e:
            logger.error(f"Erro na conversão de KM: {e}")

        # Anos
        try:
            df['ano_fabricacao'] = pd.to_numeric(df['ano_fabricacao'], errors='coerce')
            df['ano_modelo'] = pd.to_numeric(df['ano_modelo'], errors='coerce')
            logger.info("Anos convertidos")
        except Exception as e:
            logger.error(f"Erro na conversão de anos: {e}")

        # Melhorar extração de localização (remover KM da string)
        try:
            df['localizacao'] = (
                df['localizacao']
                .str.replace(r'\d+\.\d+\s*KM\s*•\s*', '', regex=True)  # Remove "XX.XXX KM • "
                .str.strip()
            )
            logger.info("Localização limpa")
        except Exception as e:
            logger.error(f"Erro na limpeza de localização: {e}")

        logger.info(f"Tipos finais: {df.dtypes.to_dict()}")
        return df

    def _add_calculated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas calculadas para análise"""
        logger.info(f"Adicionando colunas calculadas. Preços: {df['preco'].dtype}, KM: {df['km'].dtype}")

        # Faixas de preço
        try:
            df['faixa_preco'] = pd.cut(
                df['preco'],
                bins=PRECO_BINS,
                labels=PRECO_LABELS,
                include_lowest=True
            )
            logger.info("Faixa de preço adicionada")
        except Exception as e:
            logger.error(f"Erro na faixa de preço: {e}")

        # Faixas de quilometragem
        try:
            df['faixa_km'] = pd.cut(
                df['km'],
                bins=KM_BINS,
                labels=KM_LABELS,
                include_lowest=True
            )
            logger.info("Faixa de KM adicionada")
        except Exception as e:
            logger.error(f"Erro na faixa de KM: {e}")

        # Idade do veículo (ano atual aproximado)
        ano_atual = pd.Timestamp.now().year
        df['idade_veiculo'] = ano_atual - df['ano_fabricacao']

        # Eficiência preço/KM (evitar divisão por zero ou NaN)
        try:
            df['preco_por_km'] = df.apply(
                lambda row: row['preco'] / row['km'] if pd.notna(row['preco']) and pd.notna(row['km']) and row['km'] > 0 else None,
                axis=1
            )
            logger.info("Preço por KM calculado")
        except Exception as e:
            logger.error(f"Erro no cálculo preço/KM: {e}")

        return df

    def _convert_raw_to_dataframe(self, raw_data: List[List[str]]) -> pd.DataFrame:
        """
        Converte dados brutos em DataFrame mínimo para camada Bronze

        Args:
            raw_data: Lista de listas com dados crus dos cards

        Returns:
            DataFrame com colunas básicas da camada Bronze
        """
        if not raw_data:
            return pd.DataFrame()

        # Criar DataFrame sem especificar colunas para lidar com dados de tamanho variável
        df = pd.DataFrame(raw_data)

        # Padronizar para no máximo 10 colunas (ajuste conforme necessário)
        max_cols = 10
        if df.shape[1] > max_cols:
            df = df.iloc[:, :max_cols]
        elif df.shape[1] < max_cols:
            # Adicionar colunas vazias se necessário
            for i in range(df.shape[1], max_cols):
                df[f'col_{i}'] = ''

        # Renomear colunas para nomes descritivos
        bronze_columns = ['marca_modelo', 'versao', 'info_line', 'preco_bruto', 'condicoes', 'detalhes', 'extra1', 'extra2', 'extra3', 'extra4']
        df.columns = bronze_columns[:df.shape[1]]

        # Adicionar metadados
        df['id'] = range(1, len(df) + 1)
        df['data_coleta'] = pd.Timestamp.now()

        return df

    def save_to_csv(self, df: pd.DataFrame, filename: str = DEFAULT_FILENAME, output_dir: Optional[Path] = None) -> str:
        """
        Salva DataFrame em arquivo CSV

        Args:
            df: DataFrame a ser salvo
            filename: Nome do arquivo
            output_dir: Diretório de saída (padrão: OUTPUT_DIR)

        Returns:
            Caminho completo do arquivo salvo
        """
        if output_dir is None:
            output_dir = self.output_dir
        else:
            output_dir = Path(output_dir)

        filepath = output_dir / filename

        df.to_csv(filepath, index=False, encoding=ENCODING)

        logger.info(f"Dados salvos em {filepath}")
        logger.info(f"Colunas: {list(df.columns)}")
        logger.info(f"Total registros: {len(df)}")

        # Estatísticas básicas
        if 'preco' in df.columns:
            preco_medio = df['preco'].mean()
            logger.info(f"Preço médio: R$ {preco_medio:.2f}")

        if 'km' in df.columns:
            km_medio = df['km'].mean()
            logger.info(f"KM médio: {km_medio:.0f}")

        return str(filepath)

    def get_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Calcula estatísticas básicas dos dados

        Args:
            df: DataFrame com dados dos carros

        Returns:
            Dicionário com estatísticas
        """
        stats = {
            'total_carros': len(df),
            'marcas_unicas': df['marca'].nunique() if 'marca' in df.columns else 0,
            'preco_medio': df['preco'].mean() if 'preco' in df.columns else 0,
            'preco_min': df['preco'].min() if 'preco' in df.columns else 0,
            'preco_max': df['preco'].max() if 'preco' in df.columns else 0,
            'km_medio': df['km'].mean() if 'km' in df.columns else 0,
            'ano_medio': df['ano_fabricacao'].mean() if 'ano_fabricacao' in df.columns else 0,
        }

        return stats

    def parse_texto_completo_simple(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parseia a coluna 'texto_completo' para extrair ano_fabricacao, ano_modelo e km

        Args:
            df: DataFrame com coluna 'texto_completo'

        Returns:
            DataFrame com colunas preenchidas
        """
        def extract_simple(text):
            # Procurar por padrão "ANO/ANO • KM KM • LOCAL"
            match = re.search(r'(\d{4})/(\d{4})\s*•\s*([\d\.]+)\s*KM', text)
            if match:
                ano_fab = int(match.group(1))
                ano_mod = int(match.group(2))
                km = int(match.group(3).replace('.', ''))
                return ano_fab, ano_mod, km
            return None, None, None

        # Aplicar extração
        df[['ano_fabricacao', 'ano_modelo', 'km']] = df['texto_completo'].apply(
            lambda x: pd.Series(extract_simple(x))
        )

        # Recalcular colunas dependentes
        df = self._add_calculated_columns(df)

        return df

    def parse_texto_completo(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parseia a coluna 'texto_completo' para extrair informações adicionais

        Args:
            df: DataFrame com coluna 'texto_completo'

        Returns:
            DataFrame com colunas adicionais preenchidas
        """
        def extract_info(text):
            lines = text.split('\n')
            if len(lines) < 3:
                return {}

            # Linha 2: "2020/2021 • 82.817 KM • São Paulo Vila Carrão, SP"
            info_line = lines[1].strip()

            # Extrair ano
            ano_match = re.search(r'(\d{4})/(\d{4})', info_line)
            ano_fab = int(ano_match.group(1)) if ano_match else None
            ano_mod = int(ano_match.group(2)) if ano_match else None

            # Extrair KM
            km_match = re.search(r'(\d+(?:\.\d+)?)\s*KM', info_line)
            km = int(km_match.group(1).replace('.', '')) if km_match else None

            # Extrair localização
            loc_match = re.search(r'•\s*(.+)$', info_line)
            localizacao = loc_match.group(1).strip() if loc_match else ''

            # Parse localização: "Cidade Bairro, Estado"
            if ', ' in localizacao:
                loc_parts = localizacao.rsplit(', ', 1)
                endereco = loc_parts[0].strip()
                estado = loc_parts[1].strip()

                # Lista de cidades com múltiplas palavras
                multi_word_cities = ['Rio de Janeiro', 'São Paulo', 'São Bernardo do Campo', 'São José dos Campos']

                cidade = None
                bairro = None

                for city in multi_word_cities:
                    if endereco.startswith(city):
                        cidade = city
                        bairro = endereco[len(city):].strip()
                        break

                if not cidade:
                    # Assume primeira palavra é cidade, resto bairro
                    parts = endereco.split(' ', 1)
                    cidade = parts[0]
                    bairro = parts[1] if len(parts) > 1 else ''
            else:
                estado = ''
                cidade = localizacao
                bairro = ''

        # Recalcular colunas dependentes
        df = self._add_calculated_columns(df)

        return df


class DataValidator:
    """Classe para validação de dados brutos dos cards"""

    @staticmethod
    def validate_raw_cards(cards: List[List[str]]) -> Dict[str, Any]:
        """
        Valida dados brutos dos cards

        Args:
            cards: Lista de listas com dados dos cards

        Returns:
            Dicionário com resultado da validação
        """
        if not cards:
            return {'valid': False, 'pass_rate': 0.0, 'errors': ['Lista vazia']}

        total_cards = len(cards)
        valid_cards = 0
        errors = []

        for i, card in enumerate(cards):
            if not isinstance(card, list):
                errors.append(f"Card {i}: não é uma lista")
                continue

            # Verificar tamanho mínimo
            if len(card) < 3:
                errors.append(f"Card {i}: muito curto ({len(card)} elementos)")
                continue

            # Verificar se tem dados básicos
            if not card[0] or not card[2] or not card[3]:
                errors.append(f"Card {i}: dados básicos ausentes")
                continue

            # Verificar padrão de ano na terceira coluna
            info_line = card[2]
            if not re.search(r'\d{4}/\d{4}', info_line):
                errors.append(f"Card {i}: padrão de ano inválido em '{info_line}'")
                continue

            valid_cards += 1

        pass_rate = valid_cards / total_cards if total_cards > 0 else 0.0
        valid = pass_rate >= 0.8  # Pelo menos 80% dos cards válidos

        return {
            'valid': valid,
            'pass_rate': pass_rate,
            'total_cards': total_cards,
            'valid_cards': valid_cards,
            'errors': errors[:10]  # Limitar erros reportados
        }