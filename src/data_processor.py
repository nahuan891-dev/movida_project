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
        Converte a coluna preco_bruto em um valor numérico inteiro
        removendo 'R$', pontos de milhar e tratando a vírgula decimal.
        """
        if 'preco_bruto' not in df.columns:
            return df

        def clean_price_to_numeric(price_val):
            if pd.isna(price_val) or price_val == '':
                return 0
            
            # Se já for número, apenas retorna como int
            if isinstance(price_val, (int, float)):
                return int(price_val)
                
            # Se for string, limpa
            price_str = str(price_val)
            # 1. Remover R$ e espaços
            price_str = re.sub(r'R\$\s*', '', price_str)
            # 2. Remover pontos de milhar
            price_str = price_str.replace('.', '')
            # 3. Trocar vírgula decimal por ponto (se houver)
            price_str = price_str.replace(',', '.')
            
            # 4. Extrair apenas a parte numérica
            match = re.search(r'(\d+\.?\d*)', price_str)
            if match:
                return int(float(match.group(1)))
            
            return 0

        df['preco_bruto'] = df['preco_bruto'].apply(clean_price_to_numeric).astype(int)
        logger.info("Coluna 'preco_bruto' convertida para inteiro")
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
    """Encapsula lógica de parsing de dados dos cards de forma robusta"""

    # Padrões regex para identificação de tipos de linha
    PATTERNS = {
        'info': re.compile(r'\d{4}/\d{4}.*KM', re.IGNORECASE),
        'price': re.compile(r'R\$\s*[\d\.,]+'),
        'tags': ['BLINDADO', 'BAIXO KM', 'IPVA PAGO', 'MIDIA', 'OFERTA', 'NOVIDADE', 'ÚNICO DONO']
    }

    @staticmethod
    def parse(card_data: List[str]) -> Optional[Dict[str, Any]]:
        """
        Parseia dados do card identificando campos por conteúdo, não por posição.
        """
        if not card_data or len(card_data) < 2:
            return None

        try:
            # Inicializar campos
            res = {
                'marca': '', 'modelo': '', 'versao': '',
                'ano_fabricacao': None, 'ano_modelo': None,
                'km': None, 'localizacao': '', 'preco_bruto': '',
                'condicoes': '', 'texto_completo': ' '.join(card_data),
                'blindado': 'Não', 'tags': []
            }

            # 1. Identificar linhas especiais e limpar "lixo"
            limpas = []
            for line in card_data:
                line_upper = line.upper().strip()
                
                # Checar se é flag de Compra Online (adicionada pelo scraper)
                if "COMPRA_ONLINE:" in line_upper:
                    res['compra_online'] = line.split(':')[-1].strip()
                    continue

                # Checar se é tag conhecida
                is_tag = False
                for tag in CardParser.PATTERNS['tags']:
                    if tag in line_upper:
                        res['tags'].append(tag)
                        if tag == 'BLINDADO': res['blindado'] = 'Sim'
                        is_tag = True
                        break
                if is_tag: continue

                # Checar se é linha de Preço
                if CardParser.PATTERNS['price'].search(line_upper):
                    if not res['preco_bruto']: # Pega o primeiro preço encontrado
                        res['preco_bruto'] = line.strip()
                    continue

                # Checar se é linha de Info (Ano/KM/Local)
                if CardParser.PATTERNS['info'].search(line_upper):
                    # Extrair Anos
                    ano_match = re.search(r'(\d{4})/(\d{4})', line)
                    if ano_match:
                        res['ano_fabricacao'] = int(ano_match.group(1))
                        res['ano_modelo'] = int(ano_match.group(2))
                    
                    # Extrair KM
                    km_match = re.search(r'([\d\.]+)\s*KM', line_upper)
                    if km_match:
                        res['km'] = int(km_match.group(1).replace('.', ''))
                    
                    # Extrair Localização (o que sobra após o último •)
                    if '•' in line:
                        res['localizacao'] = line.split('•')[-1].strip()
                    continue

                # Se não for nada disso, é parte do nome ou descrição
                if line.strip() and "DETALHES" not in line_upper:
                    limpas.append(line.strip())

            # 2. Atribuir Marca, Modelo e Versão do que restou
            if len(limpas) >= 1:
                res['modelo'] = limpas[0]
                # Tentar extrair marca do modelo
                for brand in ['VOLKSWAGEN', 'FIAT', 'RENAULT', 'HYUNDAI', 'TOYOTA', 'JEEP', 'CHEVROLET', 'AUDI', 'BMW', 'VOLVO']:
                    if brand in limpas[0].upper():
                        res['marca'] = brand.capitalize()
                        break
                if not res['marca'] and ' ' in limpas[0]:
                    res['marca'] = limpas[0].split()[0]

            if len(limpas) >= 2:
                res['versao'] = ' '.join(limpas[1:])

            # 3. Condições (geralmente linhas que sobraram com valores menores ou prazos)
            for line in card_data:
                if 'parcela' in line.lower() or 'entrada' in line.lower():
                    res['condicoes'] = line.strip()
                    break

            return res
        except Exception as e:
            logger.warning(f"Erro ao parsear card: {e}")
            return None

    @staticmethod
    def _validate_structure(card_data: List[str]) -> bool:
        """Mantido por compatibilidade, mas a lógica agora é mais flexível"""
        return len(card_data) >= 3

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
        Extrai informações estruturadas de um card usando o novo CardParser
        """
        return CardParser.parse(card_data)

    def process_car_data(self, dados_cards: List[List[str]]) -> pd.DataFrame:
        """
        Processa lista de dados dos cards e retorna DataFrame estruturado
        Utiliza multithreading para acelerar o processamento de grandes volumes.
        """
        from concurrent.futures import ThreadPoolExecutor
        
        logger.info(f"Processando {len(dados_cards)} cards em paralelo...")
        start_time = pd.Timestamp.now()

        def process_single_card(args):
            i, card = args
            try:
                registro = self._extract_car_info(card)
                if registro:
                    registro['id'] = i + 1
                    # data_coleta já vem do registro se quisermos manter a original do card
                    return registro
            except Exception as e:
                return None
            return None

        # Processar em paralelo
        with ThreadPoolExecutor() as executor:
            args_list = list(enumerate(dados_cards))
            resultados = list(executor.map(process_single_card, args_list))

        # Filtrar nulos e criar DataFrame
        registros = [r for r in resultados if r is not None]
        df = pd.DataFrame(registros)

        if df.empty:
            logger.warning("Nenhum dado válido foi processado")
            return df

        # Limpar e converter dados
        df = self._clean_data(df)

        # Adicionar colunas calculadas
        df = self._add_calculated_columns(df)

        duration = (pd.Timestamp.now() - start_time).total_seconds()
        logger.info(f"Processamento concluído em {duration:.2f}s. Total registros: {len(df)}")
        return df

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e converte tipos de dados de forma segura"""
        
        # Converter colunas para numérico se necessário
        for col in ['ano_fabricacao', 'ano_modelo', 'km']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        # Preço - usar a lógica robusta que já temos
        if 'preco_bruto' in df.columns:
            # Extrair valor numérico, converter para float e depois para int
            df['preco'] = (
                df['preco_bruto']
                .str.replace(r'R\$\s*', '', regex=True)
                .str.replace(r'\.', '', regex=True)
                .str.replace(',', '.', regex=False)
                .str.extract(r'(\d+\.?\d*)')[0]
                .astype(float)
                .fillna(0)
                .astype(int)
            )

        return df

    def _add_calculated_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Adiciona colunas calculadas para análise"""
        # Faixas de preço
        if 'preco' in df.columns:
            df['faixa_preco'] = pd.cut(
                df['preco'], bins=PRECO_BINS, labels=PRECO_LABELS, include_lowest=True
            )

        # Faixas de quilometragem
        if 'km' in df.columns:
            df['faixa_km'] = pd.cut(
                df['km'], bins=KM_BINS, labels=KM_LABELS, include_lowest=True
            )

        # Idade do veículo
        if 'ano_fabricacao' in df.columns:
            ano_atual = pd.Timestamp.now().year
            df['idade_veiculo'] = ano_atual - df['ano_fabricacao']

        # Eficiência preço/KM
        if 'preco' in df.columns and 'km' in df.columns:
            df['preco_por_km'] = df.apply(
                lambda row: row['preco'] / row['km'] if pd.notna(row['preco']) and pd.notna(row['km']) and row['km'] > 0 else None,
                axis=1
            )

        return df

    def _convert_raw_to_dataframe(self, raw_data: List[List[str]]) -> pd.DataFrame:
        """
        Converte dados brutos em DataFrame Bronze de forma alinhada e inteligente.
        Resolve o problema de deslocamento de colunas causado por tags extras.
        """
        if not raw_data:
            return pd.DataFrame()

        processados = []
        for i, card in enumerate(raw_data):
            # Usar o parse inteligente para alinhar os dados antes de salvar o Bronze
            info = self._extract_car_info(card)
            if info:
                # Criar dicionário com colunas fixas para o Bronze
                row = {
                    'marca_modelo': info['modelo'],
                    'versao': info['versao'],
                    'info_line': f"{info['ano_fabricacao']}/{info['ano_modelo']} • {info['km']} KM • {info['localizacao']}",
                    'preco_bruto': info['preco_bruto'],
                    'condicoes': info['condicoes'],
                    'blindado': info['blindado'],
                    'tags': '|'.join(info['tags']),
                    'compra_online': info.get('compra_online', 'Não'),
                    'id': i + 1,
                    'data_coleta': pd.Timestamp.now()
                }
                processados.append(row)

        return pd.DataFrame(processados)

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