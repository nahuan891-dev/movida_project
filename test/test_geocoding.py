
import pandas as pd
from prepare_powerbi import adicionar_coordenadas
from pathlib import Path
import logging
import sys

# Configurar logging para ver o que acontece na geocodificação
logging.basicConfig(level=logging.INFO)

def test_gold_file_enrichment(file_path):
    path = Path(file_path)
    if not path.exists():
        print(f"❌ Arquivo não encontrado: {file_path}")
        return

    print(f"🔍 Carregando arquivo Gold: {file_path}")
    df = pd.read_csv(path)
    
    print(f"📊 Registros totais: {len(df)}")
    print(f"📍 Coordenadas nulas antes: {df['latitude'].isnull().sum()}")

    # Executar enriquecimento
    print("🌍 Iniciando geocodificação de teste...")
    try:
        # Usar apenas uma amostra se o arquivo for muito grande para não ser banido pelo Nominatim
        # Mas para o teste, vamos tentar o arquivo todo se for razoável (< 100 locs únicas)
        df_enriched = adicionar_coordenadas(df, 'localizacao', max_workers=2)
        
        print(f"✅ Geocodificação concluída!")
        print(f"📍 Coordenadas preenchidas: {df_enriched['latitude'].notnull().sum()}")
        
        # Salvar resultado do teste
        output_path = path.parent / f"test_enriched_{path.name}"
        df_enriched.to_csv(output_path, index=False)
        print(f"💾 Resultado salvo em: {output_path}")
        
        if df_enriched['latitude'].notnull().any():
            print("🚀 SUCESSO: Pelo menos algumas coordenadas foram recuperadas!")
        else:
            print("⚠️  AVISO: Nenhuma coordenada foi recuperada. Verifique a conexão ou rate limit.")
            
    except Exception as e:
        print(f"❌ Erro durante o teste: {e}")

if __name__ == "__main__":
    file_to_test = r"data\gold\gold_20260330_215715.csv"
    test_gold_file_enrichment(file_to_test)
