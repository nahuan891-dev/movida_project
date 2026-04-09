import pandas as pd
import os
from src.ml_engine.fleet_model import FleetHealthModel

def run_ml_pipeline():
    """
    Pipeline principal para execução dos modelos de IA.
    """
    print("Iniciando Pipeline de Inteligência Artificial...")
    
    # 1. Carregar dados do Gold
    gold_path = 'data/gold_final.csv'
    if not os.path.exists(gold_path):
        print(f"Erro: Arquivo {gold_path} não encontrado.")
        return

    df = pd.read_csv(gold_path)
    
    # 2. Modelo de Saúde da Frota (Aging)
    model = FleetHealthModel()
    mae, r2 = model.train(df)
    
    # 3. Gerar Predições
    print("Gerando predições para exportação ao Power BI...")
    predictions = model.predict(df)
    
    # 4. Preparar Output para Power BI (gold_predictions.csv)
    df_output = df.copy()
    df_output['Aging_Predicted'] = predictions
    df_output['Prediction_Date'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
    
    # Cálculo de "Necessidade de Renovação" baseado em desvios do modelo
    # Se o Aging real for muito maior que o previsto, o carro pode estar subutilizado ou precisar de manutenção
    df_output['Fleet_Health_Score'] = (df_output['idade_veiculo'] - df_output['Aging_Predicted']).abs()
    
    output_path = 'data/gold_predictions.csv'
    df_output.to_csv(output_path, index=False)
    
    print(f"Pipeline finalizado com sucesso! Arquivo salvo em: {output_path}")

if __name__ == "__main__":
    run_ml_pipeline()
