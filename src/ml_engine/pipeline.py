import pandas as pd
import os
import torch
from src.ml_engine.fleet_model import FleetHealthModel
from src.ml_engine.demand_model import DemandPredictor

def run_ml_pipeline():
    """
    Pipeline principal para execução dos modelos de IA e Deep Learning.
    """
    print("="*50)
    print(" INICIANDO PIPELINE DE INTELIGÊNCIA ARTIFICIAL ")
    print("="*50)
    
    # 1. Carregar dados do Gold
    gold_path = 'data/gold_final.csv'
    if not os.path.exists(gold_path):
        print(f"Erro: Arquivo {gold_path} não encontrado.")
        return

    df = pd.read_csv(gold_path)
    print(f"Total de registros carregados: {len(df)}")
    
    # 2. IMPLEMENTAÇÃO II: Machine Learning (Saúde da Frota - XGBoost)
    print("\n[FRENTE 1] Iniciando Machine Learning (XGBoost)...")
    fleet_model = FleetHealthModel()
    mae_fleet, r2_fleet = fleet_model.train(df)
    fleet_preds = fleet_model.predict(df)
    
    # 3. IMPLEMENTAÇÃO I: Deep Learning (Previsão de Demanda - LSTM)
    print("\n[FRENTE 2] Iniciando Deep Learning (LSTM)...")
    demand_model = DemandPredictor()
    loss_demand = demand_model.train(df, epochs=20)
    future_demand = demand_model.predict_next(df)
    print(f"Previsão de Demanda para o Próximo Período: {future_demand:.2f} anúncios")
    
    # 4. Preparar Output para Power BI (gold_predictions.csv)
    print("\nPreparando exportação para Power BI...")
    df_output = df.copy()
    
    # Colunas de Predição da Frota
    df_output['Aging_Predicted'] = fleet_preds
    df_output['Fleet_Health_Score'] = (df_output['idade_veiculo'] - df_output['Aging_Predicted']).abs()
    
    # Coluna de Predição de Demanda (Valor Global repetido ou agregável)
    df_output['Predicted_Demand_Next_Period'] = future_demand
    
    # Carimbos de auditoria (Diferencial para Defesa do Projeto)
    df_output['Prediction_Date'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')
    df_output['Model_Metrics'] = f"Fleet R2: {r2_fleet:.2f} | Demand Loss: {loss_demand:.4f}"
    
    output_path = 'data/gold_predictions.csv'
    df_output.to_csv(output_path, index=False)
    
    print("="*50)
    print(f"PIPELINE FINALIZADO COM SUCESSO!")
    print(f"Arquivo gerado: {output_path}")
    print("="*50)

if __name__ == "__main__":
    run_ml_pipeline()
