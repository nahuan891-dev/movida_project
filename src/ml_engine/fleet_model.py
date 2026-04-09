import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, r2_score
import joblib
import os

class FleetHealthModel:
    """
    Modelo de IA (XGBoost) para análise de Saúde da Frota e Aging.
    """
    def __init__(self, model_path='data/models/fleet_xgboost.joblib'):
        self.model_path = model_path
        self.model = None
        self.label_encoders = {}
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)

    def prepare_data(self, df):
        """
        Engenharia de Atributos para o modelo de Aging.
        """
        # Cópia para não alterar o original
        data = df.copy()
        
        # Colunas relevantes
        features = ['marca', 'modelo', 'ano_fabricacao', 'km', 'preco']
        target = 'idade_veiculo' # Exemplo de target para Aging
        
        # Encoding de variáveis categóricas
        for col in ['marca', 'modelo']:
            le = LabelEncoder()
            data[col] = le.fit_transform(data[col].astype(str))
            self.label_encoders[col] = le
            
        return data[features], data[target]

    def train(self, df):
        """
        Treina o modelo XGBoost.
        """
        X, y = self.prepare_data(df)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        self.model = xgb.XGBRegressor(
            n_estimators=100,
            learning_rate=0.1,
            max_depth=5,
            objective='reg:squarederror'
        )
        
        print("Treinando modelo XGBoost para Saúde da Frota...")
        self.model.fit(X_train, y_train)
        
        # Avaliação
        preds = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, preds)
        r2 = r2_score(y_test, preds)
        
        print(f"Treinamento concluído. MAE: {mae:.2f}, R2: {r2:.2f}")
        
        # Salvar modelo
        joblib.dump({
            'model': self.model,
            'encoders': self.label_encoders,
            'features': X.columns.tolist()
        }, self.model_path)
        
        return mae, r2

    def predict(self, df):
        """
        Gera predições para novos dados.
        """
        if self.model is None:
            if os.path.exists(self.model_path):
                checkpoint = joblib.load(self.model_path)
                self.model = checkpoint['model']
                self.label_encoders = checkpoint['encoders']
            else:
                raise Exception("Modelo não treinado ou arquivo não encontrado.")
                
        # Preparação (usando encoders já treinados)
        data = df.copy()
        for col, le in self.label_encoders.items():
            # Tratar novos labels desconhecidos se necessário
            data[col] = data[col].map(lambda s: le.transform([s])[0] if s in le.classes_ else -1)
            
        features = ['marca', 'modelo', 'ano_fabricacao', 'km', 'preco']
        return self.model.predict(data[features])
