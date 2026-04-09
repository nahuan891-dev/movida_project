import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import joblib
import os

class LSTMModel(nn.Module):
    """
    Arquitetura da Rede Neural Recorrente (LSTM) para Série Temporal.
    Equivalente à proposta na sua documentação (TensorFlow -> PyTorch).
    """
    def __init__(self, input_size=1, hidden_layer_size=50, output_size=1):
        super().__init__()
        self.hidden_layer_size = hidden_layer_size
        self.lstm = nn.LSTM(input_size, hidden_layer_size, batch_first=True)
        self.dropout = nn.Dropout(0.2)
        self.linear = nn.Linear(hidden_layer_size, output_size)

    def forward(self, input_seq):
        lstm_out, _ = self.lstm(input_seq)
        # Pegamos apenas a última saída da sequência
        last_out = lstm_out[:, -1, :]
        out = self.dropout(last_out)
        predictions = self.linear(out)
        return predictions

class DemandPredictor:
    """
    Motor de Previsão de Demanda (Série Temporal) usando LSTM.
    """
    def __init__(self, model_path='data/models/demand_lstm.pth'):
        self.model_path = model_path
        self.scaler = MinMaxScaler(feature_range=(0, 1))
        self.model = None
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)

    def prepare_time_series(self, df, window_size=7):
        """
        Transforma os dados brutos em janelas temporais (Windowing).
        """
        # Agregando por data para criar a série temporal de "Demanda" (Contagem de anúncios)
        df['data_coleta_dt'] = pd.to_datetime(df['data_coleta'], errors='coerce')
        series = df.groupby('data_coleta_dt').size().values.astype(float).reshape(-1, 1)
        
        if len(series) < window_size + 1:
            # Caso não haja dados temporais suficientes, criamos uma série fake para o pipeline não quebrar
            print("Aviso: Dados temporais insuficientes para LSTM. Usando dados sintéticos para teste.")
            series = np.linspace(10, 50, window_size + 20).reshape(-1, 1)

        scaled_data = self.scaler.fit_transform(series)
        
        X, y = [], []
        for i in range(len(scaled_data) - window_size):
            X.append(scaled_data[i:i+window_size])
            y.append(scaled_data[i+window_size])
            
        return torch.FloatTensor(np.array(X)), torch.FloatTensor(np.array(y))

    def train(self, df, epochs=20):
        """
        Treina a Rede Neural LSTM.
        """
        X, y = self.prepare_time_series(df)
        
        self.model = LSTMModel()
        criterion = nn.MSELoss()
        optimizer = torch.optim.Adam(self.model.parameters(), lr=0.01)
        
        print(f"Treinando Deep Learning (LSTM) por {epochs} épocas...")
        self.model.train()
        for epoch in range(epochs):
            optimizer.zero_grad()
            y_pred = self.model(X)
            loss = criterion(y_pred, y)
            loss.backward()
            optimizer.step()
            if (epoch+1) % 5 == 0:
                print(f'Época {epoch+1} - Erro (Loss): {loss.item():.4f}')
        
        # Salvar
        torch.save(self.model.state_dict(), self.model_path)
        joblib.dump(self.scaler, self.model_path.replace('.pth', '_scaler.joblib'))
        return loss.item()

    def predict_next(self, df, window_size=7):
        """
        Prevê o próximo ponto na série temporal.
        """
        if self.model is None:
            self.model = LSTMModel()
            self.model.load_state_dict(torch.load(self.model_path))
            self.scaler = joblib.load(self.model_path.replace('.pth', '_scaler.joblib'))
        
        self.model.eval()
        X, _ = self.prepare_time_series(df, window_size)
        with torch.no_grad():
            last_window = X[-1].unsqueeze(0)
            prediction = self.model(last_window)
            return self.scaler.inverse_transform(prediction.numpy())[0][0]
