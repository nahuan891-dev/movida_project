#!/usr/bin/env python3
"""
RAG Sistema com Gemini API (Google AI)
Versão corrigida - Usando google-generativeai SDK com gemini-pro
"""

import csv
import os
from pathlib import Path
from typing import List, Dict, Tuple
import math
import sys

# Tentar importar o SDK do Google
try:
    import google.generativeai as genai
except ImportError:
    print("❌ Instale o SDK: pip install google-generativeai")
    sys.exit(1)

GEMINI_API_KEY = ''
CSV_FILE = r"data\gold_predictions.csv"
GEMINI_MODEL = "gemini-2.5-flash"  # ✅ Modelo que FUNCIONA

 
class BM25Retriever:
    """Busca usando BM25 com fallback para busca por coluna"""
    
    def __init__(self, documents: List[str], rows: List[Dict]):
        self.documents = documents
        self.rows = rows
        self.inverted_index = {}
        self.doc_lengths = [len(doc.split()) for doc in documents]
        self.avg_doc_length = sum(self.doc_lengths) / len(self.doc_lengths) if documents else 0
        self._build_index()
    
    def _build_index(self):
        """Constrói índice invertido"""
        for doc_idx, doc in enumerate(self.documents):
            tokens = doc.lower().split()
            for token in set(tokens):
                if token not in self.inverted_index:
                    self.inverted_index[token] = []
                self.inverted_index[token].append(doc_idx)
    
    def bm25_score(self, query: str, k1: float = 1.5, b: float = 0.75) -> List[Tuple[int, float]]:
        """Calcula BM25 score"""
        tokens = query.lower().split()
        scores = [0.0] * len(self.documents)
        
        for token in tokens:
            if token not in self.inverted_index:
                continue
            
            idf = math.log((len(self.documents) - len(self.inverted_index[token]) + 0.5) / 
                          (len(self.inverted_index[token]) + 0.5) + 1)
            
            for doc_idx in self.inverted_index[token]:
                doc = self.documents[doc_idx]
                freq = doc.lower().split().count(token)
                numerator = freq * (k1 + 1)
                denominator = freq + k1 * (1 - b + b * (self.doc_lengths[doc_idx] / self.avg_doc_length))
                scores[doc_idx] += idf * (numerator / denominator)
        
        return sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
    
    def retrieve(self, query: str, top_k: int = 10) -> List[Tuple[int, float]]:
        """Retorna top-k documentos com fallback"""
        scores = self.bm25_score(query)
        results = [(idx, score) for idx, score in scores[:top_k] if score > 0]
        
        # Se nenhum resultado, retornar os primeiros top_k
        if not results:
            results = [(idx, score) for idx, score in scores[:top_k]]
        
        return results
 
 
class RAGGeminiMelhorado:
    """RAG System melhorado com busca flexível"""
    
    def __init__(self, csv_path: str, model: str = GEMINI_MODEL):
        self.csv_path = csv_path
        self.model = model
        self.rows = []
        self.documents = []
        self.retriever = None
        self.api_key = GEMINI_API_KEY
        self.columns = []
        
    def load_csv(self):
        """Carrega CSV"""
        print(f"📂 Carregando {self.csv_path}...")
        
        if not Path(self.csv_path).exists():
            print(f"❌ Arquivo '{self.csv_path}' não encontrado!")
            print(f"   Pasta atual: {Path.cwd()}")
            return False
        
        with open(self.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            self.rows = list(reader)
        
        if not self.rows:
            print(f"❌ CSV vazio!")
            return False
        
        self.columns = list(self.rows[0].keys())
        print(f"✅ {len(self.rows)} linhas carregadas")
        print(f"   Colunas: {', '.join(self.columns)}\n")
        
        # Converter linhas em documentos
        self.documents = []
        for row in self.rows:
            doc = " ".join([f"{k}: {v}" for k, v in row.items() if v])
            self.documents.append(doc)
        
        # Inicializar retriever
        self.retriever = BM25Retriever(self.documents, self.rows)
        return True
    
    def get_csv_summary(self) -> str:
        """Gera um resumo automático do CSV"""
        summary = f"📊 RESUMO DO CSV:\n"
        summary += f"- Total de linhas: {len(self.rows)}\n"
        summary += f"- Colunas: {len(self.columns)}\n"
        summary += f"- Coluna principal: {self.columns[0]}\n\n"
        
        # Mostrar primeiras linhas
        summary += "Dados:\n"
        for i, row in enumerate(self.rows[:3]):
            summary += f"\nRegistro {i+1}:\n"
            for col in self.columns[:5]:  # Mostrar apenas 5 primeiras colunas
                summary += f"  {col}: {row.get(col, 'N/A')}\n"
        
        return summary
    
    def retrieve(self, query: str, top_k: int = 10) -> List[Dict]:
        """Busca documentos relevantes"""
        results = self.retriever.retrieve(query, top_k=top_k)
        
        retrieved_rows = []
        for doc_idx, score in results:
            if doc_idx < len(self.rows):
                retrieved_rows.append({
                    'row': self.rows[doc_idx],
                    'score': score,
                    'index': doc_idx
                })
        
        return retrieved_rows
    
    def format_context(self, retrieved: List[Dict], show_all: bool = False) -> str:
        """Formata contexto para o modelo"""
        context = "Dados do CSV relevantes para a pergunta:\n\n"
        
        if not retrieved:
            context = "Resumo dos dados do CSV:\n"
            context += f"Total de registros: {len(self.rows)}\n"
            context += f"Colunas: {', '.join(self.columns)}\n\n"
            context += "Primeiros registros:\n"
            for i, row in enumerate(self.rows[:5]):
                context += f"\n--- Registro {i+1} ---\n"
                for col in self.columns[:8]:  # Limitar colunas
                    context += f"{col}: {row.get(col, 'N/A')}\n"
        else:
            for i, item in enumerate(retrieved[:5], 1):  # Limitar a 5 resultados
                row = item['row']
                context += f"--- Dado {i} ---\n"
                for col in self.columns[:8]:  # Limitar a 8 colunas
                    context += f"{col}: {row.get(col, 'N/A')}\n"
                context += "\n"
        
        return context
    
    def answer(self, query: str, top_k: int = 10) -> str:
        """Gera resposta com Gemini"""
        
        # Detectar se é uma pergunta de resumo
        resumo_keywords = ['resuma', 'resume', 'resumo', 'o que há', 'quais', 'quantos', 'dados', 'informacao', 'informações']
        is_summary = any(keyword in query.lower() for keyword in resumo_keywords)
        
        # Buscar
        print("🔍 Buscando...", end=" ")
        retrieved = self.retrieve(query, top_k=top_k)
        print(f"({len(retrieved)} resultados)\n")
        
        context = self.format_context(retrieved, show_all=is_summary)
        
        # Preparar mensagem
        system_instruction = """Você é um assistente especializado em analisar dados de CSV.
 
REGRAS IMPORTANTES:
1. Responda APENAS com dados fornecidos no contexto
2. Seja claro e bem estruturado
3. Use formatação com bullet points quando apropriado
4. Cite dados específicos do CSV
5. Se a pergunta for sobre resumo, dê um bom resumo dos dados"""
        
        user_message = f"""{context}
 
Pergunta: {query}
 
Responda baseado nos dados do CSV:"""
        
        print("🤖 Gerando resposta com Gemini...", end=" ")
        
        try:
            # Configurar Gemini
            genai.configure(api_key=self.api_key)
            model = genai.GenerativeModel(self.model)
            
            # Gerar conteúdo
            response = model.generate_content(
                system_instruction + "\n\n" + user_message,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.7,
                    top_p=0.95,
                    top_k=40,
                    max_output_tokens=2048,
                )
            )
            
            answer_text = response.text
            print("✅\n")
            return answer_text
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌\n")
            return f"❌ Erro: {error_msg[:200]}"
    
    def interactive(self):
        """Chat interativo"""
        print("\n" + "="*70)
        print("💬 RAG CSV com Gemini - Chat Interativo (VERSÃO MELHORADA)")
        print(f"Modelo: {self.model}")
        print("="*70)
        print("Digite suas perguntas sobre o CSV (ou 'sair' para encerrar)\n")
        print("💡 Dicas:")
        print("  - 'resuma' → Resumo do CSV")
        print("  - 'quantos registros' → Contagem de dados")
        print("  - 'o que há' → Descrição do conteúdo")
        print("  - 'buscar [algo]' → Procura específica\n")
        
        while True:
            try:
                query = input("👤 Você: ").strip()
                
                if query.lower() in ['sair', 'quit', 'exit', 'q']:
                    print("\n👋 Até logo!")
                    break
                
                if not query:
                    continue
                
                answer = self.answer(query)
                print(f"\n🤖 Assistente:\n{answer}\n")
                print("-" * 70 + "\n")
                
            except KeyboardInterrupt:
                print("\n\n👋 Encerrado.")
                break
            except Exception as e:
                print(f"\n❌ Erro inesperado: {str(e)}\n")
 
 
def main():
    """Executa o RAG"""
    
    if not GEMINI_API_KEY:
        print("❌ GEMINI_API_KEY não configurada!")
        print("\nWindows PowerShell:")
        print("  $env:GEMINI_API_KEY='sua-chave-aqui'")
        return
    
    print("🚀 Iniciando RAG com Gemini (Versão Melhorada)...\n")
    
    try:
        rag = RAGGeminiMelhorado(CSV_FILE)
        if not rag.load_csv():
            print("Tente procurar manualmente pelo seu CSV:")
            print(f"  find . -name '*.csv'")
            return
        
        rag.interactive()
    except Exception as e:
        print(f"❌ Erro: {str(e)}")
 
 
if __name__ == "__main__":
    main()