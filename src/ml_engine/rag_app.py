import streamlit as st
import os
import pandas as pd
from dotenv import load_dotenv
from langchain_community.document_loaders import CSVLoader
from langchain_community.vectorstores import FAISS

# Carregar variáveis do arquivo .env
load_dotenv()

# Configurações iniciais
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if GOOGLE_API_KEY:
    os.environ["GOOGLE_API_KEY"] = GOOGLE_API_KEY

st.set_page_config(page_title="Especialista Movida AI", layout="wide", page_icon="🚗")

# Sidebar para status e config
with st.sidebar:
    st.image("https://logodownload.org/wp-content/uploads/2018/10/movida-logo.png", width=150)
    st.title("Configurações")
    st.success("Motor de IA: Gemini-1.5-Flash")
    st.info("Status: Conectado à Base Gold Predictions")

st.title("🔒 Especialista Gold Prediction - Movida")
st.markdown("Este assistente responde exclusivamente com base nas **predições de IA (Aging e Demanda)** geradas pelo pipeline.")

# 1. Definição do Prompt Restritivo (Otimizado)
template = """Você é um analista de dados sênior da Movida. Use os dados abaixo para responder à pergunta. 

Regras Críticas:
1. Responda apenas com base no contexto do CSV fornecido.
2. Se não souber, diga: "Sinto muito, mas essa informação não consta na base gold_predictions.csv."
3. Seja técnico. Fale sobre 'Aging_Predicted', 'Fleet_Health_Score' e 'Demand_Next_Period' quando relevante.

Contexto:
{context}

Pergunta: {question}
Resposta útil:"""

QA_CHAIN_PROMPT = PromptTemplate(
    input_variables=["context", "question"],
    template=template,
)

@st.cache_resource
def setup_rag():
    # Ajuste do caminho para a estrutura do projeto
    file_path = "data/gold_predictions.csv"
    
    if os.path.exists(file_path):
        # Carregamento do CSV com encoding adequado para Windows/Excel
        loader = CSVLoader(file_path=file_path, encoding='utf-8-sig')
        documents = loader.load()
        
        # Embeddings do Google (Padrão 004 ou 001)
        embeddings = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
        
        # Criando o buscador vetorial FAISS
        vectorstore = FAISS.from_documents(documents, embeddings)
        return vectorstore
    return None

# Inicialização
if "GOOGLE_API_KEY" not in os.environ:
    st.warning("⚠️ Chave API do Google não configurada. Configure a variável GOOGLE_API_KEY no seu .env ou no terminal.")
    st.stop()

vectorstore = setup_rag()

if vectorstore:
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Exibir histórico
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Chat Input
    if prompt := st.chat_input("Ex: Qual o carro com maior risco de manutenção?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Configuração da Chain (Gemini Flash para velocidade e custo zero em tier gratuito)
        llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0)
        
        qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=vectorstore.as_retriever(search_kwargs={"k": 5}),
            chain_type_kwargs={"prompt": QA_CHAIN_PROMPT}
        )

        with st.chat_message("assistant"):
            with st.spinner("Analisando base preditiva..."):
                result = qa_chain.invoke({"query": prompt})
                response_text = result["result"]
                st.markdown(response_text)
                st.session_state.messages.append({"role": "assistant", "content": response_text})
else:
    st.error("❌ Erro: Arquivo 'data/gold_predictions.csv' não encontrado. Rode o scraper ou o pipeline de IA primeiro.")
