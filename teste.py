import os
import logging
from dotenv import load_dotenv
from openai import OpenAI
import pdfplumber
import pandas as pd

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()
API_KEY = os.getenv("API_KEY")

# Configurar cliente OpenAI
client = OpenAI(api_key=API_KEY)

# Função para extrair texto do PDF
def extrair_texto_pdf(caminho_pdf):
    try:
        with pdfplumber.open(caminho_pdf) as pdf:
            texto = ""
            for pagina in pdf.pages:
                texto += pagina.extract_text() + "\n"
        logger.info("Texto extraído com sucesso do PDF.")
        return texto
    except Exception as e:
        logger.error(f"Erro ao extrair texto do PDF: {e}")
        return None

# Função para enviar o texto ao ChatGPT e obter dados formatados
def obter_dados_estruturados(texto_pdf):
    prompt = f"""
O texto abaixo é um extrato bancário. Organize os dados no formato de tabela com as colunas: Data, Descrição e Valor. Retorne os dados formatados, mas sem alterar nenhum dos nomes das descrições, para que possam ser lidos como um CSV ou uma tabela Excel. Ignore cabeçalhos como "Saldo Anterior", "Total de Entradas" ou "Total de Saídas". Se algum dado não tiver data, tente inferir do contexto.

Texto do extrato:
{texto_pdf}

Retorne no formato:
Data | Descrição | Valor
DD/MM/AAAA | ... | ...
    """
    try:
        resposta = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        conteudo = resposta.choices[0].message.content
        logger.info("Dados estruturados recebidos do ChatGPT.")
        return conteudo
    except Exception as e:
        logger.error(f"Erro na API do ChatGPT: {e}")
        return None

# Função para converter o texto em DataFrame
def converter_para_dataframe(conteudo):
    try:
        linhas = conteudo.strip().split("\n")
        dados = []
        for linha in linhas:
            partes = [p.strip() for p in linha.split("|")]
            if len(partes) == 3 and partes[0].lower() != "data":
                dados.append(partes)
        df = pd.DataFrame(dados, columns=["Data", "Descrição", "Valor"])
        logger.info("Dados convertidos em DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Erro ao converter para DataFrame: {e}")
        return None

# Função principal
def main():
    caminho_pdf = "PDF.pdf"

    texto_pdf = extrair_texto_pdf(caminho_pdf)
    if not texto_pdf:
        logger.error("Falha na extração do texto. Encerrando.")
        return

    dados_formatados = obter_dados_estruturados(texto_pdf)
    if not dados_formatados:
        logger.error("Falha na obtenção dos dados formatados. Encerrando.")
        return

    df = converter_para_dataframe(dados_formatados)
    if df is None or df.empty:
        logger.error("Nenhum dado válido encontrado para gerar o Excel.")
        return

    arquivo_saida = "saida.xlsx"
    df.to_excel(arquivo_saida, index=False)
    logger.info(f"Arquivo Excel gerado com sucesso: {arquivo_saida}")

if __name__ == "__main__":
    main()
