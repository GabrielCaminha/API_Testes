import os
import logging
from dotenv import load_dotenv
from openai import OpenAI
import pdfplumber
from tkinter import Tk, filedialog

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente
load_dotenv()
API_KEY = os.getenv("API_KEY2")

# Configurar cliente OpenAI
client = OpenAI(api_key=API_KEY)

def selecionar_pdf():
    try:
        root = Tk()
        root.withdraw()
        root.update()
        caminho_arquivo = filedialog.askopenfilename(
            title="Selecione o PDF do extrato bancário",
            filetypes=[("Arquivos PDF", "*.pdf")]
        )
        root.destroy()
        if not caminho_arquivo:
            logger.warning("Nenhum arquivo selecionado.")
        return caminho_arquivo
    except Exception as e:
        logger.error(f"Erro ao abrir o seletor de arquivos: {e}")
        return None

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

def obter_dados_estruturados(texto_pdf):
    prompt = f"""
O texto abaixo é um extrato bancário. Organize os dados no formato de tabela com as colunas: Data, Descrição e Valor. Retorne os dados de valor de forma formatada. Os outros dados da forma exata que estao, nao resuma as descricoes, isso é de extrema importancia, copie letra por letra e numero por numero. Verifique caso exista texto apos o valor e nao for a data, ele pode fazer parte da descricao anterior, caso sim adicione ele a descricao anterior. Ignore cabeçalhos como "Saldo Anterior", "Total de Entradas" ou "Total de Saídas". Se algum dado não tiver data, tente inferir. E nao adicione nenhum outro comentario adicional alem do formato abaixo.

Texto:
{texto_pdf}

Formato obrigatorio:
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
        logger.info("Resposta do ChatGPT recebida.")
        return conteudo
    except Exception as e:
        logger.error(f"Erro na API do ChatGPT: {e}")
        return None

# 🔑 Função pública que o main.py pode chamar
def processar_texto_extrato(texto_extraido):
    logger.info("Iniciando processamento de texto de extrato via ChatGPT.")
    resultado = obter_dados_estruturados(texto_extraido)
    return resultado

def main():
    caminho_pdf = selecionar_pdf()
    if not caminho_pdf:
        logger.error("Nenhum PDF selecionado. Encerrando.")
        return

    texto_pdf = extrair_texto_pdf(caminho_pdf)
    if not texto_pdf:
        logger.error("Falha na extração do texto. Encerrando.")
        return

    dados_formatados = obter_dados_estruturados(texto_pdf)
    if not dados_formatados:
        logger.error("Falha na obtenção dos dados formatados. Encerrando.")
        return
    print("\n=== Texto extraído do PDF ===\n")
    print(texto_pdf)
    print("\n=============================\n")
    print("\n=== Resposta do ChatGPT ===\n")
    print(dados_formatados)
    print("\n===========================\n")


if __name__ == "__main__":
    main()
