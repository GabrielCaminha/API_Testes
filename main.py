import os
import fitz
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
import associador
import leitorNota
import requests
from urllib.parse import urlparse
import tempfile

app = FastAPI()

def baixar_arquivo(url: str):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Verifica se é um PDF ou OFX pelo content-type ou extensão
        content_type = response.headers.get('content-type', '').lower()
        parsed_url = urlparse(url)
        file_ext = os.path.splitext(parsed_url.path)[1].lower()
        
        if 'pdf' not in content_type and 'ofx' not in content_type and file_ext not in ['.pdf', '.ofx']:
            raise HTTPException(status_code=400, detail="URL deve apontar para um arquivo PDF ou OFX")
        
        # Cria arquivo temporário com a extensão apropriada
        suffix = file_ext if file_ext in ['.pdf', '.ofx'] else '.pdf' if 'pdf' in content_type else '.ofx'
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            for chunk in response.iter_content(chunk_size=8192):
                temp_file.write(chunk)
            return temp_file.name
            
    except requests.RequestException as e:
        raise HTTPException(status_code=400, detail=f"Erro ao baixar o arquivo: {str(e)}")

def categorizar_arquivo(caminho_arquivo):
    _, extensao = os.path.splitext(caminho_arquivo)
    extensao = extensao.lower()

    if extensao == ".ofx":
        return "extrato", None

    elif extensao == ".pdf":
        texto_extraido = extrair_texto_pdf(caminho_arquivo)

        palavras_chave_nota_fiscal = ["nota fiscal", "nfe", "nf-e"]
        palavras_chave_boleto = ["boleto", "linha digitável", "cedente"]

        texto_normalizado = texto_extraido.lower()

        if any(palavra in texto_normalizado for palavra in palavras_chave_nota_fiscal):
            return "nota fiscal", texto_extraido
        elif any(palavra in texto_normalizado for palavra in palavras_chave_boleto):
            return "boleto", None
        else:
            return "não identificado", None
    else:
        return "formato não suportado", None

def extrair_texto_pdf(caminho_pdf):
    texto_total = ""
    with fitz.open(caminho_pdf) as pdf:
        for pagina in pdf:
            texto_total += pagina.get_text()
    return texto_total

@app.post("/processar-url")
async def processar_url(url: str):
    caminho_temp = None
    try:
        caminho_temp = baixar_arquivo(url)
        categoria, texto = categorizar_arquivo(caminho_temp)

        if categoria == "extrato":
            associador.processar_extrato(caminho_temp)
            os.remove(caminho_temp)

            caminho_saida_excel = "resultado.xlsx"
            if not os.path.exists(caminho_saida_excel):
                raise HTTPException(status_code=500, detail="Arquivo resultado.xlsx não encontrado.")

            return FileResponse(
                caminho_saida_excel,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename="resultado.xlsx"
            )

        elif categoria == "nota fiscal":
            leitorNota.processar_nota_fiscal(texto)
            os.remove(caminho_temp)

            caminho_saida_txt = leitorNota.PLANO_CONTAS_PATH
            if not os.path.exists(caminho_saida_txt):
                raise HTTPException(status_code=500, detail="Arquivo plano_de_contas.txt não encontrado.")

            return FileResponse(
                caminho_saida_txt,
                media_type="text/plain",
                filename="plano_de_contas.txt"
            )

        else:
            return {
                "url": url,
                "categoria": categoria,
                "mensagem": "Arquivo não será processado por nenhum módulo."
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno no servidor: {str(e)}")
    finally:
        if caminho_temp and os.path.exists(caminho_temp):
            os.remove(caminho_temp)