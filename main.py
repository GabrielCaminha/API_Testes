# ... (importações iguais)

from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
import os
import fitz
import tempfile
import logging
import uuid
from associador import Associador
import leitorNota

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

DOCUMENTOS_DIR = "documentos"
os.makedirs(DOCUMENTOS_DIR, exist_ok=True)

BASE_URL = "https://api-testes.onrender.com"

# Funções utilitárias (iguais)
# salvar_arquivo_temporario(), extrair_texto_pdf(), categorizar_arquivo()

@app.post("/processar")
async def processar_documentos(
    arquivos: List[UploadFile] = File(...),
    arquivo_plano: Optional[UploadFile] = File(None)
):
    resultados = []
    caminho_temp_plano = None

    try:
        if not arquivos:
            raise HTTPException(status_code=400, detail="Nenhum arquivo foi enviado")

        if arquivo_plano:
            caminho_temp_plano = salvar_arquivo_temporario(arquivo_plano, '.txt')

        associador = Associador(caminho_temp_plano) if caminho_temp_plano else Associador()

        for arquivo in arquivos:
            caminho_temp = None
            try:
                caminho_temp = salvar_arquivo_temporario(arquivo)
                categoria, conteudo = categorizar_arquivo(caminho_temp)
                
                if categoria == "extrato":
                    logger.info(f"Processando extrato: {arquivo.filename}")
                    documento_id = f"extrato_{uuid.uuid4().hex[:8]}"
                    caminho_saida = os.path.join(DOCUMENTOS_DIR, f"{documento_id}.xlsx")

                    # Processa o extrato e gera também o TXT
                    caminho_xlsx, caminho_txt = associador.processar_extrato(caminho_temp, caminho_saida)

                    download_url = f"{BASE_URL}/documentos/{documento_id}"
                    download_url_txt = f"{BASE_URL}/documentos/{documento_id}".replace(".xlsx", ".txt")

                    # Verifica arquivo _chatgpt.xlsx
                    caminho_saida_chatgpt = caminho_saida.replace(".xlsx", "_chatgpt.xlsx")
                    download_url_chatgpt = None
                    documento_id_chatgpt = None

                    if os.path.exists(caminho_saida_chatgpt):
                        documento_id_chatgpt = f"{documento_id}_chatgpt"
                        novo_caminho_chatgpt = os.path.join(DOCUMENTOS_DIR, f"{documento_id_chatgpt}.xlsx")
                        os.rename(caminho_saida_chatgpt, novo_caminho_chatgpt)
                        download_url_chatgpt = f"{BASE_URL}/documentos/{documento_id_chatgpt}"

                    resultado = {
                        "arquivo": arquivo.filename,
                        "status": "processado",
                        "tipo": "extrato",
                        "download_url_excel": download_url,
                        "download_url_txt": download_url_txt,
                        "documento_id": documento_id
                    }

                    if download_url_chatgpt:
                        resultado["download_url_chatgpt"] = download_url_chatgpt

                    resultados.append(resultado)

                elif categoria == "nota fiscal":
                    if not caminho_temp_plano:
                        resultados.append({
                            "arquivo": arquivo.filename,
                            "status": "erro",
                            "mensagem": "Arquivo de plano de contas não fornecido"
                        })
                        continue

                    logger.info(f"Processando nota fiscal: {arquivo.filename}")
                    documento_id = f"plano_{uuid.uuid4().hex[:8]}"
                    caminho_saida = os.path.join(DOCUMENTOS_DIR, f"{documento_id}.txt")
                    
                    resultado_nota = leitorNota.processar_nota_fiscal_com_plano(conteudo, caminho_temp_plano, caminho_saida)
                    
                    download_url = f"{BASE_URL}/documentos/{documento_id}"
                    resultados.append({
                        "arquivo": arquivo.filename,
                        "status": "processado",
                        "tipo": "nota fiscal",
                        "download_url": download_url,
                        "documento_id": documento_id,
                        "empresa": resultado_nota["empresa"]
                    })

                else:
                    resultados.append({
                        "arquivo": arquivo.filename,
                        "status": "ignorado",
                        "mensagem": f"Arquivo não processável ({categoria})"
                    })

            except Exception as e:
                logger.error(f"Erro ao processar arquivo {arquivo.filename}: {str(e)}")
                resultados.append({
                    "arquivo": arquivo.filename,
                    "status": "erro",
                    "mensagem": str(e)
                })
            finally:
                if caminho_temp and os.path.exists(caminho_temp):
                    os.remove(caminho_temp)

        return JSONResponse(content={"resultados": resultados}, status_code=200)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    finally:
        if caminho_temp_plano and os.path.exists(caminho_temp_plano):
            os.remove(caminho_temp_plano)


@app.get("/documentos/{documento_id}")
async def obter_documento(documento_id: str):
    try:
        arquivos = [f for f in os.listdir(DOCUMENTOS_DIR) if f.startswith(documento_id)]
        
        if not arquivos:
            raise HTTPException(status_code=404, detail="Documento não encontrado")
        
        caminho_arquivo = os.path.join(DOCUMENTOS_DIR, arquivos[0])

        if caminho_arquivo.endswith('.xlsx'):
            media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            filename = "resultado_financeiro.xlsx"
        elif caminho_arquivo.endswith('.txt'):
            media_type = "text/plain"
            filename = "conciliado.txt"
        else:
            media_type = "application/octet-stream"
            filename = arquivos[0]
        
        return FileResponse(
            caminho_arquivo,
            media_type=media_type,
            filename=filename
        )
    except Exception as e:
        logger.error(f"Erro ao recuperar documento: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao recuperar documento: {str(e)}")


@app.get("/")
async def root_check():
    return {
        "status": "online",
        "message": "Envie uma requisição POST para /processar com arquivos (PDF/OFX) e opcionalmente um plano de contas",
        "endpoints": {
            "POST /processar": "Processa múltiplos arquivos",
            "GET /documentos/{id}": "Recupera um documento processado",
            "GET /healthcheck": "Verifica status do serviço"
        },
        "api_url": BASE_URL
    }


@app.get("/healthcheck")
async def health_check():
     return {"status": "healthy", "app": "running", "api_url": BASE_URL}
