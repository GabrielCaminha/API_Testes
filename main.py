from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os
import fitz
import tempfile
import logging
import uuid
from associador import Associador
import leitorNota
import shutil

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

def salvar_arquivo_temporario(arquivo: UploadFile, extensao: str = None):
    try:
        file_ext = extensao if extensao else os.path.splitext(arquivo.filename)[1].lower()
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_ext) as temp_file:
            conteudo = arquivo.file.read()
            temp_file.write(conteudo)
            return temp_file.name
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo temporário: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao processar o arquivo: {str(e)}")

def extrair_texto_pdf(caminho_pdf: str):
    try:
        texto_total = ""
        with fitz.open(caminho_pdf) as pdf:
            for pagina in pdf:
                texto_total += pagina.get_text()
        return texto_total
    except Exception as e:
        logger.error(f"Erro ao extrair texto do PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao ler o arquivo PDF: {str(e)}")

def categorizar_arquivo(caminho_arquivo: str):
    try:
        _, extensao = os.path.splitext(caminho_arquivo)
        extensao = extensao.lower()

        if extensao == ".ofx":
            return "extrato", None
        elif extensao == ".pdf":
            texto_extraido = extrair_texto_pdf(caminho_arquivo)
            palavras_chave_nota_fiscal = ["nota fiscal", "nfe", "nf-e"]
            texto_normalizado = texto_extraido.lower()

            if any(palavra in texto_normalizado for palavra in palavras_chave_nota_fiscal):
                return "nota fiscal", texto_extraido
            else:
                return "pdf não identificado", None
        else:
            return "formato não suportado", None
    except Exception as e:
        logger.error(f"Erro ao categorizar arquivo: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao categorizar o arquivo: {str(e)}")

@app.post("/processar")
async def processar_documentos(
    arquivos: List[UploadFile] = File(...),
    arquivo_plano: Optional[UploadFile] = File(None)
):
    resultados = []
    caminho_temp_plano = None
    arquivos_processados = {
        "extratos": [],
        "notas_fiscais": []
    }

    try:
        # Verifica se há arquivos para processar
        if not arquivos:
            raise HTTPException(status_code=400, detail="Nenhum arquivo foi enviado")

        # Se houver arquivo de plano, salva temporariamente
        if arquivo_plano:
            caminho_temp_plano = salvar_arquivo_temporario(arquivo_plano, '.txt')

        # Cria instância do Associador com o plano de contas (se existir)
        associador = Associador(caminho_temp_plano) if caminho_temp_plano else Associador()

        # Processa cada arquivo
        for arquivo in arquivos:
            caminho_temp = None
            try:
                caminho_temp = salvar_arquivo_temporario(arquivo)
                categoria, conteudo = categorizar_arquivo(caminho_temp)
                
                if categoria == "extrato":
                    logger.info(f"Processando extrato: {arquivo.filename}")
                    # Gera um nome único para o arquivo de saída
                    nome_arquivo = f"resultado_{uuid.uuid4().hex[:8]}.xlsx"
                    caminho_saida_excel = os.path.join(tempfile.gettempdir(), nome_arquivo)
                    
                    # Processa o extrato
                    with open(caminho_temp, 'rb') as f:
                        associador.processar_extrato(f, caminho_saida_excel)
                    
                    arquivos_processados["extratos"].append(caminho_saida_excel)
                    resultados.append({
                        "arquivo": arquivo.filename,
                        "status": "processado",
                        "tipo": "extrato",
                        "resultado": nome_arquivo
                    })

                elif categoria == "nota fiscal":
                    if not caminho_temp_plano:
                        resultados.append({
                            "arquivo": arquivo.filename,
                            "status": "erro",
                            "mensagem": "Arquivo de plano de contas não fornecido"
                        })
                        continue

                    logger.info(f"Processando nota fiscal: {arquivo.filename}")
                    resultado = leitorNota.processar_nota_fiscal_com_plano(conteudo, caminho_temp_plano)
                    arquivos_processados["notas_fiscais"].append({
                        "arquivo": arquivo.filename,
                        "empresa": resultado["empresa"]
                    })
                    resultados.append({
                        "arquivo": arquivo.filename,
                        "status": "processado",
                        "tipo": "nota fiscal",
                        "empresa": resultado["empresa"]
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

        # Prepara a resposta
        if arquivos_processados["notas_fiscais"] and caminho_temp_plano:
            return FileResponse(
                caminho_temp_plano,
                media_type="text/plain",
                filename="plano_de_contas_atualizado.txt",
                headers={"X-Resultados": str(resultados)}
            )
        elif arquivos_processados["extratos"]:
            caminho_excel = arquivos_processados["extratos"][0]
            return FileResponse(
                caminho_excel,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=os.path.basename(caminho_excel),
                headers={"X-Resultados": str(resultados)}
            )
        else:
            return JSONResponse(
                content={"resultados": resultados},
                status_code=200,
                headers={"X-Resultados": str(resultados)}
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro inesperado: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro interno ao processar os documentos: {str(e)}")
    finally:
        # Limpeza de arquivos temporários
        if caminho_temp_plano and os.path.exists(caminho_temp_plano):
            os.remove(caminho_temp_plano)

@app.get("/")
async def root_check():
    return {
        "status": "online",
        "message": "Envie uma requisição POST para /processar com arquivos (PDF/OFX) e opcionalmente um plano de contas",
        "endpoints": {
            "POST /processar": "Processa múltiplos arquivos",
            "GET /healthcheck": "Verifica status do serviço"
        }
    }

@app.get("/healthcheck")
async def health_check():
    return {"status": "healthy", "app": "running"}