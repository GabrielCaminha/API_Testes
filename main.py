from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, JSONResponse
import os
import fitz
import tempfile
import logging
import uuid
from associador import Associador
import associadorPorTexto
import leitorNota
import interpretarExtrato

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

DOCUMENTOS_DIR = "documentos"
ASSOCIACOES_DIR = "associacoes"
os.makedirs(DOCUMENTOS_DIR, exist_ok=True)
os.makedirs(ASSOCIACOES_DIR, exist_ok=True)

BASE_URL = "https://api-testes.onrender.com"

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
            texto_normalizado = texto_extraido.lower()

            palavras_chave_nota_fiscal = ["nota fiscal", "nfe", "nf-e"]
            palavras_chave_extrato = ["extrato", "saldo", "agência", "conta corrente", "data da operação"]

            if any(palavra in texto_normalizado for palavra in palavras_chave_nota_fiscal):
                return "nota fiscal", texto_extraido
            elif any(palavra in texto_normalizado for palavra in palavras_chave_extrato):
                return "extrato_pdf", texto_extraido
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
    arquivo_plano: Optional[UploadFile] = File(None),
    usuario_id: str = Form("default"),
    associacoes_json: Optional[str] = Form(None)  # ✅ Novo parâmetro opcional
):
    resultados = []
    caminho_temp_plano = None

    try:
        if not arquivos:
            raise HTTPException(status_code=400, detail="Nenhum arquivo foi enviado")

        if arquivo_plano:
            caminho_temp_plano = salvar_arquivo_temporario(arquivo_plano, '.txt')

        associador = Associador(caminho_temp_plano, usuario_id) if caminho_temp_plano else Associador(None, usuario_id)

        for arquivo in arquivos:
            caminho_temp = None
            try:
                caminho_temp = salvar_arquivo_temporario(arquivo)
                categoria, conteudo = categorizar_arquivo(caminho_temp)

                if categoria == "extrato":
                    logger.info(f"Processando extrato: {arquivo.filename}")
                    documento_id = f"extrato_{uuid.uuid4().hex[:8]}"
                    nome_xlsx = f"{documento_id}.xlsx"
                    nome_txt = f"{documento_id}.txt"

                    caminho_saida_xlsx = os.path.join(DOCUMENTOS_DIR, nome_xlsx)
                    caminho_saida_txt = os.path.join(DOCUMENTOS_DIR, nome_txt)

                    associador.processar_extrato(caminho_temp, caminho_saida_xlsx, caminho_saida_txt)

                    download_url_xlsx = f"{BASE_URL}/documentos/{nome_xlsx}"
                    download_url_txt = f"{BASE_URL}/documentos/{nome_txt}"

                    caminho_saida_chatgpt = caminho_saida_xlsx.replace(".xlsx", "_chatgpt.xlsx")
                    download_url_chatgpt = None

                    if os.path.exists(caminho_saida_chatgpt):
                        nome_chatgpt = nome_xlsx.replace(".xlsx", "_chatgpt.xlsx")
                        novo_caminho_chatgpt = os.path.join(DOCUMENTOS_DIR, nome_chatgpt)
                        os.rename(caminho_saida_chatgpt, novo_caminho_chatgpt)
                        download_url_chatgpt = f"{BASE_URL}/documentos/{nome_chatgpt}"

                    resultado = {
                        "arquivo": arquivo.filename,
                        "status": "processado",
                        "tipo": "extrato",
                        "download_url_excel": download_url_xlsx,
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
                    nome_txt = f"{documento_id}.txt"
                    caminho_saida = os.path.join(DOCUMENTOS_DIR, nome_txt)

                    resultado_nota = leitorNota.processar_nota_fiscal_com_plano(conteudo, caminho_temp_plano, caminho_saida)

                    download_url = f"{BASE_URL}/documentos/{nome_txt}"
                    resultados.append({
                        "arquivo": arquivo.filename,
                        "status": "processado",
                        "tipo": "nota fiscal",
                        "download_url": download_url,
                        "documento_id": documento_id,
                        "empresa": resultado_nota["empresa"]
                    })

                elif categoria == "extrato_pdf":
                    try:
                        logger.info(f"Encaminhando PDF de extrato para o interpretarExtrato: {arquivo.filename}")
                        resultado_extrato = interpretarExtrato.processar_texto_extrato(texto_extraido=conteudo)

                        if caminho_temp_plano:
                            logger.info(f"Enviando texto extraído para associadorPorTexto.associar com plano de contas")

                            documento_id = f"extrato_pdf_{uuid.uuid4().hex[:8]}"
                            nome_xlsx = f"{documento_id}.xlsx"
                            nome_txt = f"{documento_id}.txt"

                            caminho_saida_xlsx = os.path.join(DOCUMENTOS_DIR, usuario_id, nome_xlsx)
                            caminho_saida_txt = os.path.join(DOCUMENTOS_DIR, usuario_id, nome_txt)

                            associadorPorTexto.associar(
                                texto=resultado_extrato,
                                caminho_saida_xlsx=caminho_saida_xlsx,
                                caminho_saida_txt=caminho_saida_txt,
                                caminho_plano=caminho_temp_plano,
                                usuario_id=usuario_id,
                                associacoes_json=associacoes_json  # ✅ Novo argumento passado
                            )

                            download_url_xlsx = f"{BASE_URL}/documentos/{usuario_id}/{nome_xlsx}"
                            download_url_txt = f"{BASE_URL}/documentos/{usuario_id}/{nome_txt}"

                            nome_plano_atualizado = f"novo_plano_completo_{usuario_id}.txt"
                            download_url_plano_atualizado = f"{BASE_URL}/documentos/{usuario_id}/{nome_plano_atualizado}"

                            nome_associacoes_json = "associacoes.json"
                            download_url_associacoes_json = f"{BASE_URL}/associacoes/{usuario_id}/{nome_associacoes_json}"

                            caminho_saida_chatgpt = caminho_saida_xlsx.replace(".xlsx", "_chatgpt.xlsx")
                            download_url_chatgpt = None

                            if os.path.exists(caminho_saida_chatgpt):
                                nome_chatgpt = nome_xlsx.replace(".xlsx", "_chatgpt.xlsx")
                                novo_caminho_chatgpt = os.path.join(DOCUMENTOS_DIR, usuario_id, nome_chatgpt)
                                os.rename(caminho_saida_chatgpt, novo_caminho_chatgpt)
                                download_url_chatgpt = f"{BASE_URL}/documentos/{usuario_id}/{nome_chatgpt}"

                            resultado = {
                                "arquivo": arquivo.filename,
                                "status": "processado",
                                "tipo": "extrato_pdf",
                                "download_url_excel": download_url_xlsx,
                                "download_url_txt": download_url_txt,
                                "download_url_plano_atualizado": download_url_plano_atualizado,
                                "download_url_associacoes_json": download_url_associacoes_json,
                                "documento_id": documento_id
                            }

                            if download_url_chatgpt:
                                resultado["download_url_chatgpt"] = download_url_chatgpt

                            resultados.append(resultado)

                        else:
                            resultados.append({
                                "arquivo": arquivo.filename,
                                "status": "erro",
                                "mensagem": "Arquivo de plano de contas não fornecido para o PDF de extrato"
                            })

                    except Exception as e:
                        logger.error(f"Erro ao processar extrato PDF via interpretarExtrato ou associadorPorTexto: {str(e)}")
                        resultados.append({
                            "arquivo": arquivo.filename,
                            "status": "erro",
                            "mensagem": f"Erro ao processar extrato PDF: {str(e)}"
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

@app.get("/documentos/{usuario_id}/{filename}")
async def obter_documento_usuario(usuario_id: str, filename: str):
    caminho_arquivo = os.path.join(DOCUMENTOS_DIR, usuario_id, filename)
    if not os.path.exists(caminho_arquivo):
        raise HTTPException(status_code=404, detail="Documento não encontrado")
    if caminho_arquivo.endswith('.xlsx'):
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif caminho_arquivo.endswith('.txt'):
        media_type = "text/plain"
    else:
        media_type = "application/octet-stream"

    return FileResponse(
        caminho_arquivo,
        media_type=media_type,
        filename=filename
    )

@app.get("/documentos/{filename}")
async def obter_documento(filename: str):
    caminho_arquivo = os.path.join(DOCUMENTOS_DIR, filename)
    if not os.path.exists(caminho_arquivo):
        raise HTTPException(status_code=404, detail="Documento não encontrado")
    if caminho_arquivo.endswith('.xlsx'):
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    elif caminho_arquivo.endswith('.txt'):
        media_type = "text/plain"
    else:
        media_type = "application/octet-stream"

    return FileResponse(
        caminho_arquivo,
        media_type=media_type,
        filename=filename
    )

@app.get("/associacoes/{usuario_id}/{filename}")
async def obter_associacoes(usuario_id: str, filename: str):
    caminho_arquivo = os.path.join(ASSOCIACOES_DIR, usuario_id, filename)
    if not os.path.exists(caminho_arquivo):
        raise HTTPException(status_code=404, detail="Arquivo de associações não encontrado")
    return FileResponse(
        caminho_arquivo,
        media_type="application/json",
        filename=filename
    )

@app.get("/")
async def root_check():
    return {
        "status": "online",
        "message": "Envie uma requisição POST para /processar com arquivos (PDF/OFX) e opcionalmente um plano de contas",
        "endpoints": {
            "POST /processar": "Processa múltiplos arquivos",
            "GET /documentos/{filename}": "Recupera um documento processado (geral)",
            "GET /documentos/{usuario_id}/{filename}": "Recupera documentos do usuário",
            "GET /associacoes/{usuario_id}/{filename}": "Recupera JSON de associações",
            "GET /healthcheck": "Verifica status do serviço"
        },
        "api_url": BASE_URL
    }

@app.get("/healthcheck")
async def health_check():
    return {"status": "healthy", "app": "running", "api_url": BASE_URL}
