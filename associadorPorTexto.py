import os
import json
import logging
import pandas as pd
import re
from openai import OpenAI
from dotenv import load_dotenv
from typing import Optional

# Configura logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carrega a API key do ambiente
load_dotenv()
API_KEY = os.getenv("API_KEY2")
client = OpenAI(api_key=API_KEY)


class AssociadorPorTexto:
    def __init__(self, caminho_plano: Optional[str] = None, usuario_id: Optional[str] = "default"):
        self.caminho_plano = caminho_plano
        self.usuario_id = usuario_id

        self.pasta_associacoes = os.path.join("associacoes", self.usuario_id)
        os.makedirs(self.pasta_associacoes, exist_ok=True)
        self.associacoes_path = os.path.join(self.pasta_associacoes, "associacoes.json")

        self.pasta_documentos = os.path.join("documentos", self.usuario_id)
        os.makedirs(self.pasta_documentos, exist_ok=True)

    def ler_plano_de_contas(self):
        codigos, ids_estendidos, nomes = [], [], []
        try:
            with open(self.caminho_plano, 'r', encoding='utf-8') as f:
                linhas = f.readlines()
        except UnicodeDecodeError:
            with open(self.caminho_plano, 'r', encoding='latin-1') as f:
                linhas = f.readlines()

        for linha in linhas:
            partes = linha.strip().split('|')
            if len(partes) >= 3:
                codigos.append(partes[0].strip())
                ids_estendidos.append(partes[1].strip())
                nomes.append(partes[2].strip())

        return pd.DataFrame({
            'Conta Código': codigos,
            'ID Estendido': ids_estendidos,
            'Nome da Conta': nomes
        })

    def carregar_associacoes_json(self):
        if not os.path.exists(self.associacoes_path):
            self.salvar_associacoes_json({})
        try:
            with open(self.associacoes_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.decoder.JSONDecodeError:
            self.salvar_associacoes_json({})
            return {}

    def salvar_associacoes_json(self, associacoes):
        with open(self.associacoes_path, 'w', encoding='utf-8') as f:
            json.dump(associacoes, f, indent=2, ensure_ascii=False)

    def extrair_json(self, texto: str):
        """
        Tenta extrair o primeiro objeto JSON válido contido em 'texto',
        removendo qualquer texto antes ou depois das chaves { ... }.
        """
        texto = texto.strip()
        match = re.search(r'\{.*\}', texto, re.DOTALL)
        if not match:
            raise ValueError("Não foi possível extrair JSON válido da resposta")
        return match.group(0)

    def consultar_chatgpt_para_associacao(self, descricoes_sem_associacao, plano_df, associacoes_dict):
        descricoes_sem_associacao = [d.strip() for d in descricoes_sem_associacao if d.strip()]

        nomes_validos = sorted(set(plano_df['Nome da Conta'].tolist()))
        nomes_validos.append("A IDENTIFICAR")
        nomes_validos_lower = [n.lower() for n in nomes_validos]

        payload = {
            "descricoes": descricoes_sem_associacao,
            "opcoes": nomes_validos
        }

        logger.info(f"📤 Payload JSON: {json.dumps(payload, ensure_ascii=False)}")

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Você é um assistente contábil.\n"
                        "Associe cada descrição da lista `descricoes` a UM item de `opcoes`.\n"
                        "⚠️ Se não houver correspondência lógica, retorne 'A IDENTIFICAR'.\n"
                        "✅ Retorne EXATAMENTE neste formato JSON: { \"desc1\": \"ContaX\", \"desc2\": \"ContaY\" }."
                    )
                },
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False)
                }
            ],
            temperature=0.0
        )

        resposta_raw = response.choices[0].message.content
        logger.info(f"📥 Resposta bruta: {resposta_raw}")

        try:
            json_limpo = self.extrair_json(resposta_raw)
            associacoes_resposta = json.loads(json_limpo)
        except Exception as e:
            logger.warning(f"⚠️ Resposta não é JSON válido ({e}). Marcando tudo como 'A IDENTIFICAR'.")
            associacoes_resposta = {}

        novas_associacoes = False
        for idx, desc in enumerate(descricoes_sem_associacao, start=1):
            chave = f"desc{idx}"
            conta = associacoes_resposta.get(chave, "A IDENTIFICAR").strip()
            if conta.lower() not in nomes_validos_lower:
                conta = "A IDENTIFICAR"
            associacoes_dict[desc] = conta
            novas_associacoes = True

        if novas_associacoes:
            self.salvar_associacoes_json(associacoes_dict)

        return associacoes_dict

    def processar_texto(self, texto: str, caminho_saida_xlsx: str, caminho_saida_txt: Optional[str] = None):
        def limpar_valor(valor_str):
            if not isinstance(valor_str, str):
                return None
            valor_str = valor_str.strip()
            negativo = valor_str.startswith('-')
            valor_limpo = re.sub(r'[^\d,.-]', '', valor_str).replace('.', '').replace(',', '.')
            try:
                valor_num = float(valor_limpo)
                if negativo and valor_num > 0:
                    valor_num = -valor_num
                return valor_num
            except:
                return None

        from io import StringIO
        df = pd.read_csv(StringIO(texto), sep='|', skipinitialspace=True,
                         names=['Data', 'Descrição', 'Valor'], skiprows=1,
                         dayfirst=True, engine='python')

        df['Descrição'] = df['Descrição'].astype(str).str.strip()
        df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
        df['Valor'] = df['Valor'].apply(limpar_valor)

        plano_df = self.ler_plano_de_contas()
        plano_df['Nome da Conta'] = plano_df['Nome da Conta'].str.strip()

        associacoes = self.carregar_associacoes_json()
        df['Conta Associada'] = df['Descrição'].map(lambda desc: associacoes.get(desc.strip()))

        nao_associados = df[df['Conta Associada'].isna()]['Descrição'].unique().tolist()
        logger.info(f"📌 Não associados: {nao_associados}")

        if nao_associados:
            associacoes = self.consultar_chatgpt_para_associacao(nao_associados, plano_df, associacoes)
            for desc in nao_associados:
                conta = associacoes.get(desc)
                if conta:
                    df.loc[df['Descrição'] == desc, 'Conta Associada'] = conta

        self.salvar_associacoes_json(associacoes)

        # 🗂️ Gera novo plano de contas antes de Excel/TXT
        pendencias = [desc for desc in associacoes if associacoes[desc] == "A IDENTIFICAR"]
        logger.info(f"📌 Pendências: {pendencias}")

        try:
            plano_df['Conta Código Int'] = plano_df['Conta Código'].astype(str).str.extract(r'(\d+)').astype(int)
            ultimo_codigo = int(plano_df['Conta Código Int'].max())
        except:
            ultimo_codigo = 0

        identificador_base = ultimo_codigo + 1
        id_estendido_padrao = "00000000"

        linhas_finais = [
            f"{row['Conta Código']}|{row['ID Estendido']}|{row['Nome da Conta']}"
            for _, row in plano_df.iterrows()
        ]

        if pendencias:
            linhas_finais.append(f"{identificador_base}|{id_estendido_padrao}|CONTAS A IDENTIFICAR")
            for idx, desc in enumerate(pendencias, start=1):
                novo_codigo = f"{identificador_base}-{idx}"
                linhas_finais.append(f"{novo_codigo}|{id_estendido_padrao}|{desc}")

        # Salva novo plano antes do TXT
        if linhas_finais:
            novo_plano_path = os.path.join(self.pasta_documentos, f"novo_plano_completo_{self.usuario_id}.txt")
            with open(novo_plano_path, 'w', encoding='utf-8') as f:
                for linha in linhas_finais:
                    f.write(linha + '\n')
            logger.info(f"✅ Novo plano de contas COMPLETO salvo em: {novo_plano_path}")

        # 🗝️ Atribui códigos corretos usando novo plano
        df['Conta Código'] = None
        df['ID Estendido'] = None

        for idx, row in df.iterrows():
            conta = row['Conta Associada']
            if conta and conta != "A IDENTIFICAR":
                match = plano_df[plano_df['Nome da Conta'] == conta]
                if len(match) == 1:
                    df.at[idx, 'Conta Código'] = match.iloc[0]['Conta Código']
                    df.at[idx, 'ID Estendido'] = match.iloc[0]['ID Estendido']
            elif conta == "A IDENTIFICAR":
                desc = row['Descrição'].strip()
                if desc in pendencias:
                    pos = pendencias.index(desc) + 1
                    df.at[idx, 'Conta Código'] = f"{identificador_base}-{pos}"
                    df.at[idx, 'ID Estendido'] = id_estendido_padrao

        # Salva Excel
        resultado = df[['Conta Código', 'Conta Associada', 'ID Estendido', 'Descrição', 'Valor', 'Data']]
        nome_excel = os.path.basename(caminho_saida_xlsx)
        caminho_excel_final = os.path.join(self.pasta_documentos, nome_excel)
        resultado.to_excel(caminho_excel_final, index=False)
        logger.info(f"✅ Excel salvo em: {caminho_excel_final}")

        # Gera TXT depois
        if caminho_saida_txt:
            nome_txt = os.path.basename(caminho_saida_txt)
            caminho_txt_final = os.path.join(self.pasta_documentos, nome_txt)
            with open(caminho_txt_final, 'w', encoding='utf-8') as f:
                f.write("|0000|32662718000130|\n")
                for _, row in resultado.iterrows():
                    f.write("|6000|X||||\n")
                    data = row['Data'] or ""
                    conta_codigo = row['Conta Código'] or ""
                    campo5 = row['ID Estendido'] or ""
                    valor = f"{row['Valor']:.2f}".replace('.', ',') if pd.notnull(row['Valor']) else "0,00"
                    descricao = row['Descrição'] or ""
                    linha_6100 = f"|6100|{data}|{conta_codigo}|{campo5}|{valor}||{descricao}||||\n"
                    f.write(linha_6100)
            logger.info(f"✅ TXT salvo em: {caminho_txt_final}")

        return resultado.to_dict(orient='records')


def associar(texto: str, caminho_saida_xlsx: str = "saida_associada.xlsx",
             caminho_saida_txt: Optional[str] = None,
             caminho_plano: Optional[str] = None,
             usuario_id: Optional[str] = "default"):
    associador = AssociadorPorTexto(caminho_plano=caminho_plano, usuario_id=usuario_id)
    return associador.processar_texto(texto, caminho_saida_xlsx, caminho_saida_txt)
