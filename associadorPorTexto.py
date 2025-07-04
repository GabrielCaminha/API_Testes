import pandas as pd
import json
import os
import logging
from openai import OpenAI
from difflib import get_close_matches
from dotenv import load_dotenv
from typing import Optional
import re

# === Setup Logging ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Load API Key ===
load_dotenv()
API_KEY = os.getenv("API_KEY2")
client = OpenAI(api_key=API_KEY)

# === Plano de Contas ===

def carregar_plano_de_contas_lista(caminho_arquivo):
    if not os.path.exists(caminho_arquivo):
        raise ValueError("Arquivo de plano de contas não encontrado")
    try:
        with open(caminho_arquivo, "r", encoding="utf-8") as f:
            return [tuple(linha.strip().split("|")) for linha in f if linha.strip()]
    except UnicodeDecodeError:
        with open(caminho_arquivo, "r", encoding="latin-1") as f:
            return [tuple(linha.strip().split("|")) for linha in f if linha.strip()]

def salvar_plano_de_contas_lista(caminho_arquivo, plano):
    with open(caminho_arquivo, "w", encoding="utf-8", errors="replace") as f:
        for linha in plano:
            linha_limpa = []
            for item in linha:
                if isinstance(item, str):
                    item_limpo = "".join(c for c in item if c.isprintable() or c in "\t\n\r")
                    linha_limpa.append(item_limpo)
                else:
                    linha_limpa.append(str(item))
            f.write("|".join(linha_limpa) + "\n")

def adicionar_contas_a_identificar_no_plano(plano_original_path, descricoes_a_identificar, pasta_usuario="documentos/default"):
    plano = carregar_plano_de_contas_lista(plano_original_path)

    # Pega maior código existente
    codigos_num = []
    for linha in plano:
        try:
            codigos_num.append(int(re.sub(r'\D', '', linha[0])))
        except:
            pass
    max_codigo = max(codigos_num) if codigos_num else 0
    novo_codigo = max_codigo + 1
    novo_codigo_str = str(novo_codigo).zfill(3)

    # Cria entrada principal para novo grupo
    grupo_nome = f"GRUPO A IDENTIFICAR {novo_codigo_str}"
    nova_entrada_principal = (novo_codigo_str, "00000000", grupo_nome, "A")

    # Remover duplicatas por similaridade
    nomes_unicos = []
    for nome in descricoes_a_identificar:
        if not get_close_matches(nome, nomes_unicos, n=1, cutoff=0.85):
            nomes_unicos.append(nome.strip())

    subentradas = []
    for i, nome in enumerate(nomes_unicos, start=1):
        codigo_sub = f"{novo_codigo_str}-{i}"
        subentradas.append((codigo_sub, "00000000", nome, "A"))

    plano.append(nova_entrada_principal)
    plano.extend(subentradas)

    os.makedirs(pasta_usuario, exist_ok=True)
    saida_path = os.path.join(pasta_usuario, "plano_de_contas_atualizado.txt")
    salvar_plano_de_contas_lista(saida_path, plano)

    logger.info(f"✅ Novo grupo '{grupo_nome}' salvo em: {saida_path}")

    return saida_path

# === Classe Associador ===

class AssociadorPorTexto:
    def __init__(self, caminho_plano: Optional[str] = None, usuario_id: Optional[str] = "default"):
        self.caminho_plano = caminho_plano 
        self.usuario_id = usuario_id

        # Pasta para associacoes.json
        self.pasta_associacoes = os.path.join("associacoes", self.usuario_id)
        os.makedirs(self.pasta_associacoes, exist_ok=True)
        self.associacoes_path = os.path.join(self.pasta_associacoes, "associacoes.json")

        # Pasta para Excel e TXT
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

    def associar_conta_similaridade(self, descricao, plano_df, associacoes_dict, cutoff=0.5):
        descricao = descricao.strip()
        if descricao in associacoes_dict:
            return associacoes_dict[descricao].strip()
        nomes = plano_df['Nome da Conta'].tolist()
        match = get_close_matches(descricao, nomes, n=1, cutoff=cutoff)
        if match:
            conta = match[0].strip()
            associacoes_dict[descricao] = conta
            return conta
        return None

    def encontrar_conta_a_identificar(self, plano_df):
        linhas_identificar = plano_df[plano_df['Nome da Conta'].str.contains("IDENTIFICAR", case=False, na=False)]
        if not linhas_identificar.empty:
            return linhas_identificar.iloc[0]
        return None

    def consultar_chatgpt_para_associacao(self, descricoes_sem_associacao, plano_df, associacoes_dict):
        nomes_validos = sorted(set(plano_df['Nome da Conta'].tolist()))
        prompt_nomes = (
            "Você é um assistente contábil. Associe uma transação a um nome do plano de contas abaixo.\n"
            "⚠️ Use apenas um dos nomes abaixo. Se não houver correspondência lógica, use 'A IDENTIFICAR'.\n\n"
            "Nomes disponíveis:\n" + "\n".join(nomes_validos)
        )

        novas_associacoes = False

        for descricao in descricoes_sem_associacao:
            desc_limpa = descricao.strip()
            match_existente = get_close_matches(desc_limpa, associacoes_dict.keys(), n=1, cutoff=0.9)
            if match_existente:
                continue

            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": f"{prompt_nomes}\n\n{desc_limpa}"}],
                temperature=0.0,
                max_tokens=100
            )
            resposta = response.choices[0].message.content.strip()
            conta_sugerida = resposta.split("\n")[0].strip()

            if conta_sugerida not in nomes_validos:
                conta_identificar = self.encontrar_conta_a_identificar(plano_df)
                conta_sugerida = conta_identificar['Nome da Conta'] if conta_identificar is not None else "A IDENTIFICAR"

            associacoes_dict[desc_limpa] = conta_sugerida
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
        df['Conta Associada'] = df['Descrição'].apply(
            lambda desc: self.associar_conta_similaridade(desc, plano_df, associacoes)
        )
        df['Conta Código'] = None
        df['ID Estendido'] = None

        for desc in df['Descrição'].unique():
            conta_nome = associacoes.get(desc)
            if conta_nome:
                match = plano_df[plano_df['Nome da Conta'] == conta_nome]
                if len(match) == 1:
                    df.loc[df['Descrição'] == desc, 'Conta Código'] = match.iloc[0]['Conta Código']
                    df.loc[df['Descrição'] == desc, 'ID Estendido'] = match.iloc[0]['ID Estendido']

        nao_associados = df[df['Conta Associada'].isna()]['Descrição'].unique().tolist()
        if nao_associados:
            associacoes = self.consultar_chatgpt_para_associacao(nao_associados, plano_df, associacoes)
            for desc in nao_associados:
                conta = associacoes.get(desc)
                if conta:
                    df.loc[df['Descrição'] == desc, 'Conta Associada'] = conta
                    match = plano_df[plano_df['Nome da Conta'] == conta]
                    if len(match) == 1:
                        df.loc[df['Descrição'] == desc, 'Conta Código'] = match.iloc[0]['Conta Código']
                        df.loc[df['Descrição'] == desc, 'ID Estendido'] = match.iloc[0]['ID Estendido']

        self.salvar_associacoes_json(associacoes)

        contas_a_identificar = df[df['Conta Associada'].str.contains("IDENTIFICAR", case=False, na=False)]
        descricoes_a_identificar = contas_a_identificar['Descrição'].unique().tolist()

        if descricoes_a_identificar:
            adicionar_contas_a_identificar_no_plano(
                self.caminho_plano,
                descricoes_a_identificar,
                pasta_usuario=self.pasta_documentos
            )

        resultado = df[['Conta Código', 'Conta Associada', 'ID Estendido', 'Descrição', 'Valor', 'Data']]

        nome_excel = os.path.basename(caminho_saida_xlsx)
        caminho_excel_final = os.path.join(self.pasta_documentos, nome_excel)
        resultado.to_excel(caminho_excel_final, index=False)
        logger.info(f"✅ Excel salvo em: {caminho_excel_final}")

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

# === Função principal ===

def associar(texto: str, caminho_saida_xlsx: str = "saida_associada.xlsx",
             caminho_saida_txt: Optional[str] = None,
             caminho_plano: Optional[str] = None,
             usuario_id: Optional[str] = "default"):
    associador = AssociadorPorTexto(caminho_plano=caminho_plano, usuario_id=usuario_id)
    return associador.processar_texto(texto, caminho_saida_xlsx, caminho_saida_txt)
