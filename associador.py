import pandas as pd
import json
import os
import uuid
from openai import OpenAI
from difflib import get_close_matches
from ofxparse import OfxParser
from dotenv import load_dotenv
import logging
from typing import Union, BinaryIO

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("associador")

load_dotenv()
API_KEY = os.getenv("API_KEY")
client = OpenAI(api_key=API_KEY)


class Associador:
    def __init__(self, caminho_plano=None):
        self.caminho_plano = caminho_plano or "plano_de_contas.txt"
        self.associacoes_path = "associacoes.json"

    def carregar_associacoes_json(self):
        if os.path.exists(self.associacoes_path):
            try:
                with open(self.associacoes_path, 'r', encoding='utf-8') as f:
                    conteudo = f.read().strip()
                    if conteudo:
                        return json.loads(conteudo)
                    else:
                        logger.warning(f"Arquivo {self.associacoes_path} está vazio.")
                        return {}
            except Exception as e:
                logger.error(f"Erro ao carregar associações existentes: {str(e)}")
                return {}
        return {}

    def salvar_associacoes_json(self, associacoes):
        try:
            with open(self.associacoes_path, 'w', encoding='utf-8') as f:
                json.dump(associacoes, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar associações: {str(e)}")
            raise

    def ler_plano_de_contas(self):
        codigos, nomes = [], []
        with open(self.caminho_plano, 'r', encoding='latin-1') as f:
            for linha in f:
                partes = linha.strip().split('|')
                if len(partes) >= 3:
                    codigos.append(partes[0].strip())
                    nomes.append(partes[2].strip())
        return pd.DataFrame({'Conta Código': codigos, 'Nome da Conta': nomes})

    def ler_ofx(self, file_input: Union[str, BinaryIO]):
        if isinstance(file_input, str):
            with open(file_input, 'rb') as f:
                ofx = OfxParser.parse(f)
        else:
            if hasattr(file_input, 'seek'):
                file_input.seek(0)
            ofx = OfxParser.parse(file_input)

        transacoes = []
        for account in ofx.accounts:
            for t in account.statement.transactions:
                transacoes.append({
                    "Data": t.date.strftime("%d/%m/%Y"),
                    "Descrição": t.memo or t.payee or "",
                    "Valor": t.amount,
                    "Crédito/Débito": t.type.upper(),
                    "Saldo": None
                })
        return pd.DataFrame(transacoes)

    def associar_por_similaridade(self, descricao, plano_df, associacoes_dict, cutoff=0.80):
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

    def consultar_chatgpt_para_associacao(self, descricoes_sem_associacao, plano_df):
        try:
            nomes_validos = sorted(set(plano_df['Nome da Conta'].tolist()))

            prompt = (
                "Você é um assistente contábil. Sua tarefa é associar as descrições abaixo "
                "ao nome correto do plano de contas. ⚠️ Regras importantes:\n"
                "- Sempre escolha exatamente um dos nomes do plano de contas.\n"
                "- Nunca repita a descrição como nome da conta.\n"
                "- Formato da resposta: [descrição] -> [nome da conta]\n\n"
                "Nomes disponíveis no plano de contas:\n"
            )

            for nome in nomes_validos:
                prompt += f"- {nome}\n"

            prompt += "\nDescrições para associar:\n"
            for desc in descricoes_sem_associacao:
                prompt += f"- {desc}\n"

            response = client.chat.completions.create(
                model="gpt-3.5-turbo-1106",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=3000
            )

            resposta = response.choices[0].message.content
            sugestoes = {}
            for linha in resposta.splitlines():
                if "->" in linha:
                    partes = linha.split("->", 1)
                    descricao = partes[0].strip().lstrip("-").strip()
                    conta = partes[1].strip()
                    if conta in nomes_validos and descricao in descricoes_sem_associacao:
                        sugestoes[descricao] = conta
                    else:
                        logger.warning(f"Ignorado: '{descricao} -> {conta}' (conta inválida ou não reconhecida)")

            return sugestoes

        except Exception as e:
            logger.error(f"Erro ao consultar ChatGPT: {str(e)}")
            return {}

    def processar_extrato(self, file_input: Union[str, BinaryIO], caminho_saida, salvar_associacoes_gpt=False):
        extrato_df = self.ler_ofx(file_input)
        plano_df = self.ler_plano_de_contas()

        plano_df['Nome da Conta'] = plano_df['Nome da Conta'].apply(lambda x: x.strip())
        associacoes = self.carregar_associacoes_json()

        # Associações por Similaridade
        extrato_df['Conta Similaridade'] = extrato_df['Descrição'].apply(
            lambda desc: self.associar_por_similaridade(desc, plano_df, associacoes)
        )

        nao_associados = extrato_df[extrato_df['Conta Similaridade'].isna()]['Descrição'].unique().tolist()

        chatgpt_sugestoes = {}
        if nao_associados:
            chatgpt_sugestoes = self.consultar_chatgpt_para_associacao(nao_associados, plano_df)
            extrato_df['Conta ChatGPT'] = extrato_df['Descrição'].map(chatgpt_sugestoes)

            if salvar_associacoes_gpt:
                for desc, conta in chatgpt_sugestoes.items():
                    associacoes[desc] = conta
        else:
            extrato_df['Conta ChatGPT'] = None

        self.salvar_associacoes_json(associacoes)

        # Para prefixar os arquivos com um código (por exemplo o UUID)
        prefixo_codigo = str(uuid.uuid4())[:8]
        pasta_saida = os.path.dirname(caminho_saida) if caminho_saida else "."

        # Preparar DataFrame para similaridade, incluindo código da conta
        df_similaridade = extrato_df[extrato_df['Conta Similaridade'].notna()][
            ['Data', 'Descrição', 'Valor', 'Crédito/Débito', 'Conta Similaridade']
        ]

        # Fazer merge para adicionar o código da conta correspondente
        df_similaridade = df_similaridade.merge(
            plano_df[['Conta Código', 'Nome da Conta']],
            left_on='Conta Similaridade',
            right_on='Nome da Conta',
            how='left'
        ).drop(columns=['Nome da Conta'])

        # Reorganizar colunas para colocar código da conta no começo
        df_similaridade = df_similaridade[
            ['Conta Código', 'Data', 'Descrição', 'Valor', 'Crédito/Débito', 'Conta Similaridade']
        ]

        caminho_similaridade = os.path.join(pasta_saida, f"{prefixo_codigo}_similaridade.xlsx")
        df_similaridade.to_excel(caminho_similaridade, index=False)
        logger.info(f"✅ Resultado por similaridade salvo: {caminho_similaridade}")

        # Preparar DataFrame para resultados do ChatGPT, se houver
        if chatgpt_sugestoes:
            df_gpt = extrato_df[extrato_df['Conta ChatGPT'].notna()][
                ['Data', 'Descrição', 'Valor', 'Crédito/Débito', 'Conta ChatGPT']
            ]

            # Merge para adicionar código da conta
            df_gpt = df_gpt.merge(
                plano_df[['Conta Código', 'Nome da Conta']],
                left_on='Conta ChatGPT',
                right_on='Nome da Conta',
                how='left'
            ).drop(columns=['Nome da Conta'])

            df_gpt = df_gpt[
                ['Conta Código', 'Data', 'Descrição', 'Valor', 'Crédito/Débito', 'Conta ChatGPT']
            ]

            caminho_gpt = os.path.join(pasta_saida, f"{prefixo_codigo}_chatgpt.xlsx")
            df_gpt.to_excel(caminho_gpt, index=False)
            logger.info(f"✅ Resultado ChatGPT salvo: {caminho_gpt}")
        else:
            caminho_gpt = None

        return caminho_similaridade, caminho_gpt


def processar_extrato(caminho_ofx, caminho_saida=None, salvar_associacoes_gpt=False):
    associador = Associador()
    return associador.processar_extrato(caminho_ofx, caminho_saida, salvar_associacoes_gpt)
