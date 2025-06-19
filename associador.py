import pandas as pd
import json
import os
import logging
from openai import OpenAI
from difflib import get_close_matches
from ofxparse import OfxParser
from dotenv import load_dotenv
from typing import Union, BinaryIO

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("API_KEY")

client = OpenAI(api_key=API_KEY)

class Associador:
    def __init__(self, caminho_plano=None):
        self.caminho_plano = caminho_plano or "plano_de_contas.txt"
        self.associacoes_path = "associacoes.json"

    def ler_plano_de_contas(self):
        codigos = []
        nomes = []
        try:
            # Tenta primeiro com UTF-8
            with open(self.caminho_plano, 'r', encoding='utf-8') as f:
                for linha in f:
                    partes = linha.strip().split('|')
                    if len(partes) >= 3:
                        codigos.append(partes[0].strip())
                        nomes.append(partes[2].strip())
            return pd.DataFrame({'Conta Código': codigos, 'Nome da Conta': nomes})
        except UnicodeDecodeError:
            # Fallback para latin-1 se UTF-8 falhar
            try:
                with open(self.caminho_plano, 'r', encoding='latin-1') as f:
                    for linha in f:
                        partes = linha.strip().split('|')
                        if len(partes) >= 3:
                            codigos.append(partes[0].strip())
                            nomes.append(partes[2].strip())
                return pd.DataFrame({'Conta Código': codigos, 'Nome da Conta': nomes})
            except Exception as e:
                logger.error(f"Erro ao ler plano de contas (latin-1): {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Erro ao ler plano de contas: {str(e)}")
            raise

    def ler_ofx(self, file_input: Union[str, BinaryIO]):
        """Aceita tanto caminho do arquivo quanto objeto de arquivo binário"""
        try:
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

        except Exception as e:
            logger.error(f"Erro ao ler OFX: {str(e)}")
            raise ValueError(f"Formato OFX inválido: {str(e)}")

    def carregar_associacoes_json(self):
        try:
            if os.path.exists(self.associacoes_path):
                with open(self.associacoes_path, 'r', encoding='utf-8') as f:
                    associacoes = json.load(f)
                    return {k.strip(): v.strip() for k, v in associacoes.items()}
            return {}
        except Exception as e:
            logger.error(f"Erro ao carregar associações: {str(e)}")
            return {}

    def salvar_associacoes_json(self, associacoes):
        try:
            with open(self.associacoes_path, 'w', encoding='utf-8') as f:
                json.dump(associacoes, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar associações: {str(e)}")
            raise

    def associar_conta_similaridade(self, descricao, plano_df, associacoes_dict, cutoff=0.20):
        try:
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
        except Exception as e:
            logger.error(f"Erro na associação por similaridade: {str(e)}")
            return None

    def consultar_chatgpt_para_associacao(self, descricoes_sem_associacao, plano_df):
        try:
            nomes_validos = sorted(set(plano_df['Nome da Conta'].tolist()))

            prompt = (
                "Você é um assistente contábil. Sua tarefa é associar descrições de transações bancárias a nomes do plano de contas a seguir.\n"
                "⚠️ Regras importantes:\n"
                "- Use **exatamente um dos nomes do plano de contas** como resposta.\n"
                "- Nunca repita a descrição como nome de conta.\n"
                "- Responda no formato: [descrição] -> [nome da conta do plano]\n\n"
                "Nomes disponíveis no plano de contas:\n"
            )

            for nome in nomes_validos:
                prompt += f"{nome}\n"

            prompt += "\nDescrições para associar:\n"
            for desc in descricoes_sem_associacao:
                prompt += f"{desc}\n"

            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=800
            )
            resposta = response.choices[0].message.content

            sugestoes = {}
            for linha in resposta.splitlines():
                if "->" in linha:
                    partes = linha.split("->", 1)
                    descricao = partes[0].strip()
                    conta = partes[1].strip()
                    if conta in nomes_validos and descricao in descricoes_sem_associacao:
                        sugestoes[descricao] = conta
                    else:
                        logger.warning(f"Ignorado: '{descricao} -> {conta}' (conta inválida ou repetida)")
            return sugestoes

        except Exception as e:
            logger.error(f"Erro ao consultar ChatGPT: {str(e)}")
            return {}

    def processar_extrato(self, file_input: Union[str, BinaryIO], caminho_saida_xlsx: str, caminho_saida_txt: str = None):
        """
        Processa o extrato OFX, associa contas, salva planilha Excel e opcionalmente gera arquivo TXT.
        """
        try:
            extrato_df = self.ler_ofx(file_input)
            plano_df = self.ler_plano_de_contas()

            plano_df['Nome da Conta'] = plano_df['Nome da Conta'].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )

            associacoes = self.carregar_associacoes_json()

            # Associa contas por similaridade ou usando associações salvas
            extrato_df['Conta Associada'] = extrato_df['Descrição'].apply(
                lambda desc: self.associar_conta_similaridade(desc, plano_df, associacoes)
            )

            nao_associados = extrato_df[extrato_df['Conta Associada'].isna()]['Descrição'].unique().tolist()

            if nao_associados:
                novas_associacoes = self.consultar_chatgpt_para_associacao(nao_associados, plano_df)
                for desc, conta in novas_associacoes.items():
                    conta_limpa = conta.strip()
                    associacoes[desc.strip()] = conta_limpa
                    extrato_df.loc[extrato_df['Descrição'] == desc, 'Conta Associada'] = conta_limpa

                df_chatgpt = extrato_df[extrato_df['Descrição'].isin(novas_associacoes.keys())].copy()
                df_chatgpt = df_chatgpt.merge(
                    plano_df, left_on='Conta Associada', right_on='Nome da Conta', how='left'
                )
                df_chatgpt = df_chatgpt[['Conta Código', 'Descrição', 'Nome da Conta', 'Valor', 'Crédito/Débito', 'Data']]
                caminho_chatgpt = caminho_saida_xlsx.replace('.xlsx', '_chatgpt.xlsx')
                df_chatgpt.to_excel(caminho_chatgpt, index=False)
                logger.info(f"✅ Planilha com respostas do ChatGPT salva em: {caminho_chatgpt}")

            extrato_df['Conta Associada'] = extrato_df['Conta Associada'].apply(
                lambda x: x.strip() if isinstance(x, str) else x
            )

            self.salvar_associacoes_json(associacoes)

            resultado = extrato_df.merge(
                plano_df, left_on='Conta Associada', right_on='Nome da Conta', how='left'
            )

            resultado = resultado[['Conta Código', 'Descrição', 'Nome da Conta', 'Valor', 'Crédito/Débito', 'Data']]

            # Salva Excel
            resultado.to_excel(caminho_saida_xlsx, index=False)
            logger.info(f"✅ Arquivo Excel salvo em: {caminho_saida_xlsx}")

            # Se caminho_saida_txt foi informado, gera arquivo TXT com formato customizado
            if caminho_saida_txt:
                with open(caminho_saida_txt, 'w', encoding='utf-8') as f:
                    # Exemplo de cabeçalho fixo, você pode adaptar conforme necessário
                    f.write("|0000|32662718000130|\n")  # Pode parametrizar se quiser
                    for _, row in resultado.iterrows():
                        f.write("|6000|X||||\n")
                        # Exemplo formatado de linha 6100 com campos (Data, Conta Código, Conta Associada e Valor)
                        # Como você pediu, preencher os campos segundo o padrão:
                        # |6100|Data|Conta Código|???|Valor|...|Descrição|||||
                        # Vou considerar que o campo após Conta Código é um código fixo (por ex. 8), e outro fixo (11) — ajuste se quiser
                        # Para seu exemplo, vou deixar campos 4 e 5 vazios exceto os que você pediu.

                        # Campos para linha 6100:
                        data = row['Data']
                        conta_codigo = row['Conta Código'] or ""
                        # Vou usar placeholders 8 e 11 para os campos fixos conforme seu exemplo:
                        campo4 = "8"
                        campo5 = "11"
                        valor = f"{row['Valor']:.2f}".replace('.', ',')  # valor com vírgula decimal
                        descricao = row['Descrição'] or ""

                        linha_6100 = f"|6100|{data}|{conta_codigo}|{campo4}|{valor}| |{descricao}|||||\n"
                        f.write(linha_6100)

                logger.info(f"✅ Arquivo TXT salvo em: {caminho_saida_txt}")

            return caminho_saida_xlsx

        except Exception as e:
            logger.error(f"❌ Erro no processamento do extrato: {str(e)}")
            raise

# Função compatível para uso direto
def processar_extrato(caminho_ofx, caminho_saida_xlsx):
    associador = Associador()
    return associador.processar_extrato(caminho_ofx, caminho_saida_xlsx)
