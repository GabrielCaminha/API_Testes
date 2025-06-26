import pandas as pd
import json
import os
import logging
from openai import OpenAI
from difflib import get_close_matches
from dotenv import load_dotenv
from typing import Optional
import re

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
API_KEY = os.getenv("API_KEY2")

client = OpenAI(api_key=API_KEY)

class AssociadorPorTexto:
    def __init__(self, caminho_plano: Optional[str] = None, usuario_id: Optional[str] = "default"):
        self.caminho_plano = caminho_plano or "plano_de_contas.txt"
        self.usuario_id = usuario_id
        self.pasta_associacoes = os.path.join("associacoes", self.usuario_id)
        os.makedirs(self.pasta_associacoes, exist_ok=True)
        self.associacoes_path = os.path.join(self.pasta_associacoes, "associacoes.json")

    def ler_plano_de_contas(self):
        codigos, ids_estendidos, nomes = [], [], []
        try:
            with open(self.caminho_plano, 'r', encoding='utf-8') as f:
                for linha in f:
                    partes = linha.strip().split('|')
                    if len(partes) >= 3:
                        codigos.append(partes[0].strip())
                        ids_estendidos.append(partes[1].strip())
                        nomes.append(partes[2].strip())
        except UnicodeDecodeError:
            with open(self.caminho_plano, 'r', encoding='latin-1') as f:
                for linha in f:
                    partes = linha.strip().split('|')
                    if len(partes) >= 3:
                        codigos.append(partes[0].strip())
                        ids_estendidos.append(partes[1].strip())
                        nomes.append(partes[2].strip())
        return pd.DataFrame({'Conta Código': codigos, 'ID Estendido': ids_estendidos, 'Nome da Conta': nomes})

    def carregar_associacoes_json(self):
        try:
            if os.path.exists(self.associacoes_path):
                with open(self.associacoes_path, 'r', encoding='utf-8') as f:
                    associacoes = json.load(f)
                    return {k.strip(): v.strip() for k, v in associacoes.items()}
            return {}
        except Exception as e:
            logger.error(f"Erro ao carregar associações: {e}")
            return {}

    def salvar_associacoes_json(self, associacoes):
        try:
            with open(self.associacoes_path, 'w', encoding='utf-8') as f:
                json.dump(associacoes, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erro ao salvar associações: {e}")
            raise

    def associar_conta_similaridade(self, descricao, plano_df, associacoes_dict, cutoff=0.35):
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
            logger.error(f"Erro na associação por similaridade: {e}")
            return None

    def consultar_chatgpt_para_associacao(self, descricoes_sem_associacao, plano_df):
        try:
            nomes_validos = sorted(set(plano_df['Nome da Conta'].tolist()))
            prompt = (
                "Você é um assistente contábil. Sua tarefa é associar descrições de transações bancárias a nomes do plano de contas a seguir.\n"
                "⚠️ Regras importantes:\n"
                "- Use **exatamente um dos nomes do plano de contas** como resposta.\n"
                "- Nunca repita a descrição como nome de conta.\n"
                "- Caso não encontre uma associação possivel, use a conta padrão de A IDENTIFICAR que esta dentro do plano de contas.\n"
                "- A cade item que você identificar, os proximos que forem praticamente identicos a um já encontrado devem ter a mesma associação.\n"
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
            return sugestoes

        except Exception as e:
            logger.error(f"Erro ao consultar ChatGPT: {e}")
            return {}

    def processar_texto(self, texto: str, caminho_saida_xlsx: str, caminho_saida_txt: Optional[str] = None):
        def limpar_valor(valor_str):
            if not isinstance(valor_str, str):
                return None
            valor_str = valor_str.strip()
            negativo = valor_str.startswith('-')
            valor_limpo = re.sub(r'[^\d,.-]', '', valor_str)
            valor_limpo = valor_limpo.replace('.', '').replace(',', '.')
            try:
                valor_num = float(valor_limpo)
                if negativo and valor_num > 0:
                    valor_num = -valor_num
                return valor_num
            except:
                return None

        try:
            from io import StringIO
            df = pd.read_csv(StringIO(texto), sep='|', skipinitialspace=True,
                             names=['Data', 'Descrição', 'Valor'], skiprows=1,
                             dayfirst=True, engine='python')

            df['Descrição'] = df['Descrição'].astype(str).str.strip()
            df['Data'] = pd.to_datetime(df['Data'], dayfirst=True, errors='coerce').dt.strftime('%d/%m/%Y')
            df['Valor'] = df['Valor'].apply(limpar_valor)

            plano_df = self.ler_plano_de_contas()
            plano_df['Nome da Conta'] = plano_df['Nome da Conta'].apply(lambda x: x.strip() if isinstance(x, str) else x)

            associacoes = self.carregar_associacoes_json()
            df['Conta Associada'] = df['Descrição'].apply(
                lambda desc: self.associar_conta_similaridade(desc, plano_df, associacoes)
            )

            nao_associados = df[df['Conta Associada'].isna()]['Descrição'].unique().tolist()

            if nao_associados:
                novas_associacoes = self.consultar_chatgpt_para_associacao(nao_associados, plano_df)
                for desc, conta in novas_associacoes.items():
                    conta_limpa = conta.strip()
                    associacoes[desc.strip()] = conta_limpa
                    df.loc[df['Descrição'] == desc, 'Conta Associada'] = conta_limpa

                df_chatgpt = df[df['Descrição'].isin(novas_associacoes.keys())].copy()
                df_chatgpt = df_chatgpt.merge(plano_df, left_on='Conta Associada', right_on='Nome da Conta', how='left')
                df_chatgpt = df_chatgpt[['Conta Código', 'Descrição', 'Nome da Conta', 'Valor', 'Data']]
                caminho_chatgpt = caminho_saida_xlsx.replace('.xlsx', '_chatgpt.xlsx')
                df_chatgpt.to_excel(caminho_chatgpt, index=False)
                logger.info(f"✅ Planilha com respostas do ChatGPT salva em: {caminho_chatgpt}")

            df['Conta Associada'] = df['Conta Associada'].apply(lambda x: x.strip() if isinstance(x, str) else x)
            self.salvar_associacoes_json(associacoes)

            resultado = df.merge(plano_df, left_on='Conta Associada', right_on='Nome da Conta', how='left')
            resultado = resultado[['Conta Código', 'ID Estendido', 'Descrição', 'Nome da Conta', 'Valor', 'Data']]

            resultado.to_excel(caminho_saida_xlsx, index=False)
            logger.info(f"✅ Arquivo Excel salvo em: {caminho_saida_xlsx}")

            if caminho_saida_txt:
                with open(caminho_saida_txt, 'w', encoding='utf-8') as f:
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

                logger.info(f"✅ Arquivo TXT salvo em: {caminho_saida_txt}")

            return resultado.to_dict(orient='records')

        except Exception as e:
            logger.error(f"❌ Erro no processamento do texto: {e}")
            raise

# ✅ Função externa para uso no FastAPI
def associar(texto: str, caminho_saida_xlsx: str = "saida_associada.xlsx", caminho_saida_txt: Optional[str] = None, caminho_plano: Optional[str] = None, usuario_id: Optional[str] = "default"):
    associador = AssociadorPorTexto(caminho_plano=caminho_plano, usuario_id=usuario_id)
    return associador.processar_texto(texto, caminho_saida_xlsx, caminho_saida_txt)
