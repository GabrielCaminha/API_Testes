import pandas as pd
import json
import os
from openai import OpenAI
from difflib import get_close_matches
from ofxparse import OfxParser
from dotenv import load_dotenv

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
        with open(self.caminho_plano, 'r', encoding='utf-8') as f:
            for linha in f:
                partes = linha.strip().split('|')
                if len(partes) >= 3:
                    codigos.append(partes[0].strip())
                    nomes.append(partes[2].strip())
        return pd.DataFrame({'Conta Código': codigos, 'Nome da Conta': nomes})

    def ler_ofx(self, caminho_ofx):
        with open(caminho_ofx, 'rb') as f:
            ofx = OfxParser.parse(f)

        transacoes = []
        for account in ofx.accounts:
            for t in account.statement.transactions:
                data = t.date.strftime("%d/%m/%Y")
                descricao = t.memo or t.payee or ""
                valor = t.amount
                tipo = t.type.upper()  # CREDIT ou DEBIT

                transacoes.append({
                    "Data": data,
                    "Descrição": descricao,
                    "Valor": valor,
                    "Crédito/Débito": tipo,
                    "Saldo": None
                })

        return pd.DataFrame(transacoes)

    def carregar_associacoes_json(self):
        if os.path.exists(self.associacoes_path):
            with open(self.associacoes_path, 'r', encoding='utf-8') as f:
                associacoes = json.load(f)
                return {k.strip(): v.strip() for k, v in associacoes.items()}
        return {}

    def salvar_associacoes_json(self, associacoes):
        with open(self.associacoes_path, 'w', encoding='utf-8') as f:
            json.dump(associacoes, f, indent=2, ensure_ascii=False)

    def associar_conta_similaridade(self, descricao, plano_df, associacoes_dict, cutoff=0.20):
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

        try:
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
                        print(f"⚠️ Ignorado: '{descricao} -> {conta}' (conta inválida ou repetida)")

            return sugestoes

        except Exception as e:
            print(f"⚠️ Erro ao consultar ou interpretar a resposta do ChatGPT: {e}")
            return {}

    def processar_extrato(self, caminho_ofx, caminho_saida=None):
        caminho_saida = caminho_saida or "resultado.xlsx"
        
        extrato_df = self.ler_ofx(caminho_ofx)
        plano_df = self.ler_plano_de_contas()

        plano_df['Nome da Conta'] = plano_df['Nome da Conta'].apply(lambda x: x.strip() if isinstance(x, str) else x)

        associacoes = self.carregar_associacoes_json()

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

        extrato_df['Conta Associada'] = extrato_df['Conta Associada'].apply(lambda x: x.strip() if isinstance(x, str) else x)

        self.salvar_associacoes_json(associacoes)

        resultado = extrato_df.merge(
            plano_df, left_on='Conta Associada', right_on='Nome da Conta', how='left'
        )

        resultado = resultado[['Conta Código', 'Descrição', 'Nome da Conta', 'Valor', 'Crédito/Débito', 'Data']]
        resultado.to_excel(caminho_saida, index=False)

        print(f"\n✅ Arquivo salvo em: {caminho_saida}")
        return caminho_saida

# Função de compatibilidade para o código existente
def processar_extrato(caminho_ofx, caminho_saida=None):
    associador = Associador()
    return associador.processar_extrato(caminho_ofx, caminho_saida)