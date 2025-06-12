import os
from difflib import get_close_matches

PLANO_CONTAS_PATH = "plano_de_contas.txt"

def extrair_nome_empresa(texto):
    #Sempre que surgir um novo tipo de nota fiscal com uma escrita diferente sua regra pra leitura correta deve ser adicionada aqui
    texto = texto.lower()
    linhas = texto.splitlines()
    
    for i, linha in enumerate(linhas):
        if "recebemos de" in linha:
            # Caso especial para "recebemos de"
            inicio = linha.find("recebemos de") + len("recebemos de")
            fim = linha.find("na nota fiscal indicada ao lado")
            
            if fim == -1:  
                candidato = linha[inicio:].strip()
            else:
                candidato = linha[inicio:fim].strip()
                
            if candidato:
                return candidato.upper()
                
        elif ("razão social" in linha or "emitente" in linha) and "nome" not in linha:
            # Processamento original para outros casos
            for j in range(i + 1, len(linhas)):
                candidato = linhas[j].strip()
                if candidato and not candidato.startswith(("cnpj", "ie", "endereço")):
                    return candidato.upper()
    
    # Fallback genérico
    for linha in linhas:
        candidato = linha.strip()
        if candidato and not candidato.startswith(("cnpj", "ie", "endereço", "danfe")):
            return candidato.upper()
    
    return "EMPRESA DESCONHECIDA"

def carregar_plano_de_contas():
    if not os.path.exists(PLANO_CONTAS_PATH):
        return []
    with open(PLANO_CONTAS_PATH, "r", encoding="latin-1") as f:
        return [tuple(linha.strip().split("|")) for linha in f if linha.strip()]

def salvar_plano_de_contas(plano):
    with open(PLANO_CONTAS_PATH, "w", encoding="utf-8") as f:
        for linha in plano:
            f.write("|".join(linha) + "\n")

def empresa_já_existente(nome_empresa, plano, similaridade=0.8):
    nomes_existentes = [linha[2] for linha in plano]
    
    if nome_empresa in nomes_existentes:
        return True, nome_empresa
    
    similares = get_close_matches(nome_empresa, nomes_existentes, n=1, cutoff=similaridade)
    if similares:
        return True, similares[0]
    
    return False, None

def adicionar_empresa(nome_empresa, plano):
    existe, similar = empresa_já_existente(nome_empresa, plano)
    if existe:
        print(f"Empresa '{nome_empresa}' já está no plano de contas (encontrado: '{similar}').")
        return plano

    novo_codigo = str(max([int(l[0]) for l in plano] + [0]) + 1).zfill(3)
    novo_contabil = str(max([int(l[1]) for l in plano] + [11102000]) + 1)
    nova_entrada = (novo_codigo, novo_contabil, nome_empresa, "A")
    
    print(f"Adicionando nova empresa: {nova_entrada}")
    plano.append(nova_entrada)
    return plano

def processar_nota_fiscal(texto):
    """
    Função que processa o texto já extraído do PDF e adiciona a empresa ao plano de contas.
    """
    print("Processando texto da nota fiscal...")
    nome_empresa = extrair_nome_empresa(texto)
    print(texto)
    if nome_empresa == "EMPRESA DESCONHECIDA":
        print("\nATENÇÃO: Não foi possível identificar o nome da empresa. Não será adicionado ao plano de contas.\n")
        return
    print(f"\n===== EMPRESA EXTRAÍDA: {nome_empresa} =====\n")
    plano = carregar_plano_de_contas()
    plano = adicionar_empresa(nome_empresa, plano)
    salvar_plano_de_contas(plano)
