import os
from difflib import get_close_matches

def extrair_nome_empresa(texto):
    texto = texto.lower()
    linhas = texto.splitlines()
    
    for i, linha in enumerate(linhas):
        if "recebemos de" in linha:
            inicio = linha.find("recebemos de") + len("recebemos de")
            fim = linha.find("na nota fiscal indicada ao lado")
            
            if fim == -1:  
                candidato = linha[inicio:].strip()
            else:
                candidato = linha[inicio:fim].strip()
                
            if candidato:
                return candidato.upper()
                
        elif ("razão social" in linha or "emitente" in linha) and "nome" not in linha:
            for j in range(i + 1, len(linhas)):
                candidato = linhas[j].strip()
                if candidato and not candidato.startswith(("cnpj", "ie", "endereço")):
                    return candidato.upper()
    
    for linha in linhas:
        candidato = linha.strip()
        if candidato and not candidato.startswith(("cnpj", "ie", "endereço", "danfe")):
            return candidato.upper()
    
    return "EMPRESA DESCONHECIDA"

def carregar_plano_de_contas(caminho_arquivo):
    if not os.path.exists(caminho_arquivo):
        raise ValueError("Arquivo de plano de contas não encontrado")
    
    try:
        with open(caminho_arquivo, "r", encoding="utf-8") as f:
            return [tuple(linha.strip().split("|")) for linha in f if linha.strip()]
    except UnicodeDecodeError:
        with open(caminho_arquivo, "r", encoding="latin-1") as f:
            conteudo = f.read()
        return [tuple(linha.strip().split("|")) for linha in conteudo.splitlines() if linha.strip()]

def salvar_plano_de_contas(caminho_arquivo, plano):
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
        return plano, similar

    novo_codigo = str(max([int(l[0]) for l in plano] + [0]) + 1).zfill(3)
    novo_contabil = str(max([int(l[1]) for l in plano] + [11102000]) + 1)
    nova_entrada = (novo_codigo, novo_contabil, nome_empresa, "A")
    
    print(f"Adicionando nova empresa: {nova_entrada}")
    plano.append(nova_entrada)
    return plano, nome_empresa

def processar_nota_fiscal_com_plano(texto_nota, caminho_plano):
    """
    Processa a nota fiscal com o plano de contas fornecido
    Retorna um dicionário com os resultados
    """
    nome_empresa = extrair_nome_empresa(texto_nota)
    if nome_empresa == "EMPRESA DESCONHECIDA":
        return {
            "status": "erro",
            "mensagem": "Não foi possível identificar o nome da empresa",
            "empresa": None,
            "plano_atualizado": False
        }
    
    plano = carregar_plano_de_contas(caminho_plano)
    plano, empresa = adicionar_empresa(nome_empresa, plano)
    salvar_plano_de_contas(caminho_plano, plano)
    
    return {
        "status": "sucesso",
        "empresa": empresa,
        "plano_atualizado": True,
        "mensagem": "Plano de contas atualizado com sucesso"
    }