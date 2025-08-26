import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import os

# Caminhos
pasta_dados = r"C:\Users\veron\OneDrive\Desktop\dados"
arquivo_saida = os.path.join(pasta_dados, "populacao_ibge_final_2019_2023.csv")

# Lista das UFs válidas
ufs = ['RO', 'AC', 'AM', 'RR', 'PA', 'AP', 'TO', 'MA', 'PI', 'CE', 'RN', 'PB', 'PE',
       'AL', 'SE', 'BA', 'MG', 'ES', 'RJ', 'SP', 'PR', 'SC', 'RS', 'MS', 'MT', 'GO', 'DF']

# Anos a processar
anos = [2019, 2020, 2021, 2022, 2023]
lista_df = []

for ano in anos:
    print(f"🔄 Processando {ano}...")
    
    caminho = os.path.join(pasta_dados, f"populacao_uf_{ano}.xlsx")
    df = pd.read_excel(caminho, skiprows=1, header=None)
    
    # Remove colunas totalmente nulas
    df = df.dropna(axis=1, how='all')
    
    if df.shape[1] >= 2:
        df = df.iloc[:, [0, 1]]
        df.columns = ["UF", "Populacao"]
    else:
        raise ValueError(f"O arquivo de {ano} não possui ao menos duas colunas úteis.")

    df = df[df["UF"].isin(ufs)]

    # Corrige população
    df["Populacao"] = (
        df["Populacao"]
        .astype(str)
        .str.replace(r"[^\d]", "", regex=True)
        .astype(int)
    )

    df["Ano"] = int(ano)
    lista_df.append(df)

# Junta todos os anos
df_final = pd.concat(lista_df, ignore_index=True)

# Converte coluna Ano para inteiro
df_final["Ano"] = df_final["Ano"].astype(int)

# Salva o CSV consolidado
df_final.to_csv(arquivo_saida, index=False, encoding="utf-8-sig")
print(f"\n✅ Arquivo salvo com sucesso em: {arquivo_saida}")

# Agrupa população total por ano
df_total = df_final.groupby("Ano")["Populacao"].sum().reset_index()

# Gráfico 1 — População por UF (linhas)
plt.figure(figsize=(16, 8))
for uf in ufs:
    dados_uf = df_final[df_final["UF"] == uf].sort_values("Ano")
    plt.plot(dados_uf["Ano"], dados_uf["Populacao"], marker='o', label=uf)

plt.title("População dos Estados Brasileiros (2019–2023)")
plt.xlabel("Ano")
plt.ylabel("População")
plt.xticks(df_total["Ano"])  # Eixo X com anos inteiros
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
plt.tight_layout()
plt.savefig(os.path.join(pasta_dados, "grafico_populacao_linhas.png"), dpi=300)
plt.show()

# Gráfico 2 — População total do Brasil por ano
plt.figure(figsize=(10, 6))
plt.plot(df_total["Ano"], df_total["Populacao"], marker='o', color='darkblue')
plt.title("População Total do Brasil (2019–2023)")
plt.xlabel("Ano")
plt.ylabel("População Total")
plt.xticks(df_total["Ano"])  # Eixo X com anos inteiros
plt.grid(True, linestyle='--', alpha=0.7)
plt.gca().yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f'{int(x):,}'.replace(',', '.')))
plt.tight_layout()
plt.savefig(os.path.join(pasta_dados, "grafico_populacao_total_ano.png"), dpi=300)
plt.show()
# %%

import requests
import pandas as pd
import time

# Lista dos códigos das UFs (estados brasileiros no IBGE)
ufs = [
    11, 12, 13, 14, 15, 16, 17, 21, 22, 23, 24, 25, 26, 27,
    28, 29, 31, 32, 33, 35, 41, 42, 43, 50, 51, 52, 53
]

anos = range(2019, 2024)  # de 2019 a 2023
bimestres = range(1, 7)   # 1º ao 6º bimestre

# Lista de tributos que queremos extrair
tributos_desejados = ['ICMS', 'IPVA', 'ISS', 'ITCMD']

# Lista para armazenar os resultados
dados = []

for ano in anos:
    for bimestre in bimestres:
        for uf in ufs:
            url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"
            params = {
                "an_exercicio": ano,
                "nr_periodo": bimestre,
                "co_uf": uf,
                "nr_anexo": 8
            }

            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                registros = response.json().get("items", [])

                icms = ipva = iss = itcmd = 0

                for item in registros:
                    descricao = item.get("no_conta_contabil", "").upper()
                    valor = item.get("vl_valor", 0) or 0

                    if "ICMS" in descricao:
                        icms += valor
                    elif "IPVA" in descricao:
                        ipva += valor
                    elif "ISS" in descricao or "IMPOSTO SOBRE SERVIÇOS" in descricao:
                        iss += valor
                    elif "ITCMD" in descricao:
                        itcmd += valor

                arrec_total = icms + ipva + iss + itcmd

                dados.append({
                    "ano": ano,
                    "bimestre": bimestre,
                    "uf": uf,
                    "ICMS": icms,
                    "IPVA": ipva,
                    "ISS": iss,
                    "ITCMD": itcmd,
                    "arrecadacao_total": arrec_total
                })

                print(f"✔️ {ano} - Bim {bimestre} - UF {uf} concluído.")
                time.sleep(1.1)  # para respeitar o limite da API (1 req/s)

            except Exception as e:
                print(f"❌ Erro: {ano} Bim {bimestre} UF {uf} -> {e}")
                continue

# Converter em DataFrame e salvar
df_final = pd.DataFrame(dados)
df_final.to_csv("arrecadacao_estadual_2019_2023.csv", index=False)
print("✅ Arquivo salvo como 'arrecadacao_estadual_2019_2023.csv'")

# %%

import requests
import pandas as pd

url = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/rreo"
params = {
    "an_exercicio": 2023,
    "co_uf": 35,
    "nr_periodo": 1,
    "nr_anexo": 8
}

response = requests.get(url, params=params)
data = response.json()

# Mostra as 10 primeiras descrições de contas
for item in data.get("items", [])[:10]:
    print(item["no_conta_contabil"], "->", item["vl_valor"])
# %%

import pandas as pd
import os

# Caminhos
pasta_dados = r"C:\Users\veron\OneDrive\Desktop\dados"
caminho_entrada = os.path.join(pasta_dados, "Arrecadacao por setor.xlsx")
caminho_saida = os.path.join(pasta_dados, "arrecadacao_estadual_2019_2023.csv")

# Carrega a aba correta
df = pd.read_excel(caminho_entrada, sheet_name='arrecadacao por setor ')

# Seleciona as colunas de interesse
colunas = [
    "no_uf", "ano",
    "va_icms_total",
    "va_outros_tributos_ipva",
    "va_outros_tributos_itcd",
    "va_receita_tributaria_total"
]

df = df[colunas]

# Renomeia colunas
df = df.rename(columns={
    "no_uf": "UF",
    "ano": "Ano",
    "va_icms_total": "ICMS",
    "va_outros_tributos_ipva": "IPVA",
    "va_outros_tributos_itcd": "ITCMD",
    "va_receita_tributaria_total": "Receita_Total"
})

# Agrupa por UF e ano, somando valores
df_agrupado = df.groupby(["UF", "Ano"], as_index=False).sum()

# Ordena
df_agrupado = df_agrupado.sort_values(by=["Ano", "UF"])

# Salva CSV final
df_agrupado.to_csv(caminho_saida, index=False, encoding="utf-8-sig")

print(f"✅ Arquivo salvo com sucesso: {caminho_saida}")

