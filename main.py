import pandas as pd
import numpy as np
import math
from funcoes_captura_cvm import *

ano_mes = "202110"
lista_fundos = [
("36.178.569/0001-99","cotas fundos bolsa america.xlsx","BB ACOES BOLSA AMERICANA FUNDO DE INVESTIMENTO EM ACOES",'45699'),
("08.973.951/0001-59","cotas fundos siderurgia.xlsx","BB ACOES SIDERURGIA FUNDO DE INVESTIMENTO EM COTAS DE FUNDOS DE INVESTIMENTO",'19704'),
("31.963.249/0001-26","cotas do fundo de renda fixa.xlsx","BB RENDA FIXA LONGO PRAZO CREDITO PRIVADO FUNDO DE INVESTIMENTO EM COTAS DE FI",'40977')
]

print("Capturando dados da cvm para " + ano_mes)
df_fundos_cvm = cria_df_novo(ano_mes,lista_fundos)

## Criar uma função que captura os dados da base e deixa comentado
## a função que puxa os dados do historico

print('Juntando dados da cvm com dados historicos(local)')
df_fundos_total = junta_df(df_fundos_cvm, lista_fundos)

print('Ajustando variações')
df_fundos_total = ajuste_variacao(df_fundos_total)

print('Ajustando casas deciamais')
df_fundos_total.COTA = round(df_fundos_total.COTA,6)
df_fundos_total.VARIACAO = round(df_fundos_total.VARIACAO,6)
df_fundos_total.CAPTACAO = round(df_fundos_total.CAPTACAO,2)
df_fundos_total.RESGATE = round(df_fundos_total.RESGATE,2)
df_fundos_total.PL = round(df_fundos_total.PL,2)

print('Exportando dataframe para arquivo csv')
df_fundos_total.to_csv('Base_Historica_Fundos_CVM.csv', index=False)

print('Fim da captura dos dados de fundos na CVM')