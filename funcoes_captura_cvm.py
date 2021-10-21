import pandas as pd
import numpy as np
import math

def cria_df_novo(ano_mes,lista_fundos):
    print('Captura dados da CVM')
    dados = pd.read_csv("http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_" + ano_mes + ".csv", sep=";")
    
    print('Filtrando dados capturados')
    lista_df = []
    for i in lista_fundos:
        print('Selecionando na base dados do fundo ' + i[2])
        filter = dados.CNPJ_FUNDO == i[0] #cria o filtro a ser usado em dados
        fundo_df = dados[filter] #filtra o data frame principal
        fundo_df = fundo_df.assign(NOME_FUNDO = i[2]) #adiciona uma nova coluna
        lista_df.append(fundo_df)

    print('Ajustando dataframe')
    for i in range(0,len(lista_df)):
        lista_df[i] = lista_df[i].drop(columns=['TP_FUNDO','VL_TOTAL']) # Retira coluna TP_FUNDO
        lista_df[i] = lista_df[i].rename(columns={'DT_COMPTC': 'DATA', 
                                                  'VL_QUOTA': 'COTA',
                                                  'CAPTC_DIA':'CAPTACAO',
                                                  'RESG_DIA':'RESGATE',
                                                  'VL_PATRIM_LIQ':'PL',
                                                  'NR_COTST':'COTISTA'
                                                 }
                            ) #Altera o nome das colunas
        lista_df[i]["VARIACAO"] = -1 #Adiciona coluna variação
        lista_df[i] = lista_df[i][["CNPJ_FUNDO","NOME_FUNDO","DATA","COTA","VARIACAO","CAPTACAO","RESGATE","PL","COTISTA"]] # Reordena as colunas

    print('Concatenando os dataframe de cada fundo em um fundo')
    df_fundos = pd.concat(lista_df, ignore_index=True) #O ignore index limpa o index existente

    print('Ordenando dataframa')
    df_fundos = df_fundos.sort_values(['CNPJ_FUNDO','DATA'])
    return df_fundos

def junta_df(df_fundos_cvm, lista_fundos):
    print('Capturando dados do historico dos fundos')
    try:
        df_fundos_hist = pd.read_csv('Base_Historica_Fundos_CVM.csv')
    except:
        print('O arquivo de base não está presente')
        print('Iniciando a criação de arquivo base...')
        cria_df_historico(lista_fundos)
        

    print('Tratando dados da cvm para concatenação')
    df_fundos_hist = df_fundos_hist[df_fundos_hist.DATA < df_fundos_cvm.DATA.min()]

    print('Concatenando dados historicos com dados da cvm')
    df_fundos_total = pd.concat([df_fundos_cvm,df_fundos_hist]) #O ignore index limpa o index existente
    df_fundos_total = df_fundos_total.sort_values(['CNPJ_FUNDO','DATA'], ignore_index=True)
    return df_fundos_total

def ajuste_variacao(df_fundos_total):
    col_variacao = []
    for i in range(0,len(df_fundos_total)):
        if not np.isnan(df_fundos_total.iloc[i].VARIACAO):
            if df_fundos_total.iloc[i].VARIACAO == -1:
                col_variacao.append((df_fundos_total.iloc[i].COTA/df_fundos_total.iloc[i-1].COTA)-1) #calcula a variação
            else:
                col_variacao.append(df_fundos_total.iloc[i].VARIACAO)
        else:
            col_variacao.append(math.nan)

    df_fundos_total.VARIACAO = col_variacao #substitui a coluna de variação
    return df_fundos_total

def cria_df_historico(lista_fundos):
    # Criado para iniciar com o historico no data frame
    lista_df = []
    for i in lista_fundos:
        print("Fazendo captura do historico do fundo " + i[2])
        print("Lendo arquivo " + i[1])
        dados = pd.read_excel(i[1])
        dados = dados.drop(columns=['Unnamed: 9','Unnamed: 10','Unnamed: 11','Unnamed: 12','Unnamed: 13','Unnamed: 14']) #retira as colunas desnecessarias
        dados = dados.dropna(subset=['Fundo']) #exclui valores nulo

        dados['Código'] = i[0]
        dados = dados.rename(columns={'Data': 'DATA', 
                                      'Cota': 'COTA',
                                      'Captação':'CAPTACAO',
                                      'Resgate':'RESGATE',
                                      'Código':'CNPJ_FUNDO',
                                      'Cotistas':'COTISTA',
                                    'Variação':'VARIACAO',
                                     'Fundo':'NOME_FUNDO'})
        lista_df.append(dados)
    df = pd.concat(lista_df) #concatena os data_frame de cada fundo
    df.to_csv('Base_Historica_Fundos_CVM.csv', index=False) #Cria arquivo csv    