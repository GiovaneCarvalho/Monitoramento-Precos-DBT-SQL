import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


# Selecionando os commodities que quero buscar

commodities = ['CL=F', #Petróleo
     'GC=F', #Ouro
     'SI=F'] #Prata

def buscar_dados_commodities(commoditie, periodo = '1mo', intervalo = '1d'):
     ticker = yf.Ticker(commoditie)
     dados = ticker.history(period = periodo, interval = intervalo)[['Close']]
     dados['ticker'] = commoditie

     return dados

def buscar_todos_dados_commodities(commodities = commodities):
    todos_dados = []
    for commoditie in commodities:
          print(commodities)
          dados = buscar_dados_commodities(commoditie=commoditie)
          todos_dados.append(dados)

    return pd.concat(todos_dados)          

if __name__ == '__main__':
    dados_concatenados = buscar_todos_dados_commodities(commodities=commodities)
    print(dados_concatenados)