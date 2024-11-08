import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOSTNAME')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_DATABASE')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

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


def salvar_postgres(df, schema = 'public'):
     df.to_sql('commodities', engine, if_exists = 'replace', index = True, index_label = 'Date', schema = schema)

if __name__ == '__main__':
    dados_concatenados = buscar_todos_dados_commodities(commodities=commodities)
    salvar_postgres(dados_concatenados )
    print(dados_concatenados)