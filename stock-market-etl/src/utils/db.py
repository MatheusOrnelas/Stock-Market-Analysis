import pandas as pd
from sqlalchemy import create_engine, Integer, Float, String, DateTime, text, Double
from sqlalchemy.types import DECIMAL
from datetime import datetime
import unicodedata
import re
import os
import numpy as np
from src.config.logging_config import get_logger

logger = get_logger(__name__)

def create_database_if_not_exists(base_uri, db_name):
    """
    Conecta ao servidor MySQL (sem selecionar banco) e cria o banco de dados se não existir.
    """
    try:
        # Conecta sem especificar o banco de dados para poder criar
        engine = create_engine(base_uri)
        with engine.connect() as conn:
            # Commit automático é necessário para comandos DDL como CREATE DATABASE
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            logger.info(f"Banco de dados '{db_name}' verificado/criado com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao tentar criar banco de dados '{db_name}': {e}")
        raise

def get_db_engine():
    # Parâmetros de conexão
    DB_USER = 'root'
    DB_PASS = 'DABC212e61!!!'
    DB_HOST = 'localhost'
    DB_NAME = 'dw_acoes_silver'
    
    # URI base para conectar ao servidor (sem o banco)
    BASE_URI = f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}'
    
    # URI completa para conectar ao banco
    DATABASE_URI = f'{BASE_URI}/{DB_NAME}'
    
    try:
        # Tenta criar o banco antes de retornar a engine conectada a ele
        create_database_if_not_exists(BASE_URI, DB_NAME)
        
        engine = create_engine(DATABASE_URI)
        return engine
    except Exception as e:
        logger.error(f"Erro ao criar engine do banco de dados: {e}")
        raise

def clean_column_name(column_name):
    # Remover acentos
    column_name = unicodedata.normalize('NFKD', column_name).encode('ASCII', 'ignore').decode('ASCII')
    # Substituir espaços por underscores
    column_name = re.sub(r'\s+', '_', column_name)
    # Remover caracteres especiais
    column_name = re.sub(r'[^a-zA-Z0-9_]', '', column_name)
    # Converter para minúsculas
    column_name = column_name.lower()
    return column_name

def insert_dataframe(df, table_name, engine=None, *, if_exists: str = "replace", drop_existing: bool = True):
    if engine is None:
        engine = get_db_engine()

    try:
        initial_count = len(df)
        # Remover duplicados
        df = df.drop_duplicates()
        if len(df) < initial_count:
            logger.debug(f"Removidos {initial_count - len(df)} registros duplicados de {table_name}")

        # Limpeza de valores infinitos ou NaN que podem quebrar o banco
        df = df.replace([np.inf, -np.inf], np.nan)
        # Opcional: preencher NaN com None (NULL no SQL) ou outro valor, dependendo da coluna
        # df = df.where(pd.notnull(df), None)

        # Padronizar nomes das colunas
        df.columns = [clean_column_name(col) for col in df.columns]

        # Adicionar colunas técnicas:
        # - se o dado já tem "timestamp" (ex: snapshot), não sobrescrever; usar "ingested_at"
        df = df.copy()
        df["id"] = range(1, len(df) + 1)
        if "timestamp" in df.columns:
            df["ingested_at"] = datetime.now()
        else:
            df["timestamp"] = datetime.now()

        # Apagar tabela existente (quando apropriado)
        if drop_existing and if_exists == "replace":
            logger.info(f"Recriando tabela {table_name}...")
            with engine.connect() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

        # Mapear os tipos de dados para as colunas
        dtype_mapping = {}
        for col in df.columns:
            if df[col].dtype == 'int64':
                dtype_mapping[col] = Integer()
            elif df[col].dtype == 'float64':
                # Usar Double ou Float em vez de DECIMAL fixo para evitar "Out of range" em valores com muitos decimais
                dtype_mapping[col] = Double() 
            elif df[col].dtype == 'datetime64[ns]':
                dtype_mapping[col] = DateTime()
            else:
                # Definir um comprimento padrão para VARCHAR
                # Converte para string antes de verificar o tamanho para evitar erro se houver tipos mistos
                max_length = max(df[col].apply(lambda x: len(str(x)) if pd.notnull(x) else 0), default=255)
                # Garante um mínimo de 255
                final_length = max(max_length, 255)
                # Se for muito grande, usa TEXT (embora String(N) no SQLAlchemy mapeie para VARCHAR ou TEXT dependendo do tamanho)
                if final_length > 16000:
                    # fallback: mantém como String grande; MySQL pode virar TEXT conforme o driver
                    dtype_mapping[col] = String(length=final_length)
                else:
                    dtype_mapping[col] = String(length=final_length)

        # Inserir DataFrame na tabela, recriando-a
        # chunksize ajuda em grandes volumes
        df.to_sql(
            table_name,
            con=engine,
            index=False,
            if_exists=if_exists,
            dtype=dtype_mapping,
            chunksize=5000,
        )
        logger.info(f"Tabela {table_name} atualizada com sucesso ({len(df)} registros inseridos, if_exists={if_exists}).")
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela {table_name}: {e}", exc_info=True)
        raise
