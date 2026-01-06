from sqlalchemy import create_engine
import os

def get_db_engine():
    # DATABASE_URI = os.getenv('DATABASE_URI', 'mysql+pymysql://root:DABC212e61!!!@localhost/dw_acoes_silver')
    DATABASE_URI = 'mysql+pymysql://root:DABC212e61!!!@localhost/dw_acoes_silver'
    engine = create_engine(DATABASE_URI)
    return engine

