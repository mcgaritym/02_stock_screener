# import config settings and Setup class
import pandas as pd
import numpy as np
import re
import time
from sqlalchemy import create_engine
import pymysql
import config_cloud
from setup_cloud import Setup


# credentials
user="admin"
pwd="Nalgene09!"
host="stocks-database.cj4j9csypvlc.us-east-2.rds.amazonaws.com"
port = int(3306)
db = 'stocks-database'

engine = create_engine(
    f"mysql+mysqlconnector://{user}:{pwd}@{host}:{port}/{db}",
    echo=False)

conn = engine.connect()

df = conn.execute("""SELECT * FROM stocks-database;""", conn=engine)

engine.close()

