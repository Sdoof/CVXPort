
import pystore as ps
from datetime import datetime
import pandas as pd
import time
import psycopg2 as pg
from pytz import timezone

con = pg.connect(database='bar_data', user='postgres', password='key', host='127.0.0.1', port=5432)
cur = con.cursor()
cur.execute("select * from tick;")
res = cur.fetchall()
print(res)
# res = [item[0] for item in cur.fetchall()]
con.close()
print(res[0][1].astimezone(timezone('utc')))