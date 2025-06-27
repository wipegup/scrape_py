import pandas as pd
import sqlite3
import os
from pathlib import Path
def load_to_db(out_dl_tsv, raw_dir):
    df=pd.read_csv(out_dl_tsv, sep="\t")
    db_name = os.path.basename(os.path.dirname(os.path.dirname(raw_dir)))
    db_path = f"C:/Lichen/SQL/{db_name}.db"
    print(f'Attempting to write db to {db_path}')
    conn=sqlite3.connect(db_path)
    df.to_sql("full",conn,index=False,if_exists="replace")
    print("Attempting to add in NE dataset")
    ne_path = Path("c:/Lichen/datastor/NE/MA/sb/NEdf")
    if ne_path.exists():
        ne_df=pd.read_csv(ne_path,sep="|")
        ne_df.to_sql("New_England",conn,index=False,if_exists="replace")
    
    conn.close()
