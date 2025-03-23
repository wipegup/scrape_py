import pandas as pd
import sqlite3
import os

def load_to_db(out_dl_tsv, raw_dir):
    df=pd.read_csv(out_dl_tsv, sep="\t")
    db_name = os.path.basename(os.path.dirname(os.path.dirname(raw_dir)))
    db_path = f"C:/Lichen/SQL/{db_name}.db"
    conn=sqlite3.connect(db_path)
    df.to_sql("full",conn,index=False,if_exists="replace")
    conn.close()

# import dirs

# run_no = 76
# raw_dir = dirs.raw_dir(run_no)
# output_fn = 'full_trans'
# tsv_dir = dirs.tsv_dir(run_no)
# out_dl_tsv = f'{tsv_dir}{output_fn}.tsv'

# load_to_db(out_dl_tsv, raw_dir)