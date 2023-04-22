import pandas as pd
import dirs
import os
import datetime
import log_utils
import csv_utils
import numpy as np
import json
import utils
import difflib

if __name__== "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--ne-match')
    parser.add_argument('--column-name', default='stateProvince')
    args = parser.parse_args()
    hf = log_utils.create_default_timing_dict()

    if args.ne_match:
        hf = log_utils.start_time(hf, 'ne_match')

        if not os.path.exists(args.ne_match):
            raise Exception('No File Found')
        state_names = ['connecticut', 'vermont', 'new hampshire', 'rhode island', 'massachusetts']
        df = pd.read_csv(args.ne_match, sep='\t', na_filter=False)
        *root, ext = args.ne_match.split('.')
        root = '.'.join(root)
        exact_fn = f"{root}_NE_exact.{ext}"
        fuzzy_fn = f"{root}_NE_fuzzy.{ext}"
        rest_fn = f"{root}_NE_no_match.{ext}"

        loc_col = args.column_name
        df_exact = df[df[loc_col].str.lower().isin(state_names)]
        df_rest = df.drop(df_exact.index, axis='rows')
        hf = log_utils.interval_update(hf, 'ne_match', 'Done Finding Exact Matches, starting fuzzy matches')

        df_fuzzy = pd.DataFrame()
        def fuzzy_match(real):
            return lambda x: bool(difflib.get_close_matches(real, [x]))
        for n in state_names:
            matched = df_rest[df_rest[loc_col].apply(fuzzy_match(n))]
            df_fuzzy = pd.concat([df_fuzzy, matched])
            df_rest.drop(matched.index, axis='rows', inplace=True)
            hf = log_utils.interval_update(hf, 'ne_match', f"Done finding fuzzy matches for {n}")
        
        hf = log_utils.interval_update(hf, 'ne_match', 'Done finding all fuzzy matches, writing files')
        
        df_exact.to_csv(exact_fn, sep='\t', index=False)  
        df_fuzzy.to_csv(fuzzy_fn, sep='\t', index=False)  
        df_rest.to_csv(rest_fn, sep='\t', index=False)  