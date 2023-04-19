import pandas as pd
import dirs
import os
import datetime
import log_utils
import csv_utils
import numpy as np
import json

if __name__== "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--recent', action='store_true')
    parser.add_argument('--days', default=30, type=int)
    parser.add_argument('-r', '--run-no', required=True)
    parser.add_argument('--diff', action='store_true')
    parser.add_argument('--prev-run')
    parser.add_argument('--coll', default='all')


    args = parser.parse_args()
    hf = log_utils.create_default_timing_dict()

    if args.diff:
        if args.prev_run is None:
            raise Exception('Need to specify --prev-run for diff')

        diffs = []
        diff_fn = dirs.tsv_dir(args.run_no, f'diffs/{args.prev_run}.tsv')
        old_dir = dirs.clean_dir(args.prev_run)
        new_dir = dirs.clean_dir(args.run_no)
        if args.coll == 'all':
            new_files = [f for f in list(os.listdir(new_dir)) if f.endswith('.jsonl')]
            old_files = [f for f in list(os.listdir(old_dir)) if f.endswith('.jsonl')]
            fresh_files = [f for f in new_files if f not in old_files]
            stale_files = [f for f in old_files if f not in new_files]
            common_files = [f for f in new_files if f in old_files]
            collids = [f.replace('.jsonl', '') for f in common_files]

            for desc, path, files in zip(['added occid', 'dropped occid'], [new_dir, old_dir], [fresh_files, stale_files]):
                for  f in files:
                    collid = f.replace('.jsonl', '')
                    with open(f'{path}{f}', 'r') as f:
                        for line in f:
        
                
                            diffs.append({'description': desc, 'occid': json.loads(line)['occid'], 'collid': collid, 'prev': '', 'curr': '' })  
        else:
            collids = [args.coll]

        hf = log_utils.start_time(hf, 'compare_cells')
        
        for idx, collid in enumerate(collids):
                
            j_fn = f"{collid}.jsonl"
            t_fn = f"tsvs/{collid}.tsv"
            # Check to see if TSVs made:
            for r in [args.prev_run, args.run_no]:
                if not os.path.exists(dirs.clean_dir(r, 'tsvs/')):
                    os.makedirs(dirs.clean_dir(r, 'tsvs/'))
                if not os.path.exists(dirs.clean_dir(r, t_fn)):
                    if os.path.exists(dirs.clean_dir(r, j_fn)):
                        csv_utils.add_json_to_raw_csv(dirs.clean_dir(r, j_fn), dirs.clean_dir(r, t_fn))

            old_df = pd.read_csv(dirs.clean_dir(args.prev_run, t_fn), sep='\t', na_filter=False)                
            new_df = pd.read_csv(dirs.clean_dir(args.run_no, t_fn), sep='\t', na_filter=False)
            old_occids = list(old_df['occid'])
            new_occids = list(new_df['occid'])
            fresh_occids = [i for i in new_occids if i not in old_occids]
            stale_occids = [i for i in old_occids if i not in new_occids]
            common_occids = [i for i in old_occids if i in new_occids]
            new_df = new_df[new_df['occid'].isin(common_occids)].sort_values(['occid']).reset_index(drop=True)
            old_df = old_df[old_df['occid'].isin(common_occids)].sort_values(['occid']).reset_index(drop=True)

            for desc, occids in zip(['added occid', 'dropped occid'],   [fresh_occids, stale_occids]):
                for occid in occids:
                    diffs.append({'description': desc, 'occid': occid, 'collid': collid, 'prev': '', 'curr': '' })
            
            diffs = pd.DataFrame(diffs)
                
            # Need to reorder and reindex both DFs just in case

            for col in new_df.columns:
                # print(np.where(old_df[col] == new_df[col] | (old_df[col] != old_df[col] & new_df[col] != new_df[col])), [new_df[col], old_df[col], col], np.nan)
                d = ~(old_df[col] == new_df[col]) | ((old_df[col] != old_df[col]) & (new_df[col] != new_df[col]))
                changed_recs = new_df[d]
                if new_df[d].shape[0] > 0:
                    to_add = pd.DataFrame({'description': 'changed value', 'occid':new_df[d]['occid'], 'collid': collid, 'prev':old_df[d][col], 'curr': new_df[d][col]})
                    diffs = pd.concat([diffs, to_add])    
                    print(diffs)

            hf = log_utils.interval_update(hf, 'compare_cells', f'Done comparing cells for {collid}, {len(collids) - idx -1} colls left')    
        hf = log_utils.interval_update(hf, 'compare_cells', f'Done comparing cells writing diff tsv to {diff_fn}')

        diffs.to_csv(diff_fn, sep='\t', index=False)    
    if args.recent:
        tsv_dir = dirs.tsv_dir(args.run_no)

        tsv_fns = [f for f in os.listdir(tsv_dir) if f.endswith('.tsv') and not f.endswith('days.tsv')]

        tsv_fn = tsv_fns[0]
        hf = log_utils.start_time(hf, 'recent_tsv')
        for tsv_fn in tsv_fns:
            new_fn = f"{tsv_dir}{tsv_fn.replace('.tsv', '')}_last{args.days}days.tsv"
            df = pd.read_csv(f'{tsv_dir}{tsv_fn}', sep='\t')
            
            mod_col = [c for c in df.columns if ('mod' in c or 'Mod' in c) and df[c].dtype == 'object' and df[c].isnull().sum() / df.shape[0]< 0.5 ][0]
            
            df[mod_col] = pd.to_datetime(df[mod_col], format='%Y-%m-%d %H:%M:%S')

            prev_date = datetime.datetime.now() - datetime.timedelta(days=args.days)
            recent_df = df[df[mod_col]> prev_date]
            recent_df.to_csv(new_fn, sep='\t', index=False)
        hf = log_utils.interval_update(hf, 'recent_tsv', 'Done finding recent records')