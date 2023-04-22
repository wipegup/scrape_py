import pandas as pd
import dirs
import os
import datetime
import log_utils
import csv_utils
import numpy as np
import json
import utils

def set_compare(new_set, old_set):
    return {'fresh': new_set - old_set, 'stale': old_set - new_set, 'common': old_set & new_set}

if __name__== "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--recent', action='store_true')
    parser.add_argument('--days', default=30, type=int)
    parser.add_argument('-r', '--run-no', required=True)
    parser.add_argument('--diff', action='store_true')
    parser.add_argument('--prev-run')
    parser.add_argument('--coll', default='all')
    parser.add_argument('--ne-match')

    args = parser.parse_args()
    hf = log_utils.create_default_timing_dict()

    if args.diff:
        if args.prev_run is None:
            raise Exception('Need to specify --prev-run for diff')

        col_order = ['description', 'occid', 'collid', 'column', 'prev', 'curr', 'locality', 'municipality', 'country', 'stateProvince', 'country']
        metadata = ('occid', 'locality', 'municipality', 'country', 'stateProvince', 'country')
        diff_dir = dirs.tsv_dir(args.run_no, 'diffs/')
        if not os.path.exists(diff_dir):
            os.makedirs(diff_dir)
        diff_fn = f'{diff_dir}{args.prev_run}.tsv'

        hf = log_utils.start_time(hf, 'compare_cells')

        hf['dir']= {'old': dirs.clean_dir(args.prev_run), 'new': dirs.clean_dir(args.run_no)}
        diffs = []

        if args.coll == 'all':
            hf['files'] = {k: dirs.list_files(v, '.jsonl', utils.is_file_not_empty) for k,v in hf['dir'].items()}

            hf['files'] = {**hf['files'], **set_compare(hf['files']['new'], hf['files']['old'])}
            hf['collids'] = [f.replace('.jsonl', '') for f in hf['files']['common']]
            
            for desc, path, files in zip(
                ['added occid', 'dropped occid'],
                [hf['dir']['new'], hf['dir']['old']],
                [hf['files']['fresh'], hf['files']['stale']]):
                for  f in files:
                    collid = f.replace('.jsonl', '')
                    with open(dirs.conj(path,f), 'r') as f:
                        for line in f:
                            j = json.loads(line)
                            diffs.append(
                                {'description': desc, 'collid': collid, 'column': 'collid', 'prev': '', 'curr': '',
                                 **{k: j[k] for k in metadata}})  
        else:
            hf['collids'] = [args.coll]
        hf['diffs'] = pd.DataFrame(diffs)
        
        for idx, collid in enumerate(hf['collids']):
            
            j_fn_suffix = f"{collid}.jsonl"
            t_fn_suffix = f"tsvs/{collid}.tsv"
            # Check to see if TSVs made:
            for r in [args.prev_run, args.run_no]:
                j_fn = dirs.clean_dir(r, j_fn_suffix)
                t_fn = dirs.clean_dir(r, t_fn_suffix)
                if not os.path.exists(dirs.clean_dir(r, 'tsvs/')):
                    os.makedirs(dirs.clean_dir(r, 'tsvs/'))
                if not os.path.exists(t_fn):
                    print('need to create tsvs', j_fn, )
                    if os.path.exists(j_fn):
                        csv_utils.add_json_to_raw_csv(j_fn, t_fn)
            hf['df'] = {
                'old': pd.read_csv(dirs.clean_dir(args.prev_run, t_fn_suffix), sep='\t', na_filter=False), 
                'new': pd.read_csv(dirs.clean_dir(args.run_no, t_fn_suffix), sep='\t', na_filter=False)
            }
            hf['occids'] = {k: set(v['occid']) for k,v in hf['df'].items()}
            hf['occids'] = {**hf['occids'], **set_compare(hf['occids']['new'], hf['occids']['old'])}

            for desc, df, occids in zip(
                ['added occid', 'dropped occid'],
                [hf['df']['new'], hf['df']['old']],
                [hf['occids']['fresh'], hf['occids']['stale']]):
                changes = []

                for occid in occids:
                    d = df[df['occid'] == occid].iloc[0]
                    changes.append(
                        {'description': desc, 'collid': collid, 'column': 'collid', 'prev': '', 'curr': '',
                        **{k: j[k] for k in metadata}})
                hf['diffs'] = pd.concat([hf['diffs'], pd.DataFrame(changes)])
            hf['df'] = {k:v[v['occid'].isin(hf['occids']['common'])].sort_values(['occid']).reset_index(drop=True) for k,v in hf['df'].items()}
                
            compare_columns = [c for c in hf['df']['new'].columns if c not in ['dateLastModified']]
            for col in compare_columns:
                d = ~(hf['df']['old'][col] == hf['df']['new'][col]) | ((hf['df']['old'][col] != hf['df']['old'][col]) & (hf['df']['new'][col] != hf['df']['new'][col]))
                changed_recs = hf['df']['new'][d]
                if hf['df']['new'][d].shape[0] > 0:
                    to_add = pd.DataFrame(
                        {'description': 'changed value', 'collid': collid, 'column': col, 'prev':hf['df']['old'][d][col], 'curr': hf['df']['new'][d][col], 
                        **{k: hf['df']['new'][d][k] for k in metadata}})
                    hf['diffs'] = pd.concat([hf['diffs'], to_add])    

            hf = log_utils.interval_update(hf, 'compare_cells', f"Done comparing cells for {collid}; {len(hf['collids']) - idx -1} colls left")    
        hf = log_utils.interval_update(hf, 'compare_cells', f'Done comparing cells writing diff tsv to {diff_fn}')
        hf['diffs'].reindex(col_order, axis='columns')
        hf['diffs'].to_csv(diff_fn, sep='\t', index=False)    

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