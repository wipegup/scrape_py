import pandas as pd
import dirs
import os
import datetime
if __name__== "__main__":
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--recent', action='store_true')
    parser.add_argument('--days', default=30, type=int)
    parser.add_argument('-r', '--run-no', required=True)
    # parser.add_argument('--diff')

    args = parser.parse_args()

    if args.recent:
        tsv_dir = dirs.tsv_dir(args.run_no)

        tsv_fns = [f for f in os.listdir(tsv_dir) if f.endswith('.tsv') and not f.endswith('days.tsv')]

        tsv_fn = tsv_fns[0]

        for tsv_fn in tsv_fns:
            new_fn = f"{tsv_dir}{tsv_fn.replace('.tsv', '')}_last{args.days}days.tsv"
            df = pd.read_csv(f'{tsv_dir}{tsv_fn}', sep='\t')
            
            mod_col = [c for c in df.columns if ('mod' in c or 'Mod' in c) and df[c].dtype == 'object' and df[c].isnull().sum() / df.shape[0]< 0.5 ][0]
            
            df[mod_col] = pd.to_datetime(df[mod_col], format='%Y-%m-%d %H:%M:%S')

            prev_date = datetime.datetime.now() - datetime.timedelta(days=args.days)
            recent_df = df[df[mod_col]> prev_date]
            recent_df.to_csv(new_fn, sep='\t', index=False)