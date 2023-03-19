import os
import utils
import csv_utils

save_dir = 'raw/encoding_test/'
json_fn = f'{save_dir}/2.jsonl'
out_tsv = f'{save_dir}full.tsv'
out_dl_tsv = f'{save_dir}full_trans.tsv'

if not os.path.exists(save_dir):
    os.makedirs(save_dir)

_, occs, _ = utils.get_occ(coll_id=2)

utils.append_results(json_fn, occs)

csv_utils.add_json_to_raw_csv(json_fn, out_tsv)
csv_utils.add_json_to_dl_csv(json_fn, out_dl_tsv)
