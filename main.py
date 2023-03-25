import utils
import csv_utils
import log_utils
import os
import argparse

TEST_COL_IDS = ['100', '101', '109', '144', '45']
def download(head_dir, raw_dir, occur_log_interval, test_dl):
    # Create Directories for Download if necessary
    for d in [head_dir, raw_dir]:
        if not os.path.exists(d):
            os.makedirs(d)

    # Get head Information (hf = headfiles)
    hf = {
        'full_coll': utils.get_full_info('coll', utils.get_coll, head_dir),
        'full_occur': utils.get_full_info('occur', utils.get_occ, head_dir),
        'occur_req_log': {'fn': f'{head_dir}occur_req_log.json'},
        'count':{},
        'time':{}
    }

    # Get Individual Collection Information / Create Requests Log
    print(f"\nFinding occurence/collection counts (Up to {hf['full_coll']['ct']} API requests). Will take a several mins the first time --")
    if os.path.exists(hf['occur_req_log']['fn']):
        init_log = utils.read_json(hf['occur_req_log']['fn'])
    else:
        init_log = {}
    hf['occur_req_log']['body'] = init_log
    coll_ct_req_ct = 0
    
    hf = log_utils.start_time(hf, 'coll')

    for coll_idx, coll_info in enumerate(hf['full_coll']['res']):
        coll_id = str(coll_info['collID'])

        # Get Coll/Occur Info if not in log
        if coll_id not in hf['occur_req_log']['body']:
            if test_dl and coll_id not in TEST_COL_IDS:
                continue
            coll_ct_req_ct += 1
            ct, results, _ = utils.get_occ(limit=300, coll_id=coll_id)
            raw_fn = utils.json_fn(raw_dir, coll_id)
            utils.append_results(raw_fn, results)
            records = utils.file_line_count(raw_fn)
            hf['occur_req_log']['body'][coll_id] = {'done': ct == records, 'last_offset': 0, 'occur_total': ct, 'records':records, 'code': str(coll_info['institutionCode']) }
            utils.rewrite_hf(hf, 'occur_req_log')
        
        # Output if a % 20 request number for timing
        if coll_ct_req_ct > 0 and (coll_ct_req_ct) % 20 == 0:
            hf = log_utils.interval_update(hf, 'coll', f"{coll_idx + 1} Collections pulled (incl {coll_ct_req_ct} API reqs)")
            
    log_utils.interval_update(hf, 'coll', f"Finished finding occur/coll counts (incl {coll_ct_req_ct} API reqs)")

    # Get Occurence Records
    # Finding existing records
    print(f"\nDetermining pulled occurrences")

    json_files = [f for f in os.listdir(raw_dir)]
    hf['count'] = {fn.replace('.jsonl', ''): utils.file_line_count(f'{raw_dir}{fn}') for fn in json_files}
    for k in hf['count']:
        hf['occur_req_log']['body'][k]['records'] = hf['count'][k]
    hf['count']['start'] = sum(hf['count'].values())
    utils.rewrite_hf(hf, 'occur_req_log')

    if test_dl:
        hf['occur_req_log']['body'] = {k:v for k,v in hf['occur_req_log']['body'].items() if k in TEST_COL_IDS}
        utils.rewrite_hf(hf, 'occur_req_log')

    log_utils.have_need_req(hf)
    hf = log_utils.start_time(hf, 'occur')

    occur_request_ct=0
    for coll_id in hf['occur_req_log']['body']:

        raw_fn = utils.json_fn(raw_dir, coll_id)
        json_lines = utils.file_line_count(raw_fn)

        if hf['occur_req_log']['body'][coll_id]['done']:
            utils.verify_coll_complete(hf['occur_req_log']['body'][coll_id], coll_id, json_lines)
            continue

        if json_lines != (offset := hf['occur_req_log']['body'][coll_id]['last_offset']) + 300:
            print(f'WARN: Potentially incorrect starting offset actual(expected) {offset}({json_lines - 300})')

        new_req=True
        while new_req:
            # Add New Records
            offset += 300
            total_record_ct, results, _ = utils.get_occ(offset=offset, limit=300, coll_id=coll_id)        
            utils.append_results(raw_fn, results)
            json_lines = offset + len(results)
            log_update = {'last_offset': offset, 'records': json_lines}
            hf['count'][coll_id] = json_lines

            # Determine if collection complete; Update Request Log
            new_req = json_lines < total_record_ct
            if not new_req:
                utils.verify_coll_complete(hf['occur_req_log']['body'][coll_id], coll_id, json_lines)
                log_update['done'] = True
            
            hf['occur_req_log']['body'][coll_id].update(log_update)
            utils.rewrite_hf(hf, 'occur_req_log')
        
            # Request Orchestration + Logging
            occur_request_ct += 1
            if occur_request_ct % occur_log_interval == 0:
                hf = log_utils.occur_interval(hf, occur_request_ct)
    return True

def transform(raw_dir, head_dir, clean_dir, tsv_dir, out_dl_tsv, no_dedup):
    if not os.path.exists(tsv_dir):
        os.makedirs(tsv_dir)
    raw_tsv = f'{tsv_dir}full.tsv'
    hf = {'req_log': utils.read_json(f'{head_dir}occur_req_log.json'), 'time':{}}

    if not os.path.exists(clean_dir):
        print('JSON files not yet cleaned of whitespace, looking for raw files')
        if not os.path.exists(raw_dir):
            raise Exception("Raw JSON not Found -- Need Source Material")
        os.makedirs(clean_dir)

    # Clean Dir Exists, Raw may or may not exist
    clean_json_files = [f for f in os.listdir(clean_dir) if f.endswith('.jsonl')]

    if os.path.exists(raw_dir):
        raw_json_files = [f for f in os.listdir(raw_dir) if f.endswith('.jsonl')]
        files_to_clean = [f for f in raw_json_files if f not in clean_json_files]

        if len(files_to_clean) > 0:
            print(f'Have {len(files_to_clean)} files to clean')
            hf = log_utils.start_time(hf, 'clean_ws')
            for fn in files_to_clean:
                raw_fn = f'{raw_dir}{fn}'
                clean_fn = f'{clean_dir}{fn}'
                csv_utils.clean_json_file_whitespace(raw_fn, clean_fn)
                clean_json_files.append(fn)
                if utils.file_line_count(raw_fn) != utils.file_line_count(clean_fn):
                    print(f'WARN Line Mismatch after cleaning{fn}')
            
            hf = log_utils.interval_update(hf, 'clean_ws', 'Done Cleaning WS')         
    else:
        print('Raw JSON not found. Working only from Cleaned records')

    print('\nDeduplicating Cleaned JSON')

    hf = log_utils.start_time(hf, 'dedup_json')

    for fn in clean_json_files:
        csv_utils.deduplicate_json(f'{clean_dir}{fn}')
    hf = log_utils.interval_update(hf, 'dedup_json', 'Done Deduplicating Json')

    print('\nDeleting TSV files')
    for fn in [raw_tsv, out_dl_tsv]:
        with open(fn, 'w') as f:
            pass
        os.remove(fn)

    hf = log_utils.start_time(hf, 'write_tsv')
    for fn in clean_json_files:
        coll_id = utils.fn_to_coll_id(fn)
        code = hf['req_log'][coll_id]['code']
        clean_fn = f'{clean_dir}{fn}'
        csv_utils.add_json_to_raw_csv(clean_fn, raw_tsv)
        csv_utils.add_json_to_dl_csv(clean_fn, out_dl_tsv, institutionCode=code)

    hf = log_utils.interval_update(hf, 'write_tsv', 'Done Writing to TSV')

def main(run_no, output_fn, no_transform, no_download, test_dl, no_dedup, occur_log_interval):
    
    occur_log_interval=10

    save_dir = f'./downloads/run_{run_no}/'
    head_dir = f'{save_dir}head/'
    raw_dir = f'{save_dir}raw/'
    clean_dir = f'{save_dir}clean_ws/'
    tsv_dir = f'{save_dir}tsvs/'
    out_dl_tsv = f'{tsv_dir}{output_fn}.tsv'

    if not no_download:
        download(head_dir, raw_dir, occur_log_interval, test_dl)
    
    if not no_transform:
        transform(raw_dir, head_dir, clean_dir, tsv_dir, out_dl_tsv, no_dedup)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', '--run-no', required=True)
    parser.add_argument('--no-transform', action='store_true')
    parser.add_argument('--no-download', action='store_true')
    parser.add_argument('--test-dl', action='store_true')
    parser.add_argument('--no-dedup', action='store_true')
    parser.add_argument('--output-fn', default='full_trans')
    parser.add_argument('--occur-log-interval', default=10, type=int)

    args = vars(parser.parse_args())

    main(**args )

## TODO:
# Deduplicate file
# Create Util to automatically delete JSON from script