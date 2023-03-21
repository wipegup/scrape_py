import json
import utils
import csv_utils
import log_utils
import os
from time import sleep

#######################
## Modify as desired for different directory
run_no='t'
################

### Do not modify below this line
occur_req_interval=10

save_dir = f'./raw/run_{run_no}/'
head_dir = f'{save_dir}head/'
raw_dir = f'{save_dir}raw/'
out_tsv = f'{save_dir}full.tsv'
out_dl_tsv = f'{save_dir}full_trans.tsv'

# Create Directories for Run if necessary
for d in [head_dir, raw_dir]:
    if not os.path.exists(d):
        os.makedirs(d)

# Get head Information hf = headfiles
hf = {
    'full_coll': {'req': utils.get_coll},
    'full_occur': {'req': utils.get_occ},
    'occur_req_log': {'fn': f'{head_dir}occur_req_log.json'},
    'count':{},
    'time':{}
}

def rewrite_hf(head_file_dict, k):
    utils.write_pretty_json(head_file_dict[k]['fn'], head_file_dict[k]['body'])

for k in ['full_coll', 'full_occur']:
    pretty = k.replace('_', ' ')
    print(f"\nPulling {pretty} info --")
    hf[k]['fn'] = f'{head_dir}{k}.json'
    hf[k]['ct'], hf[k]['res'], hf[k]['body'] = utils.read_or_create_file( hf[k]['fn'], hf[k]['req'], pretty)
    print(f"{pretty.capitalize()} Count: {hf[k]['ct']:,}")

# Get Individual Collection Information / Create Requests Log
print(f"\nPulling individual collection/occurence counts (Up to {hf['full_coll']['ct']} API requests). Will take a several mins the first time --")

if os.path.exists(hf['occur_req_log']['fn']):
    hf['occur_req_log']['body'] = utils.read_json(hf['occur_req_log']['fn'])
else:
    hf['occur_req_log']['body'] = {}

hf['time']['coll_start'] = hf['time']['interval'] = utils.mark_time()
coll_ct_req_ct = 0
log_utils.start_time(hf, 'coll')

for coll_idx, coll_info in enumerate(hf['full_coll']['res']):
    coll_id = str(coll_info['collID'])

    # Get Coll/Occur Info if not in log
    if coll_id not in hf['occur_req_log']['body']:
        coll_ct_req_ct += 1
        ct, *_ = utils.get_occ(limit=0, coll_id=coll_id)
        hf['occur_req_log']['body'][coll_id] = {'done': False, 'last_offset': -300, 'occur_total': ct, 'tsv_records':0 }
        rewrite_hf(hf, 'occur_req_log')
    
    # Output if a % 20 request number for timing
    if coll_ct_req_ct > 0 and (coll_ct_req_ct) % 20 == 0:
        init_str = f"{coll_idx + 1} Collections pulled (incl {coll_ct_req_ct} API reqs)"
        hf['time']['interval'] = log_utils.interval_update(hf, 'coll', init_str)
        
init_str = f"Finished Pulling Coll/Occur counts (incl {coll_ct_req_ct} API reqs)"
log_utils.interval_update(hf, 'coll', init_str)

# Get Occurence Records
# Finding existing records
print(f"\nDetermining pulled occurrences")
if os.path.exists(out_tsv):
    print("TSV Found, Counting Records")
    tsv_coll_records = csv_utils.occ_per_coll_in_csv(out_tsv)
    for k in tsv_coll_records:
        hf['occur_req_log']['body'][k]['tsv_records'] = tsv_coll_records[k]
    hf['count']['tsv'] = sum(tsv_coll_records.values())
else:
    hf['count']['tsv'] = 0

json_files = [f for f in os.listdir(raw_dir)]
if len(json_files) > 1:
    print('Maybe Orphaned raw JSON files')
hf['count']['json'] = sum([utils.file_line_count(f'{raw_dir}{fn}') for fn in json_files])
hf['count']['start'] = hf['count']['json'] + hf['count']['tsv']
log_utils.have_need_req(hf)

hf['time']['occur_start'] = hf['time']['interval'] = utils.mark_time()
log_utils.start_time(hf, 'occur')

occur_request_ct=0
for coll_info in hf['full_coll']['res']:

    coll_id = str(coll_info['collID'])
    raw_fn = f"{raw_dir}{coll_id}.jsonl"

    if coll_id not in hf['occur_req_log']['body']:
        raise Exception(f"Collection with ID {coll_id} not previously found")

    if hf['occur_req_log']['body'][coll_id]['done']:
        utils.verify_coll_complete(hf['occur_req_log']['body'][coll_id], coll_id, raw_fn)
        continue

    if (json_lines := utils.file_line_count(raw_fn)) != (offset := hf['occur_req_log']['body'][coll_id]['last_offset']) + 300:
        print(f'WARN: Potentially incorrect starting offset actual(expected) {offset}({json_lines - 300})')

    new_req=True
    while new_req:
        # Add New Records
        offset += 300
        total_record_ct, results, _ = utils.get_occ(offset=offset, coll_id=coll_id)        
        utils.append_results(raw_fn, results)
        hf['occur_req_log']['body'][coll_id]['last_offset'] = offset

        # Determine if collection complete; Update Request Log
        new_req = offset + len(results) < total_record_ct
        if not new_req:
            utils.verify_coll_complete(hf['occur_req_log']['body'][coll_id], coll_id, raw_fn)
            print(f"Adding records to TSV; KEEPING Raw JSON; Updating TSV record count")
            csv_utils.add_json_to_raw_csv(raw_fn, out_tsv)
            csv_utils.add_json_to_dl_csv(raw_fn, out_dl_tsv)
            records_added = utils.file_line_count(raw_fn)
            hf['occur_req_log']['body'][coll_id]['tsv_records'] = records_added
            hf['count']['tsv'] += records_added
            # os.remove(raw_fn)
            hf['occur_req_log']['body'][coll_id]['done'] = True
        
        rewrite_hf(hf, 'occur_req_log')
    
        # Request Orchestration + Logging
        occur_request_ct += 1
        if occur_request_ct % occur_req_interval == 0:
            hf['count']['json'] = utils.file_line_count(raw_fn)
            hf['time']['interval'] = log_utils.occur_interval(hf, occur_request_ct)


## TODO:
# Check if CSV contains same records as all records
# If not re-download individual collection information.
# Print a happy message
# Deduplicate file
