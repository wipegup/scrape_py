import requests as req
import os
import json
from datetime import datetime
from request_manager import RequestManager
import log_utils
import re

def api_root(target):
    return f"{portal_root(target)}/api/v2"

def portal_root(target):
    return  f"https://{target}portal.org/portal"

## Timing 
TIME_FMT = "%H:%M:%S"
def mark_time():
    return datetime.now()

def pretty_time(t):
    return t.strftime(TIME_FMT)

## Endpoints
def coll_endpoint (target, coll_id=''):
    return f'{api_root(target)}/collection/{coll_id}'

def occ_endpoint(target):
    return f'{api_root(target)}/occurrence/search'

## Requests
RM = RequestManager()
def res_body(res):
    return res.json()

def body_count(body):
    return body['count']

def body_results(body):
    return body['results']

def get_paginated (endpoint, limit=300, offset=0, extra={}, msg=None, silent=True):
    params = {'limit': limit, 'offset': offset, **extra}
    msg = msg if msg else endpoint
    res = RM.make_request(req.get, endpoint, params=params, timeout=10)

    return res_success(res, endpoint, silent)

def res_success (res, msg, silent):
    if (res.status_code != 200):
        raise Exception(f'{msg} {res.status_code} error')
    else:
        if not silent:
            print(f"Got {msg}")
        body = res_body(res)
        return (body_count(body), body_results(body), body)

def get_occ(target, offset=0, limit=0, coll_id=None):
    extra = {'collid': coll_id} if coll_id else {}
    msg = f"Occur- CollID: {coll_id}; Offset: {offset}; Limit: {limit}"
    return get_paginated(occ_endpoint(target), offset=offset, limit=limit, extra=extra)

def get_coll(target):
    msg = "Collection Request"
    return get_paginated(coll_endpoint(target), msg=msg)

## Unused Generator function
# def get_all_coll_occ(coll_id, starting_offset=None):
#     new_req=True
#     offset = -300 if starting_offset is None else starting_offset
#     while new_req:
#         offset += 300
#         total_record_ct, results, body = get_occ(offset=offset, coll_id=coll_id)
#         yield (offset, total_record_ct, results, body)
#         new_req = offset + len(results) < total_record_ct

def get_full_info(n, f, head_dir, **fn_params):
    to_ret={}
    k = f'full_{n}'
    pretty = k.replace('_', ' ')
    print(f"\nFinding {pretty} info --")
    to_ret['fn'] = f'{head_dir}{k}.json'
    to_ret['ct'], to_ret['res'], to_ret['body'] = read_or_create_file( to_ret['fn'], f, pretty, **fn_params)
    print(f"{pretty.capitalize()} Count: {to_ret['ct']:,}")
    return to_ret

## File Management      
def read_json(fn):
    with open(fn, 'r') as f:
        return json.loads(f.read())

def json_fn(raw_dir, coll_id):
    return f"{raw_dir}{coll_id}.jsonl"

def fn_to_coll_id(fn):
    return fn.split('/')[-1].replace('.jsonl', '')
    
def write_pretty_json(fn, d):
    with open(fn, 'w') as f:
        f.write('{\n')
        for idx, k in enumerate(d):
            if idx != 0:
                f.write(',\n')
            f.write(f'"{k}" : ')
            f.write(f'{json.dumps(d[k])}')
        f.write('\n}')

def append_results(fn, lst):
    with open(fn, 'a', encoding='UTF-8') as f:
        for i in lst:
            f.write(f'{json.dumps(i)}\n')

def file_line_count(fn):
    if os.path.exists(fn):
        line_ct = -1 # If file is empty
        with open(fn, 'r') as f:
            for line_ct, _ in enumerate(f):
                pass
        return line_ct + 1
    else:
        return 0

def is_file_not_empty(fn):
    return file_line_count(fn) > 0

def rewrite_hf(head_file_dict, k):
    write_pretty_json(head_file_dict[k]['fn'], head_file_dict[k]['body'])

## Other
def verify_coll_complete(req_log, coll_id, json_total):
    if json_total == req_log['occur_total']:
        print(f"All occurs accounted for with collection {coll_id}")
        return True
    else:
        print(f"WARN: collection {coll_id} file record mismatch -- actual(expected): {json_total}({req_log['occur_total']})")
        return False

def read_or_create_file(fn, func, msg, **fn_params):
    if os.path.exists(fn):
        body = read_json(fn)
        ct = body_count(body)
        res = body_results(body)
    else:
        print(f'Making {msg} request')
        ct, res, body = func(**fn_params)
        write_pretty_json(fn, body)
    return (ct, res, body)

def target_parse(target_val):
    if target_val.lower() == 'l':
        return 'lichen'
    elif target_val.lower() == 'b':
        return 'bryophyte'
