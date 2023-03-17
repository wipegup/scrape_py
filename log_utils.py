import utils
def print_exception_error(error_ct, allowed_error_ct, inst):
    print(f"ENCOUNTERED ERROR {error_ct}/{allowed_error_ct} ALLOWED")
    print(type(inst))
    print(inst)

def occur_interval(hf, occur_request_ct):
    have_need = have_need_req_str(hf)
    now, interval_string = interval_update_string(hf, 'occur')
    session_records = (hf['count']['tsv'] + hf['count']['json']) - hf['count']['start']
    print(f"Request # {occur_request_ct} {interval_string} {have_need} {session_records:,} Records this session")
    return now


def have_need_req(hf):
    print(have_need_req_str(hf))

def have_need_req_str(hf):
    curr = (hf['count']['tsv'] + hf['count']['json'])
    need = hf['full_occur']['ct']
    api_reqs = round((need - curr)/300)
    s = f"Have {curr:,} / {need:,} records. ~{api_reqs:,} more API reqs."
    return s

def start_time(hf, k):
    full_k= f'{k}_start'
    s = f"{k.capitalize()} pull started at {utils.pretty_time(hf['time'][full_k])}"
    print(s)


def interval_update_string(hf, k):
    k= f'{k}_start'
    now = utils.mark_time()
    s = f" at {utils.pretty_time(now)}. Interval: {now - hf['time']['interval']}. Total: {now- hf['time'][k]}"
    return now, s

def interval_update(hf, k, init_str):
    now, s = interval_update_string(hf, k)
    print(f"{init_str}{s}")
    return now
