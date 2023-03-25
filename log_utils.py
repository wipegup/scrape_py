import utils
def print_exception_error(error_ct, allowed_error_ct, inst):
    print(f"ENCOUNTERED ERROR {error_ct}/{allowed_error_ct} ALLOWED")
    print(type(inst))
    print(inst)

def occur_interval(hf, occur_request_ct):
    have_need = have_need_req_str(hf)
    session_records = collected_record_ct(hf) - hf['count']['start']
    init_str = f"Request # {occur_request_ct} {have_need} {session_records:,} Records this session"
    return interval_update(hf, 'occur', init_str)

def collected_record_ct(hf):
    return sum([v for k,v in hf['count'].items() if k != 'start'])

def have_need_req(hf):
    print(have_need_req_str(hf))

def have_need_req_str(hf):
    curr = collected_record_ct(hf)
    need = hf['full_occur']['ct']
    api_reqs = round((need - curr)/300)
    s = f"Have {curr:,} / {need:,} records. ~{api_reqs:,} more API reqs."
    return s

def start_time(hf, k):
    full_k= f'{k}_start'
    hf['time'][full_k] = hf['time']['interval'] = utils.mark_time()
    s = f"{k.capitalize()} work started at {utils.pretty_time(hf['time'][full_k])}"
    print(s)
    return hf


def interval_update_string(hf, k):
    k= f'{k}_start'
    now = utils.mark_time()
    s = f" at {utils.pretty_time(now)}. Interval: {now - hf['time']['interval']}. Total: {now- hf['time'][k]}"
    return now, s

def interval_update(hf, k, init_str):
    now, s = interval_update_string(hf, k)
    hf['time']['interval'] = now
    print(f"{init_str}{s}")
    return hf
