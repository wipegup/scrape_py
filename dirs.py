import os

SAVE_ROOT = './downloads/'

def conj(root, *rest):
    *rest, last = rest
    if not '.' in last:
        rest.append(last)
        last = ''
    root =  f"{root}{'/'.join(rest)}"
    if not root.endswith('/'):
        root += '/'
    return f"{root}{last}"
 
def save_dir(run_no, suffix=''):
    return conj(SAVE_ROOT, f'run_{run_no}', suffix)

def head_dir(run_no, suffix=''):
    return conj(save_dir(run_no), 'head', suffix)

def raw_dir(run_no, suffix=''):
    return conj(save_dir(run_no), 'raw', suffix)

def tsv_dir(run_no, suffix=''):
    return conj(save_dir(run_no), 'tsvs', suffix)

def clean_dir(run_no, suffix=''):
    return conj(save_dir(run_no), 'clean_ws', suffix)

def list_files(target_dir, suffix='', filter_fn= lambda x: True):
    return set([f for f in list(os.listdir(target_dir)) if f.endswith(suffix)])
    
