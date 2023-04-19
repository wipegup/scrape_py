def save_dir(run_no, suffix=''):
    return f'./downloads/run_{run_no}/{suffix}'

def head_dir(run_no, suffix=''):
    return f'{save_dir(run_no)}head/{suffix}'

def raw_dir(run_no, suffix=''):
    return f'{save_dir(run_no)}raw/{suffix}'

def tsv_dir(run_no, suffix=''):
    return f'{save_dir(run_no)}tsvs/{suffix}'

def clean_dir(run_no, suffix=''):
    return f'{save_dir(run_no)}clean_ws/{suffix}'