def save_dir(run_no):
    return f'./downloads/run_{run_no}/'

def head_dir(run_no):
    return f'{save_dir(run_no)}head/'

def raw_dir(run_no):
    return f'{save_dir(run_no)}raw/'

def tsv_dir(run_no):
    return f'{save_dir(run_no)}tsvs/'

def clean_dir(run_no):
    return f'{save_dir(run_no)}clean_ws/'