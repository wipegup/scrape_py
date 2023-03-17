from datetime import datetime
from time import sleep

class RequestManager:
    def __init__(self, individual_sleep=2.5, global_sleep=600, allowed_error_ct=5, error_backoff=300, global_retries=6):
        self.individual_sleep = individual_sleep
        self.global_sleep = global_sleep
        self.allowed_error_ct = allowed_error_ct
        self.error_backoff = error_backoff
        self.global_retries = global_retries
        self.last_request = None
        self.last_error = None
        self.error_ct = 0

    def make_request(self, func, *args, **kwargs):
        sleep_needed = self.individual_sleep - (self.mark_time() - self.last_request).total_seconds() if self.last_request else 0
        if sleep_needed > 0:
            sleep(sleep_needed)
        
        # Make request
        try:
            res = func(*args, **kwargs)
        except Exception as inst:
            return self.request_error(inst, func, *args, **kwargs)
        else:
            self.last_request = self.mark_time()
            self.check_error_backoff()
            return res

    def check_error_backoff(self):
        now = self.mark_time()
        if self.last_error and (now - self.last_error).total_seconds() >= self.error_backoff:
            self.last_error = now
            new_err_ct  = self.error_ct -1
            self.error_ct = max(0, new_err_ct)
            
            if new_err_ct >=0:
                print(f"More than {self.error_backoff} seconds w/o error. Decrementing Error Count to {self.error_ct}")
        
    def mark_time(self):
        return datetime.now()

    def request_error(self, inst, func, *args, **kwargs):
        self.error_ct += 1
        print(f"Encountered Error: {type(inst)}, {inst}.\n{self.error_ct} / {self.allowed_error_ct} errors allowed ")
        self.last_error = self.mark_time()

        if self.error_ct >= self.allowed_error_ct:
            if self.global_retries > 0:
                print(f"Pausing requests for {self.global_sleep} seconds. Error count resetting")
                self.error_ct = 0
                self.global_retries -= 1
                sleep(self.global_sleep)
            else:
                print(f"Out of Retries. Ending Program")
                raise Exception("TOO MANY ERRORS")
        
        return self.make_request(func, *args, **kwargs)
    
