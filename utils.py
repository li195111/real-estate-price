import sys
import traceback
import json
from datetime import datetime

class ErrorMessage:
    def __init__(self, error_class, detail, file_track) -> None:
        self.error_class = error_class
        self.detail = detail
        self.file_track = file_track
        
    def __str__(self) -> str:
        return self.part_message
    
    @property
    def part_message(self):
        return f"[{self.error_class}] {self.detail}"
    
    @property
    def details_message(self):
        return f"\n{self.file_track}\n[{self.error_class}] {self.detail}\n"

def error_msg(err):
    error_class = err.__class__.__name__
    if len(err.args) > 0:
        detail = err.args[0]
    else:
        detail = ''
    cl, exc, tb = sys.exc_info()
    details = '\n'.join([f"File \"{s[0]}\", line {s[1]} in {s[2]}" for s in traceback.extract_tb(tb)])
    return ErrorMessage(error_class,detail,details)

# Redis
# Remove Expired Token from Redis
def clean_key_pairs(rdb):
    try:
        for k,v in rdb.hgetall('KEY_PAIR').items():
            KEY = json.loads(v)
            if datetime.fromtimestamp(KEY['exp']) < datetime.utcnow():
                rdb.hdel('KEY_PAIR',k)
    except Exception as e:
        print (error_msg(e))

def utc_now()->str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%F")
