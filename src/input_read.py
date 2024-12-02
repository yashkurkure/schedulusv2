import json
import pandas as pd
from dataclasses import dataclass


@dataclass(frozen=True)
class DfFileds:

    @dataclass(frozen=True)
    class Job:
        ID: str = "id"
        SUBMIT_TS: str = "submit"
        WAIT_T: str = "wait"
        RUN_T: str = "run"
        REQ_PROC: str = "used_proc"
        REQ_T: str = "req_time"

    @dataclass(frozen=True)
    class Event:
        JOB_ID: str = "id"
        TIME: str = "timestamp"
        TYPE: str = "event"
        LOCATION: str = "location"

@dataclass
class SystemConfig:
    nodes: int
    ppn: int

swf_columns = [
    'id',             #1
    'submit',         #2
    'wait',           #3
    'run',            #4
    'used_proc',      #5
    'used_ave_cpu',   #6
    'used_mem',       #7
    'req_proc',       #8
    'req_time',       #9
    'req_mem',        #10 
    'status',         #11
    'user_id',        #12
    'group_id',       #13
    'num_exe',        #14
    'num_queue',      #15
    'num_part',       #16
    'num_pre',        #17
    'think_time',     #18
]

event_data_columns = [
    "timestamp",
    "event",
    "id",
    "location"
]

def read_job_data(path, SWF = False) -> pd.DataFrame:
    """
    Reads job data
    """

    if SWF:
        data = []
        with open(f'{path}', 'r') as file:
            for line in file:
            
                # TODO: For now ignoring the header of the swf file
                if line[0] == ';':
                    continue

                # Split the line into elements, convert non-empty elements to integers
                row = [int(x) for x in line.split() if x]
                data.append(row)
        df = pd.DataFrame(data, columns=swf_columns)
        return df
    # TODO: validate the job data
    return pd.read_csv(path, names=swf_columns)

def read_event_data(path) -> pd.DataFrame:
    """
    Reads event data
    """
    # TODO: validate the event data
    df = pd.read_csv(path, names=event_data_columns)
    df_t0 = df['timestamp'].iloc[0]
    df['timestamp'] = df['timestamp'] - df_t0
    return df

def read_system_config(path) -> SystemConfig:
    """
    Reads system config
    """
    with open(path, 'r') as f:
        data = json.load(f)
    config = SystemConfig(**data)
    return config