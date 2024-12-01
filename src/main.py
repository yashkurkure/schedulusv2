from schedulus import SchedulusV2

s = SchedulusV2(
    '../data/input/event_log.csv',
    '../data/input/job_log.csv',
    '../data/input/system.json')

s.simulate()
