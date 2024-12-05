from simulator import Simulator

s = Simulator(
    '../data/input/event_log.csv',
    '../data/input/job_log.csv',
    '../data/input/system.json')

while True:
    input('Press a key to step')
    s.step()

