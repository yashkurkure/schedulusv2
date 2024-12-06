from simulator import Simulator

s = Simulator()

s.read_data('../data/pbs/input/job_log.swf', '../data/pbs/input/system.json')

s.initialize('../data/pbs/output')

try:
    s.simulate()
except Exception as e:
    print(e)
    s.cleanup()
s.cleanup()
# s.simulate()
