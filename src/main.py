from simulator import Simulator

s = Simulator()

s.read_data('../data/input/job_log.swf', '../data/input/system.json')

s.simulate()
