from simulator import Simulator

s = Simulator()

s.read_data('../data/theta23/input/theta23.swf', '../data/theta23/input/system.json')

s.initialize('../data/theta23/output')

# s.simulate()

try:
    s.simulate()
except Exception as e:
    print(e)
    s.cleanup()