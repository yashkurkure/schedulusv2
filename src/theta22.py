from simulator import Simulator

s = Simulator()

s.read_data('../data/theta22/input/theta22.swf', '../data/theta22/input/system.json')

s.initialize('../data/theta22/output')

# s.simulate()

try:
    s.simulate()
except Exception as e:
    print(e)
    s.cleanup()
