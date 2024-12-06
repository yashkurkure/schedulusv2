from simulator import Simulator

s = Simulator()

s.read_data('../data/polaris24/input/polaris24.swf', '../data/polaris24/input/system.json')

s.initialize('../data/polaris24/output')

# s.simulate()

try:
    s.simulate()
except Exception as e:
    print(e)
    s.cleanup()