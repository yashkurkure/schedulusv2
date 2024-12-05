from simulator import Simulator

s = Simulator()

s.read_data('../data/theta22/input/theta22.swf', '../data/theta22/input/system.json')

s.initialize('../data/theta22/output')

# s.simulate()


while True:
    input('Press a key...')
    try:
        s.step()
    except Exception as e:
        print(e)
        s.cleanup()