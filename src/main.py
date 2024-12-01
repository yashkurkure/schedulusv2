import src.schedulus as schedulus

s = schedulus.Schedulus(32, 'easy', '../data/output/test.out')
s.read_jobs('../data/input/test.swf')
s.run('fcfs')