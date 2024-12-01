import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

# Read the csv's
cqsim = pd.read_csv('cqsim.csv', names=['time', 'event', 'id'])
schedulus = pd.read_csv('schedulus.csv', names=['time', 'event', 'id'])
schedulus_t0 = schedulus['time'].iloc[0]
schedulus['time'] = schedulus['time'] - schedulus_t0
pbs1 = pd.read_csv('pbs1.csv', names=['time', 'event', 'id'])
pbs1_t0 = pbs1['time'].iloc[0]
pbs1['time'] = pbs1['time'] - pbs1_t0


cqsim_r = cqsim[cqsim['event'] == 'R'][['id', 'time']].sort_values(by='id')
print(len(cqsim_r))
schedulus_r = schedulus[schedulus['event'] == 'R'][['id', 'time']].sort_values(by='id')
print(len(schedulus_r))
pbs1_r = pbs1[pbs1['event'] == 'R'][['id', 'time']].sort_values(by='id')
print(len(pbs1_r))

# delta cqsim
cqsim_delta = pd.merge(pbs1_r, cqsim_r, on='id')
cqsim_delta['delta'] = cqsim_delta['time_x'] - cqsim_delta['time_y']
cqsim_delta['delta'] = cqsim_delta['delta']/3600
print(cqsim_delta)

# delta schedulus
sched_delta = pd.merge(pbs1_r, schedulus_r, on='id')
sched_delta['delta'] = sched_delta['time_x'] - sched_delta['time_y']
sched_delta['delta'] = sched_delta['delta']/3600
print(sched_delta)

# Create a new field to differentiate the data
plt.figure(figsize=(10, 6))  # Adjust figure size if needed

sns.scatterplot(x='id', y='delta', data=sched_delta, color='blue', label=r'x = Real 1, y = Schedulus')
sns.scatterplot(x='id', y='delta', data=cqsim_delta, color='red', label=r'x = Real 1, y = CQSim')

plt.xlabel('Job ID', fontsize=16)
plt.ylabel(r'$\Delta$ T$_{start}$ (Hrs)', fontsize=16)
plt.title(r'$\Delta$ T$_{start}$ (Hrs) vs Job ID', fontsize=16)
plt.grid(alpha=0.3)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.legend(title=r'$\Delta$ T$_{start}$ = T$_{start}^{x}$ - T$_{start}^{y}$', title_fontsize=16 ,fontsize=12, loc='lower left') 
plt.savefig('delta_start.png')

plt.figure(figsize=(10, 6))

sns.kdeplot(sched_delta['delta'], color='blue', label=r'x = Real 1, y = Schedulus')
sns.kdeplot(cqsim_delta['delta'], color='red', label=r'x = Real 1, y = CQSim')

plt.xlabel(r'$\Delta$ T$_{start}$ (Hrs)', fontsize=16)
plt.ylabel('Density', fontsize=16)
plt.title(r'Kernel Density Estimation of $\Delta$ T$_{start}$ ', fontsize=16)
plt.grid(alpha=0.3)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.legend(title=r'$\Delta$ T$_{start}$ = T$_{start}^{x}$ - T$_{start}^{y}$', title_fontsize=16, fontsize=12, loc='upper left')
plt.ylim(ymin=-0.1)
plt.savefig('kde_delta_start.png')