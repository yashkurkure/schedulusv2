import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

from utils import swf_columns

# Read the csv's
pbs1 = pd.read_csv('pbs1.events', names=['time', 'event', 'id', 'location'])
pbs2 = pd.read_csv('pbs2.events', names=['time', 'event', 'id', 'location'])
cqsim = pd.read_csv('cqsim.events', names=['time', 'event', 'id'])
cqsim2 = pd.read_csv('cqsim2.events', names=['time', 'event', 'id'])
schedulus = pd.read_csv('schedulus.events', names=['time', 'event', 'id'])


# drop pbs execjob_end(EE) and execjob_begin(EB) events
pbs1 = pbs1[~pbs1['event'].isin(['EE', 'EB'])]
pbs2 = pbs2[~pbs2['event'].isin(['EE', 'EB'])]
pbs1['event'] = pbs1['event'].replace('O', 'E')
pbs2['event'] = pbs2['event'].replace('O', 'E')

# pbs drop location column
pbs1 = pbs1.drop('location', axis=1)
pbs2 = pbs2.drop('location', axis=1)

# Adjust times to t0 where needed
pbs1_t0 = pbs1['time'].iloc[0]
pbs2_t0 = pbs2['time'].iloc[0]
cqsim2_t0 = cqsim2['time'].iloc[0]
schedulus_t0 = schedulus['time'].iloc[0]
pbs1['time'] = pbs1['time'] - pbs1_t0
pbs2['time'] = pbs2['time'] - pbs2_t0
cqsim2['time'] = cqsim2['time'] - cqsim2_t0
schedulus['time'] = schedulus['time'] - schedulus_t0



# Check if all the dataframes are of the same length as
# number of events should be the same for the same trace
# print(pbs1.size)
# print(pbs2.size)
# print(cqsim.size)
# print(schedulus.size)
# print(cqsim2.size)
assert pbs1.size == pbs2.size == cqsim.size == schedulus.size == cqsim2.size, "DataFrame sizes are not equal"

# Extract the run events
pbs1_r = pbs1[pbs1['event'] == 'R'][['id', 'time']].sort_values(by='id')
pbs2_r = pbs2[pbs2['event'] == 'R'][['id', 'time']].sort_values(by='id')
cqsim_r = cqsim[cqsim['event'] == 'R'][['id', 'time']].sort_values(by='id')
schedulus_r = schedulus[schedulus['event'] == 'R'][['id', 'time']].sort_values(by='id')
cqsim2_r = cqsim2[cqsim2['event'] == 'R'][['id', 'time']].sort_values(by='id')



# Output the parsed events
pbs1_r.to_csv('parsed_pbs1.csv', index=False)
pbs2_r.to_csv('parsed_pbs2.csv', index=False)
cqsim_r.to_csv('parsed_cqsim.csv',  index=False)
cqsim2_r.to_csv('parsed_cqsim2.csv',  index=False)

# Calcluate the delta between run events w.r.t to pbs2
pbs1_d = pd.merge(pbs2_r, pbs1_r, on='id')
pbs1_d['delta'] = pbs1_d['time_x'] - pbs1_d['time_y']
pbs1_d['delta'] = pbs1_d['delta']/3600

cqsim_d = pd.merge(cqsim_r, pbs1_r, on='id')
cqsim_d['delta'] = cqsim_d['time_x'] - cqsim_d['time_y']
cqsim_d['delta'] = cqsim_d['delta']/3600

schedulus_d = pd.merge(schedulus_r, pbs1_r, on='id')
schedulus_d['delta'] = schedulus_d['time_x'] - pbs1_d['time_y']
schedulus_d['delta'] = schedulus_d['delta']/3600

cqsim2_d = pd.merge(cqsim2_r, pbs1_r, on='id')
cqsim2_d['delta'] = cqsim2_d['time_x'] - pbs1_d['time_y']
cqsim2_d['delta'] = cqsim2_d['delta']/3600

# Create a new field to differentiate the data
plt.figure(figsize=(10, 6))  # Adjust figure size if needed

sns.scatterplot(x='id', y='delta', data=pbs1_d, color='blue', label=r'x = PBS1, y = PBS2')
# sns.scatterplot(x='id', y='delta', data=cqsim_d, color='red', label=r'x = CQSim, y = PBS2')
# sns.scatterplot(x='id', y='delta', data=schedulus_d, color='yellow', label=r'x = Schedulus, y = PBS2')
sns.scatterplot(x='id', y='delta', data=cqsim2_d, color='green', label=r'x = CQSimV2, y = PBS2')

plt.xlabel('Job ID', fontsize=16)
plt.ylabel(r'$\Delta$ T$_{start}$ (Hrs)', fontsize=16)
plt.title(r'$\Delta$ T$_{start}$ (Hrs) vs Job ID', fontsize=16)
plt.grid(alpha=0.3)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.legend(title=r'$\Delta$ T$_{start}$ = T$_{start}^{x}$ - T$_{start}^{y}$', title_fontsize=16 ,fontsize=12, loc='lower left') 
plt.savefig('delta_start.png')

plt.figure(figsize=(10, 6))

sns.kdeplot(pbs1_d['delta'], color='blue', label=r'x = PBS1, y = PBS2')
# sns.kdeplot(cqsim_d['delta'], color='red', label=r'x = CQSim, y = PBS2')
# sns.kdeplot(schedulus_d['delta'], color='yellow', label=r'x = Schedulus, y = PBS2')
sns.kdeplot(cqsim2_d['delta'], color='green', label=r'x = CQSimV2, y = PBS2')


plt.xlabel(r'$\Delta$ T$_{start}$ (Hrs)', fontsize=16)
plt.ylabel('Density', fontsize=16)
plt.title(r'Kernel Density Estimation of $\Delta$ T$_{start}$ ', fontsize=16)
plt.grid(alpha=0.3)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.legend(title=r'$\Delta$ T$_{start}$ = T$_{start}^{x}$ - T$_{start}^{y}$', title_fontsize=16, fontsize=12, loc='upper left')
plt.ylim(ymin=-0.1)
plt.savefig('kde_delta_start.png')