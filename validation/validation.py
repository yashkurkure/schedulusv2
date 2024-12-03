import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import csv

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
pbs1_r = pbs1[pbs1['event'] == 'R'][['id', 'time']].sort_values(by='id').copy()
pbs2_r = pbs2[pbs2['event'] == 'R'][['id', 'time']].sort_values(by='id').copy()
cqsim_r = cqsim[cqsim['event'] == 'R'][['id', 'time']].sort_values(by='id').copy()
schedulus_r = schedulus[schedulus['event'] == 'R'][['id', 'time']].sort_values(by='id').copy()
cqsim2_r = cqsim2[cqsim2['event'] == 'R'][['id', 'time']].sort_values(by='id').copy()


# Output the parsed events
pbs1_r.to_csv('parsed_pbs1.csv', index=False)
pbs2_r.to_csv('parsed_pbs2.csv', index=False)
cqsim_r.to_csv('parsed_cqsim.csv',  index=False)
cqsim2_r.to_csv('parsed_cqsim2.csv',  index=False)

# Calcluate the delta between run events w.r.t to pbs2
pbs2_d = pd.merge(pbs2_r, pbs1_r, on='id')
pbs2_d['delta'] = pbs2_d['time_x'] - pbs2_d['time_y']
pbs2_d['delta'] = (pbs2_d['delta']/3600)

cqsim_d = pd.merge(cqsim_r, pbs1_r, on='id')
cqsim_d['delta'] = cqsim_d['time_x'] - cqsim_d['time_y']
cqsim_d['delta'] = (cqsim_d['delta']/3600)

schedulus_d = pd.merge(schedulus_r, pbs1_r, on='id')
schedulus_d['delta'] = schedulus_d['time_x'] - schedulus_d['time_y']
schedulus_d['delta'] = (schedulus_d['delta']/3600)

cqsim2_d = pd.merge(cqsim2_r, pbs1_r, on='id')
cqsim2_d['delta'] = cqsim2_d['time_x'] - cqsim2_d['time_y']
cqsim2_d['delta'] = (cqsim2_d['delta']/3600)

# Create a new field to differentiate the data
plt.figure(figsize=(30, 10))  # Adjust figure size if needed

sns.scatterplot(x='id', y='delta', data=pbs2_d, color='black', label=r'x = PBS2', s=175, marker='o')
sns.scatterplot(x='id', y='delta', data=cqsim2_d, color='red', label=r'x = CQSimV2', s=175, marker='s')
sns.scatterplot(x='id', y='delta', data=cqsim_d, color='blue', label=r'x = CQSim', s=175, marker='^')
sns.scatterplot(x='id', y='delta', data=schedulus_d, color='green', label=r'x = Schedulus', s=175, marker='x')

plt.xlabel('Job ID', fontsize=40)
plt.ylabel(r'$\Delta$ T$_{start}$ (Hrs)', fontsize=40)
plt.title(r'$\Delta$ T$_{start}$ (Hrs) vs Job ID', fontsize=30)
plt.grid(alpha=0.3)
plt.xticks(fontsize=25)
plt.yticks(fontsize=25)
plt.legend(title=r'$\Delta$ T$_{start}$ = T$_{start}^{x}$ - T$_{start}^{y}$, y = PBS1', title_fontsize=20 ,fontsize=25, loc='lower left') 
plt.savefig('delta_start.png')

plt.figure(figsize=(10, 8))

sns.kdeplot(pbs2_d['delta'], color='black', label=r'x = PBS2')
sns.kdeplot(cqsim2_d['delta'], color='red', label=r'x = CQSimV2')
sns.kdeplot(cqsim_d['delta'], color='blue', label=r'x = CQSim')
sns.kdeplot(schedulus_d['delta'], color='green', label=r'x = Schedulus')


plt.xlabel(r'$\Delta$ T$_{start}$ (Hrs)', fontsize=20)
plt.ylabel('Density', fontsize=20)
plt.title(r'Kernel Density Estimation of $\Delta$ T$_{start}$ ', fontsize=16)
plt.grid(alpha=0.3)
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)
plt.legend(title=r'$\Delta$ T$_{start}$ = T$_{start}^{x}$ - T$_{start}^{y}$, y = PBS1', title_fontsize=16, fontsize=18, loc='upper left')
plt.ylim(ymin=-0.1)
plt.savefig('kde_delta_start.png')


# Extract the queue events
pbs1_q = pbs1[pbs1['event'] == 'Q'][['id', 'time']].sort_values(by='id').copy()
pbs2_q = pbs2[pbs2['event'] == 'Q'][['id', 'time']].sort_values(by='id').copy()
cqsim_q = cqsim[cqsim['event'] == 'Q'][['id', 'time']].sort_values(by='id').copy()
schedulus_q = schedulus[schedulus['event'] == 'Q'][['id', 'time']].sort_values(by='id').copy()
cqsim2_q = cqsim2[cqsim2['event'] == 'Q'][['id', 'time']].sort_values(by='id').copy()


# Calculate the wait times
pbs1_w = pd.merge(pbs1_q, pbs1_r, on='id', suffixes=('_q', '_r'))
pbs1_w['wait'] = pbs1_w['time_r'] - pbs1_w['time_q']
pbs2_w = pd.merge(pbs2_q, pbs2_r, on='id', suffixes=('_q', '_r'))
pbs2_w['wait'] = pbs2_w['time_r'] - pbs2_w['time_q']
cqsim_w = pd.merge(cqsim_q, cqsim_r, on='id', suffixes=('_q', '_r'))
cqsim_w['wait'] = cqsim_w['time_r'] - cqsim_w['time_q']
schedulus_w = pd.merge(schedulus_q, schedulus_r, on='id', suffixes=('_q', '_r'))
schedulus_w['wait'] = schedulus_w['time_r'] - schedulus_w['time_q']
cqsim2_w = pd.merge(cqsim2_q, cqsim2_r, on='id', suffixes=('_q', '_r'))
cqsim2_w['wait'] = cqsim2_w['time_r'] - cqsim2_w['time_q']


# Caluculate the average wait times
pbs1_aw = pbs1_w['wait'].mean()
pbs2_aw = pbs2_w['wait'].mean()
cqsim_aw = cqsim_w['wait'].mean()
schedulus_aw = schedulus_w['wait'].mean()
cqsim2_aw = cqsim2_w['wait'].mean()

# Calcluate the deviation in average wait times
cqsim_aw_delta = 100*abs(cqsim_aw - pbs1_aw)/pbs1_aw
schedulus_aw_delta = 100*abs(schedulus_aw - pbs1_aw)/pbs1_aw
cqsim2_aw_delta = 100*abs(cqsim2_aw - pbs1_aw)/pbs1_aw

# Create a list of the delta values
delta_values = [cqsim2_aw_delta, cqsim_aw_delta, schedulus_aw_delta]

# Create a list of labels
labels = ['CQSimV2', 'CQSim', 'Schedulus']

# Create the bar plot
plt.figure(figsize=(10, 8))
plt.bar(labels, delta_values, color=['red', 'blue', 'green'])

# Add labels and title
plt.xlabel('Simulator', fontsize=20)
plt.ylabel(r'% Deviation from real machine: OpenPBS', fontsize=20)
plt.title(r'% Deviation in average wait time from real machine', fontsize=16)

# Customize ticks and grid
plt.xticks(fontsize=20)
plt.yticks(fontsize=20)
plt.grid(axis='y', alpha=0.3)

# Show the plot
plt.savefig('average_wait_deviation.png')

with open('average_wait_deviation.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(labels)
    writer.writerow(delta_values)



