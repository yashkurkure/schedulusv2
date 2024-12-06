# Schedulus V2

A discrete event simulator for job sceduling in HPC based on the Simulus framework for discrete event simulation.


## Instructions

Create python virtual environment
```
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies
```
pip install .
```

## Running simulations

For the pbs trace run:
```
python3 pbs.py
```

For the theta 22 trace run:
```
python3 theta_22.py
```

Outputs can be observed in `/data/pbs/output` and `data/theta22/output`.