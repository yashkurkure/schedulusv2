# Schedulus V2

A discrete event simulator for job sceduling in HPC based on the Simulus framework.


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
python3 theta22.py
```

Outputs can be observed in `/data/pbs/output` and `data/theta22/output`.

Similary, the Theta 2023 and Polaris 2024 can be simulated using theta23.py and polaris24.py.