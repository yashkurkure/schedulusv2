from dataclasses import dataclass
import pandas as pd
import simulus
from components.scheduler import *
from components.allocator import *
from input_read import \
read_event_data, \
read_job_data, \
read_system_config, \
read_event_data_job_log, \
DfFileds, \
SystemConfig

__metaclass__ = type

class EventType(Enum):

    class Sched(Enum):
        SUBMIT = 1
        START = 2
        END = 3
        SUCCESS = 4
        FAIL = 5

    class Alloc(Enum):
        ALLOCATE = 7
        DEALLOCATE = 8
        OFFLINE = 9
        ONLINE = 10

def ET2CHAR(i):
    if i == EventType.Sched.SUBMIT:
        return 'Q'
    elif i == EventType.Sched.START:
        return 'R'
    elif i == EventType.Sched.END:
        return 'E'
    else:
        return 'X'

@dataclass
class Event:
    time: int
    type: EventType

@dataclass
class SchedulerEvent(Event):
    job_id: int

@dataclass
class AllocatorEvent(Event):
    resource_id = int


class Simulator:
    def __init__(self):
        self.sim = None
        
        self.df_events: pd.DataFrame = None
        self.df_jobs: pd.DataFrame = None
        self.system_config: SystemConfig = None

        # Initialize components
        self.allocator = None
        self.scheduler = None

    def read_data(self, path_job_log, path_system_config, job_log_CSV=False):
        self.df_jobs: pd.DataFrame = read_job_data(path_job_log, CSV=job_log_CSV)
        self.system_config: SystemConfig = read_system_config(path_system_config)
        self.df_events: pd.DataFrame = read_event_data_job_log(self.df_jobs)

    def read_data_swf(self, path_swf):
        raise NotImplementedError('Need to implement reading swf along with system config')
        self.df_events: pd.DataFrame = None
        self.df_jobs: pd.DataFrame = None
        self.system_config: SystemConfig = None

    def read_data_with_events(self, path_job_log, path_system_config, path_event_log, job_log_CSV=False):
        self.df_jobs: pd.DataFrame = read_job_data(path_job_log, job_log_CSV=job_log_CSV)
        self.system_config: SystemConfig = read_system_config(path_system_config)
        self.df_events: pd.DataFrame = read_event_data(path_event_log)
        pass

    def now(self):
        return self.sim.now

    def handle_scheduler_event(self, e: SchedulerEvent):
        print(f"{self.sim.now},{ET2CHAR(e.type)},{e.job_id}")
        
        # Get the job data related to the event
        job_data = self.df_jobs[self.df_jobs[DfFileds.Job.ID] == e.job_id]

        # Handle the event
        if e.type == EventType.Sched.SUBMIT:

            # Queue the job 
            self.scheduler.queue(
                Job(
                    id=e.job_id,
                    name=f'job.{e.job_id}',
                    resources=job_data[DfFileds.Job.REQ_PROC].item(),
                    walltime=job_data[DfFileds.Job.REQ_T].item()
                )
            )

        elif e.type == EventType.Sched.START:

            # Start the job
            self.scheduler.start(e.job_id)

            # Schedule the job end event
            e = SchedulerEvent(
                time=self.sim.now + job_data[DfFileds.Job.RUN_T].item(),
                type=EventType.Sched.END,
                job_id=e.job_id
            )
            self.sim.sched(self.handle_scheduler_event, e, until=e.time)

        elif e.type == EventType.Sched.END:
            self.scheduler.end(e.job_id)
            pass
        else:
            raise NotImplementedError(f'Event {e.type} not implemented!')

    def handle_allocator_event(self, e: AllocatorEvent):
        # print(f"{self.sim.now},{ET2CHAR(e.type)},{e.job_id}")
        if e.type == EventType.Alloc.ALLOCATE:
            pass
        elif e.type == EventType.Alloc.DEALLOCATE:
            pass
        else:
            raise NotImplementedError(f'Event {e.type} not implemented!')

    def create_run_event(self, job_id):
        # print(f'Creating run event for: {job_id}')
        e = SchedulerEvent(
            time=self.sim.now,
            type=EventType.Sched.START,
            job_id=job_id
        )
        self.sim.sched(
            self.handle_scheduler_event,
            e, 
            until=e.time
        )

    def create_alloc_event(self, resource_id):
        # print(f'Creating run event for: {job_id}')
        e = AllocatorEvent(
            time=self.sim.now,
            type=EventType.Alloc.ALLOCATE
        )
        self.sim.sched(
            self.handle_allocator_event,
            e,
            until=e.time,
            resource_ids=resource_id
        )

    def create_dealloc_event(self, resource_id):
        # print(f'Creating run event for: {job_id}')
        e = AllocatorEvent(
            time=self.sim.now,
            type=EventType.Alloc.DEALLOCATE
        )
        self.sim.sched(
            self.handle_allocator_event,
            e,
            until=e.time,
            resrource_ids=resource_id
        )
        
    def initialize(self):

        # Make sure data was read
        if self.df_jobs is None or self.system_config is None or self.df_events is None:
            raise EnvironmentError('No data was passed to simulator!')
        

        # Initialize components
        self.allocator = Allocator(self, self.system_config.nodes)
        self.scheduler = Scheduler(self)
            
        start_time: int = -1
        submit_events: list[SchedulerEvent] = []

        # Get all the submit evetns
        df_submit_events = self.df_events[self.df_events[DfFileds.Event.TYPE] == 'Q']
        for index, row in df_submit_events.iterrows():
            
            e = SchedulerEvent(
                time=row[DfFileds.Event.TIME],
                type=EventType.Sched.SUBMIT,
                job_id=row[DfFileds.Event.JOB_ID]
            )
            
            if start_time == -1:
                start_time = e.time

            submit_events.append(e)

        # Define the simulator
        # Init the time to the first submit event
        self.sim = simulus.simulator(name = 'schedulus', init_time = start_time)


        # Schedule all the submit events
        for e in submit_events:
            self.sim.sched(self.handle_scheduler_event, e, until=e.time)

    def simulate(self):
        
        # Initialize
        self.initialize()

        # Run the simulation
        self.sim.run()

    def step(self):

        if not self.sim:
            self.initialize()
        self.sim.step()