from dataclasses import dataclass
import pandas as pd
import simulus
from src.components.scheduler import *
from src.components.allocator import *
from input_read import \
read_event_data, \
read_job_data, \
read_system_config, \
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
    elif i == EventType.Sched.SUCCESS or i == EventType.Sched.SUCCESS:
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
    node_ids = list[int]


class SchedulusV2:
    def __init__(self, path_event_log, path_job_log, path_system_config):

        
        self.df_events: pd.DataFrame = read_event_data(path_event_log)
        self.df_jobs: pd.DataFrame = read_job_data(path_job_log)
        self.system_config: SystemConfig = read_system_config(path_system_config)

        # Initialize components
        self.allocator = Allocator(self.system_config.nodes)
        self.scheduler = Scheduler(self.allocator)


    def handle_scheduler_event(self, e: SchedulerEvent):
        print(f"{self.sim.now},{ET2CHAR(e.type)},{e.job_id}")
        if e.type == EventType.Sched.SUBMIT:
            pass
        elif e.type == EventType.Sched.START:
            pass
        elif e.type == EventType.Sched.END:
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
        

    def simulate(self):

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

        # Run the simulation
        self.sim.run()



    def __process_submit(self, job_id):
        print(f'{int(self.sim.now)},Q,{job_id}')
        submitted = []
        started = []

        job = self.jobs[job_id]

        if job.req_proc <= self.cluster.total:
            job.submit()
            self.waiting.append(job_id)
            self.schedule.append(job_id)
            submitted.append(job_id)

            s_job = self.jobs[self.schedule[0]]

            if self.cluster.allocate(s_job.req_proc):
                self.__initiate_job(s_job.id)
                started.append(s_job.id)
            elif self.backfill != 'none':
                started.extend(self.__backfill(s_job.id))

            # self.__log(submitted, started, [])


    def __process_end(self, job_id):
        print(f'{int(self.sim.now)},E,{job_id}')
        started = []
        finished = []

        job = self.jobs[job_id]

        job.finish(self.sim.now)
        self.cluster.release(job.req_proc)
        self.running.remove(job_id)
        finished.append(job_id)

        do_backfill = self.backfill != 'none' and len(self.schedule) > 0

        while self.schedule and self.cluster.allocate(self.jobs[self.schedule[0]].req_proc):
            next_job_id = self.schedule[0]
            self.__initiate_job(next_job_id)
            started.append(next_job_id)
            do_backfill = False

        if do_backfill:
            started.extend(self.__backfill(self.jobs[self.schedule[0]].id))



    # https://www.cse.huji.ac.il/~perf/ex11.html
    def __backfill(self, job_id):
        job = self.jobs[job_id]

        proc_pool = self.cluster.idle
        idle_procs = self.cluster.idle
        extra_procs = 0
        shadow_time = -1

        jobs_exp_end = []
        for r_job_id in self.running:
            # print('Job ' + str(r_job_id) + ' has been running for ' + str(self.sim.now - self.jobs[r_job_id].start_time) + ' seconds')
            # print('Job ' + str(r_job_id) + ' is expected to end at time ' + str(self.jobs[r_job_id].start_time + self.jobs[r_job_id].req_time))
            jobs_exp_end.append((r_job_id, self.jobs[r_job_id].start_time + self.jobs[r_job_id].req_time))
        jobs_exp_end.sort(key=lambda tup: tup[1])

        for job_exp_end in jobs_exp_end:
            if proc_pool < job.req_proc:
                proc_pool += self.jobs[job_exp_end[0]].req_proc
            if proc_pool >= job.req_proc:
                shadow_time = job_exp_end[1]
                extra_procs = proc_pool - job.req_proc

        started = []

        for w_job_id in self.waiting[1:].copy():
            # Condition 1: It uses no more than the currently available processors, and is expected to terminate by the shadow time.
            if self.sim.now + self.jobs[w_job_id].req_time < shadow_time and self.cluster.allocate(self.jobs[w_job_id].req_proc):
                self.__initiate_job(w_job_id)
                started.append(w_job_id)
            # Condition 2: It uses no more than the currently available processors, and also no more than the extra processors.
            elif self.jobs[w_job_id].req_proc <= extra_procs and self.cluster.allocate(self.jobs[w_job_id].req_proc):
                self.__initiate_job(w_job_id)
                extra_procs -= self.jobs[w_job_id].req_proc
                started.append(w_job_id)

        return started


    def __initiate_job(self, job_id):
        print(f'{int(self.sim.now)},R,{job_id}')
        job = self.jobs[job_id]

        job.start(self.sim.now)
        self.sim.sched(self.__process_end, job_id, offset=job.run)
        self.waiting.remove(job_id)
        self.schedule.remove(job_id)
        self.running.append(job_id)


    def read_jobs(self, path):
        self.jobs = {}

        with open(path) as job_file:
            for trace_line in job_file:
                # Find string before ';', then split that on whitespace to find trace fields
                job_fields = trace_line.split(';', 1)[0].split()

                if job_fields and float(job_fields[3]) != -1:
                    self.jobs[int(job_fields[0])] = Job(id=int(job_fields[0]),\
                                                        submit_time=float(job_fields[1]),\
                                                        wait=float(job_fields[2]),\
                                                        run=float(job_fields[3]),\
                                                        used_proc=int(job_fields[4]),\
                                                        used_ave_cpu=float(job_fields[5]),\
                                                        used_mem=float(job_fields[6]),\
                                                        req_proc=int(job_fields[7]) if int(job_fields[7]) != -1 else int(job_fields[4]),\
                                                        req_time=float(job_fields[8]),\
                                                        req_mem=float(job_fields[9]),\
                                                        status=int(job_fields[10]),\
                                                        user_id=int(job_fields[11]),\
                                                        group_id=int(job_fields[12]),\
                                                        num_exe=int(job_fields[13]),\
                                                        num_queue=int(job_fields[14]),\
                                                        num_part=int(job_fields[15]),\
                                                        num_pre=int(job_fields[16]),\
                                                        think_time=int(job_fields[17]),\
                                                        start_time=-1,\
                                                        end_time=-1,\
                                                        score=0,\
                                                        state=0,\
                                                        happy=-1,\
                                                        est_start=-1)


    def run(self, type):

        for job in self.jobs.values():
            self.sim.sched(self.__process_submit, job.id, until=job.submit_time)
            # self.sim.process(self.__process_submit, job, until=job.submit_time, prio=job.id)

        self.sim.run()
