from dataclasses import dataclass
from enum import Enum
import random

from components.allocator import Allocator
__metaclass__ = type


class JobState(Enum):
    WAITING = 1
    RUNNING = 2
    FINISHED = 3


@dataclass
class Job:

    # These stay the same
    id: int
    name: str
    resources: int
    walltime: int

    # These may change
    state: JobState = JobState.WAITING
    resource_ids: list[int] = None

    # Result fields
    res_submit_ts = -1
    res_run_ts = -1
    res_end_ts = -1


class Scheduler:

    def __init__(self, simulator):
        self.simulator = simulator
        self.allocator: Allocator = self.simulator.allocator
        self._queue: list[Job] = []
        self._running: list[Job] = []
        self._finished: list[Job] = []

        # Keeps track of pending run events
        self._pending_run: list[int] = []

        pass
    


    def queue(self, job: Job):
        """
        Returns list of events that must be processed by the simulator.
        """
        job.res_submit_ts = self.simulator.now()

        # Add the job to the queue
        self._queue.append(job)

        # Run a scheduling cycle
        self._schedule()

    
    def start(self, job_id):

        # Look for the job in queued jobs
        job = None
        for j in self._queue:
            if j.id == job_id:
                job = j
                break
        if job == None:
            print('Start Error: Job not found in queue')
            exit()

        # Update the state of the job
        job.res_run_ts = self.simulator.now()
        job.state = JobState.RUNNING

        # Remove from queue
        self._queue.remove(job)

        # Add to list of running jobs
        self._running.append(job)


    def end(self, job_id):

        # Look for the job in running jobs
        job = None
        for j in self._running:
            if j.id == job_id:
                job = j
                break
        if job == None:
            print('End Error: Job not found in running job')
            exit()
        
        # Update the state of the job
        job.res_end_ts = self.simulator.now()
        job.state = JobState.FINISHED

        # Deallocate the resources for the job
        self.allocator.deallocate(job.id)

        # Remove from list of running jobs
        self._running.remove(job)

        # Add to list of finished jobs
        self._finished.append(job)

        # Run a scheduling cycle
        self._schedule()


    def _schedule(self):
        """
        Goes over the queue and tries to schedule jobs.
        """
        # print('Running scheduling cycle..')
        # Try and schedule jobs in the head of the queue
        for job in self._queue:

            # If a job has a pending run event, dont consider it for sched cycle
            if job.id in self._pending_run:
                continue
            
            # Try allocating resources
            resources = self.allocator.allocate(job.id, job.resources)

            # If no resources stop
            # Ensures strict ordering
            if not resources:
                break
            
            # Give the job its resources
            job.resource_ids = resources

            # Run the job
            self._pending_run.append(job.id)
            self.simulator.create_run_event(job.id)


        # Attempt to backfill if jobs are still in queue
        # NOTE: We backfill around the top 1 job, so the queue must have 2 jobs
        if len(self._queue) > 0:
            self._backfill_easy()

        # print('Leaving scheduling cycle..')
        pass

    def _build_time_resource_map(self):
        # print('Building time resrouce map:')

        # Sort the jobs by their end times
        running_jobs = sorted(self._running, key=lambda x: x.res_run_ts + x.walltime)

        trm = {}
        cumulative = [resource.id for resource in self.allocator.get_available()]
        trm[self.simulator.now()] = cumulative[:]
        # print(f'\t\tNow {self.simulator.now()}, {trm[self.simulator.now()]}')
        for j in running_jobs:
            end_time = j.res_run_ts + j.walltime
            resource_ids = j.resource_ids

            # NOTE: If a job is ending now, and its end event hasnt occured, it must 
            # removed from the running jobs list to build the time resource map
            if end_time == self.simulator.now():
                continue

            # print(f'\t\tJob: {j.id}, {end_time}, {resource_ids}')

            cumulative += resource_ids[:]

            if end_time in trm:
                trm[end_time] += resource_ids[:]
            else:
                trm[end_time] = cumulative[:]

        return dict(sorted(trm.items()))

    def _backfill_easy(self):
        """
        Tries to backfill jobs without delaying the 1st job in the queue.
        """

        # print('#### BACKFILL ####')
        trm = self._build_time_resource_map()
        # print('\tTRM Initial:')
        # for t in trm:
        #     print(f'\t\t{t}, {trm[t]}')

        # Now given this map reserve resources for the top job
        top_job = self._queue[0]
        # print(f'\tTop job: {top_job.id} with resource requirement of {top_job.resources} for time {top_job.walltime}')
        trm = self.allocator.reserve_future(trm, top_job.id, top_job.resources, top_job.walltime)

        # print('\tTRM After top job:')
        # for t in trm:
        #     print(f'\t\t{t}, {trm[t]}')
        

        # Check if any job in the queue can be allocated using these resources now
        backfill_jobs: list[Job] = []
        for j in self._queue[1:]:
            # print(f'\t\t Trying job: {j.id} with resource requirement of {j.resources} for time {j.walltime}')

            # If a job has a pending run event, dont consider it for backfill
            if j.id in self._pending_run:
                continue


            can_backfill = True

            # This loop checks if the job can be backfilled
            # If not it sets can_backfill to False
            for t in trm:

                # Dont check beyond the walltime of the job
                if t > self.simulator.now() + j.walltime:
                    break

                resources = trm[t]
                # Not enough resources
                if len(resources) < j.resources:
                    can_backfill = False
                    break
            
            if can_backfill:
                # Add to jobs that can be backfilled
                backfill_jobs.append(j)

                # Update the time resource map
                trm = self.allocator.reserve_now(trm, j.id, j.resources, self.simulator.sim.now + j.walltime)

        # print('\tEligible:')
        # print(f'\t\t{[j.id for j in backfill_jobs]}')

        # print('Backfill:', [j.id for j in backfill_jobs])
        for job in backfill_jobs:
            # Try allocating resources
            resources = self.allocator.allocate(job.id, job.resources)

            # If no resources stop
            # Ensures strict ordering
            if not resources:
                print('Backfill Error: Job eligible but no resources!')
                exit()
            
            # Give the job its resources
            job.resource_ids = resources

            # Run the job
            self._pending_run.append(job.id)
            self.simulator.create_run_event(job.id)

        # print('#############')