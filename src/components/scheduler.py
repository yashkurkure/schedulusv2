from dataclasses import dataclass
from enum import Enum
import random

from components.allocator import Allocator
from asynclogger import AsyncLogger
import time
import copy

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
    runtime: int

    # These may change
    state: JobState = JobState.WAITING
    resource_ids: list[int] = None

    # Result fields
    res_submit_ts = -1
    res_run_ts = -1
    res_end_ts = -1


class Scheduler:

    def __init__(self, simulator, log_dir):
        self.logger = AsyncLogger(f'{log_dir}/scheduler.log')
        self.logger.write_log(f'Initialized Scheduler')
        
        self.simulator = simulator
        self.allocator: Allocator = self.simulator.allocator
        self._queue: list[Job] = []
        self._scheduled: list[int] = []
        self._running: list[Job] = []
        self._finished: list[Job] = []

        pass

    def log(self, s):
        self.logger.write_log(f'{self.simulator.now()} {s}')
    
    def queue(self, job: Job):
        """
        Queues a job
        """
        self.log(f'Queue: {job.id} with resource requirement of {job.resources} for time {job.walltime}')

        job.res_submit_ts = self.simulator.now()

        # Add the job to the queue
        self._queue.append(job)

        # Run a scheduling cycle
        self._schedule()

    
    def start(self, job_id):
        # Look for the job in queued jobs
        job: Job = None
        for j in self._scheduled:
            if j.id == job_id:
                job = j
                break
        if job == None:
            print('Start Error: Job not found in queue')
            raise LookupError

        # Update the state of the job
        job.res_run_ts = self.simulator.now()
        job.state = JobState.RUNNING
        if job.resource_ids is None:
            print('Resource ids were none')

        # Remove from scheduled
        self._scheduled.remove(job)

        # Add to list of running jobs
        self._running.append(job)

        self.log(f'Start: {job.id} with resource requirement of {job.resources} for time {job.walltime}')


    def end(self, job_id):

        # Look for the job in running jobs
        job = None
        for j in self._running:
            if j.id == job_id:
                job = j
                break
        if job == None:
            print('End Error: Job not found in running job')
            raise LookupError
        
        # Update the state of the job
        job.res_end_ts = self.simulator.now()
        job.state = JobState.FINISHED

        # Deallocate the resources for the job
        self.allocator.deallocate(job.id)

        # Remove from list of running jobs
        self._running.remove(job)

        # Add to list of finished jobs
        self._finished.append(job)

        self.log(f'End: {job.id} with resource requirement of {job.resources} for time {job.walltime}')

        # Run a scheduling cycle
        self._schedule()



    def _schedule(self):
        """
        Goes over the queue and tries to schedule jobs.
        """

        # TODO: Build a new queue evertime
        # This would get rid of _pending_run list

        t = time.time()
        self.log(f'Entered scheduling cycle...')
        # Try and schedule jobs in the head of the queue
        can_schedule: list[Job] = []
        for job in self._queue:

            self.log(f'Considering job {job.id} with resources {job.resources} for {job.walltime}.')

            # Try allocating resources
            resource_ids = self.allocator.allocate(job.id, job.resources)

            # If no resources stop
            # Ensures strict ordering
            if not resource_ids:
                self.log(f'Can not schedule')
                break

            self.log(f'Can schedule')
            job.resource_ids = resource_ids
            can_schedule.append(job)
        
        # Schedule run events
        for job in can_schedule:

            # Remove from job from queued jobs
            self._queue.remove(job)

            # Schedule the run event
            job.res_run_ts = self.simulator.create_run_event(job.id)
            self._scheduled.append(job)


        # Attempt to backfill if jobs are still in queue
        # NOTE: We backfill around the top 1 job, so the queue must have 2 jobs
        if len(self._queue) > 0:
            self._backfill_easy()

        self.log(f'Leaving scheduling cycle...')
        t_cycle = time.time() - t
        self.log(f'Cycle took {t_cycle} seconds.')
        pass

    def _build_time_resource_map(self):
        # print('Building time resrouce map:')

        # Sort the jobs by their end times
        running_jobs = sorted(self._running, key=lambda x: x.res_run_ts + x.walltime)
        for j in running_jobs:
            self.log(f'Build TRM: Running jobs: {j}')

        trm = {}
        cumulative = [resource.id for resource in self.allocator.get_available()]
        trm[self.simulator.now()] = cumulative[:]
        # print(f'\t\tNow {self.simulator.now()}, {trm[self.simulator.now()]}')
        self.log(f'Buil TRM: At {self.simulator.now()} Available {len(cumulative[:])}.')
        for j in running_jobs:
            end_time = j.res_run_ts + j.walltime
            resource_ids = j.resource_ids


            # NOTE: If any running jobs are exceeding their walltime, the scheduler does not know
            # when it ends, exclude it from the map
            if end_time < self.simulator.now():
                self.log(f'Build TRM: Job {j.id} was supposed to end at {end_time}. It is exceeding walltime!')
                continue

            # NOTE: If a job is ending now, and its end event hasnt occured, it must 
            # removed from the running jobs list to build the time resource map
            if end_time == self.simulator.now():
                self.log(f'Build TRM: Job {j.id} ends at {end_time}. End event has not been processed!')
                continue

            # print(f'\t\tJob: {j.id}, {end_time}, {resource_ids}')

            cumulative += resource_ids[:]

            if end_time in trm:
                trm[end_time] += resource_ids[:]
            else:
                trm[end_time] = cumulative[:]
            
            self.log(f'Build TRM: Job {j.id} ends at {end_time}. Available {len(trm[end_time])}.')

        return dict(sorted(trm.items()))

    def _backfill_easy(self):
        """
        Tries to backfill jobs without delaying the 1st job in the queue.
        """
        self.log(f'Entered backfill..')
        
        # Get the top job
        top_job = self._queue[0]
        
        self.log(f'Top Job: {top_job.id} with resource requirement of {top_job.resources} for time {top_job.walltime}')

        trm = self._build_time_resource_map()


        # Now given this map reserve resources for the top job
        trm = self.allocator.reserve_future(trm, top_job.id, top_job.resources, top_job.walltime)

        if trm is None:
            self.log(f'Skipped backfilling because TRM was None')
            return
        

        # Check if any job in the queue can be allocated using these resources now
        backfill_jobs: list[Job] = []
        for j in self._queue[1:]:
            self.log(f'Backfill Job: {j.id} with resource requirement of {top_job.resources} for time {top_job.walltime}')
            # # If a job has a pending run event, dont consider it for backfill
            # if j.id in self._pending_run:
            #     continue

            can_backfill = True

            # This loop checks if the job can be backfilled
            # If not it sets can_backfill to False
            for t in trm:
                resources = trm[t]
                self.log(f'Backfill Job: Resources Available {len(resources)}; Resources Required {j.resources}')

                # Dont check beyond the walltime of the job
                if t > self.simulator.now() + j.walltime:
                    break

                # Not enough resources
                if len(resources) < j.resources:
                    can_backfill = False
                    self.log(f'Backfill Job: {j.id}; Cannot backfill')
                    break
            
            if can_backfill:
                # Add to jobs that can be backfilled
                self.log(f'Backfill Job: {j.id}; Backfill Eligible, reserving resources.')
                backfill_jobs.append(j)

                # Update the time resource map
                trm = self.allocator.reserve_now(trm, j.id, j.resources, j.walltime)

        # print('\tEligible:')
        # print(f'\t\t{[j.id for j in backfill_jobs]}')

        # print('Backfill:', [j.id for j in backfill_jobs])
        for job in backfill_jobs:
            self.log(f'Backfill Job Allocate: {j.id} with resource requirement of {top_job.resources} for time {top_job.walltime}')
            # Try allocating resources
            resources = self.allocator.allocate(job.id, job.resources)

            # If no resources stop
            # Ensures strict ordering
            if not resources:
                print('Backfill Error: Job eligible but no resources!')
                raise LookupError
            
            # Remove from job from queued jobs
            self._queue.remove(job)

            # Give the job its resources
            job.resource_ids = resources

            # Schedule the run event
            job.res_run_ts = self.simulator.create_run_event(job.id)
            self._scheduled.append(job)


    def average_wait_time(self):

        total_wait = 0
        job_count = 0
        for job in self._finished:
            wait = job.res_run_ts - job.res_submit_ts
            total_wait += wait
            job_count += 1

        for job in self._running:
            wait = job.res_run_ts - job.res_submit_ts
            total_wait += wait
            job_count += 1

        if job_count == 0:
            return 0
        

        avg_wait = total_wait/job_count

        return avg_wait
