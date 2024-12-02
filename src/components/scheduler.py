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


class Scheduler:

    def __init__(self, schedulus):
        self.schedulus = schedulus
        self.allocator: Allocator = self.schedulus.allocator
        self._queue: list[Job] = []
        self._running: list[Job] = []
        self._finished: list[Job] = []
        pass
    


    def queue(self, job: Job):
        """
        Returns list of events that must be processed by the simulator.
        """

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
            
            # Try allocating resources
            resources = self.allocator.allocate(job.id, job.resources)

            # If no resources stop
            # Ensures strict ordering
            if not resources:
                break
            
            # Give the job its resources
            job.resource_ids = resources

            # Run the job
            self.schedulus.create_run_event(job.id)


        # Attempt to backfill if jobs are still in queue
        # NOTE: We backfill around the top 1 job, so the queue must have 2 jobs
        if len(self._queue) > 1:
            self._backfill_easy()

        # print('Leaving scheduling cycle..')
        pass

    def _backfill_easy(self):
        """
        Tries to backfill jobs without delaying the 1st job in the queue.
        """

        print('#### BACKFILL ####')
        # Build a map of time to resource ids
        # This map indicates the resources being freed up 
        # at each time entry in the map
        time_resource_map = {}
        # Add the resources available now to the time resource map
        cumulative = [resource.id for resource in self.allocator.get_available()]
        time_resource_map[self.schedulus.sim.now] = cumulative[:]
        # For each job add resources being freed up at it's end time
        print(f'Cumulative({len(cumulative)}):', cumulative)

        for j in self._running:
            end_time = self.schedulus.sim.now + j.walltime
            print()
            print(f'End time: {end_time}')
            resource_ids = j.resource_ids
            print(f'Freed up({len(resource_ids)}):', resource_ids)
            cumulative += resource_ids[:]
            print(f'Cumulative({len(cumulative)}):', cumulative)

            time_resource_map[end_time] = cumulative[:]


        print('\tTRM Initial:')
        for t in time_resource_map:
            print(f'\t\t{t}, {time_resource_map[t]}')

        # Now given this map reserve resources for the top job
        top_job = self._queue[0]
        time_resource_map = self.allocator.reserve_future( time_resource_map, top_job.id, top_job.resources)

        print('\tTRM After top job:')
        for t in time_resource_map:
            print(f'\t\t{t}, {time_resource_map[t]}')

        # Check if any job in the queue can be allocated using these resources now
        backfill_job_ids = []
        for j in self._queue[1:]:

            can_backfill = True

            # This loop checks if the job can be backfilled
            # If not it sets can_backfill to False
            for t in time_resource_map:
                resources = time_resource_map[t]
                # Not enough resources
                if len(resources) < j.resources:
                    can_backfill = False
                    break
            

            if can_backfill:
                # Add to jobs that can be backfilled
                backfill_job_ids.append(j.id)

                # Update the time resource map
                time_resource_map = self.allocator.reserve_now(time_resource_map, j.id, j.resources, self.schedulus.sim.now + j.walltime)


        
        print('\tEligible:')
        print(f'\t\t{backfill_job_ids}')
        print('#############')