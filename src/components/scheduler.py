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

            # self.schedulus.handle_scheduler_event(
            #     SchedulerEvent(
            #         time=self.sim.now,
            #         type=EventType.Sched.START,
            #         job_id=job.id
            #     )
            # )

        # Attempt to backfill
        self._backfill_easy()

        # print('Leaving scheduling cycle..')
        pass

    def _backfill_easy(self):
        """
        Tries to backfill jobs without delaying the 1st job in the queue.
        """
        pass