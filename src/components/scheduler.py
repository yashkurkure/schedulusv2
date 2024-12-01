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
    node_ids: list[int] = None


class Scheduler:

    def __init__(self, schedulus, allocator: Allocator):
        self.schedulus = schedulus

        self._queue = []
        self._running = []
        pass
    

    def queue(self, job: Job):
        """
        Returns list of events that must be processed by the simulator.
        """
        self._queue.append(job)
        self._schedule()

    def _schedule(self):
        """
        Goes over the queue and tries to schedule jobs.
        """
        # print('Running scheduling cycle..')

        # print('Leaving scheduling cycle..')
        pass

    def _backfill_easy(self):
        """
        Tries to backfill jobs without delaying the 1st job in the queue.
        """
        pass