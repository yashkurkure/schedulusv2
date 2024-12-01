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
    nodes: int
    walltime: int

    # These may change
    state: JobState
    node_ids: list[int]



class Scheduler:

    def __init__(self, allocator: Allocator):
        self.queue = []
        self.running = []
        pass
    

    def events():
        """
        Returns list of events that must be processed by the simulator.
        """

    def _schedule():
        """
        Goes over the queue and tries to schedule jobs.
        """
        pass

    def _backfill_easy():
        """
        Tries to backfill jobs without delaying the 1st job in the queue.
        """
        pass