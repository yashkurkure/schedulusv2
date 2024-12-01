from dataclasses import dataclass
from enum import Enum
import random


__metaclass__ = type

class NodeState(Enum):
    BUSY = 1
    OFFLINE = 2
    AVAILABLE = 3


@dataclass
class Node:

    # These stay the same
    id: int
    name: str
    cpus: int

    # These may change
    state: NodeState
    job_id: int


class Allocator:
    def __init__(self, schedulus, num_nodes):
        self.schedulus = schedulus

        self.nodes: list[Node] = []

        for i in range(0, num_nodes):
            n = Node(
                id = i,
                name = f'node{i}',
                cpus=1,
                state = NodeState.AVAILABLE,
                job_id=-1
            )
            self.nodes.append(n)

    def get_available(self) -> list[Node]:
        """
        Returns the available nodes.
        """
        return [n for n in self.nodes if n.state == NodeState.AVAILABLE]
    
    def get_busy(self) -> list[Node]:
        """
        Returns the available nodes.
        """
        return [n for n in self.nodes if n.state == NodeState.BUSY]
    
    def get_busy(self, job_id) -> list[Node]:
        """
        Returns the available nodes.
        """
        return [n for n in self.nodes if n.state == NodeState.BUSY and n.job_id == job_id]
    
    def get_offline(self) -> list[Node]:
        """
        Returns the offline nodes.
        """
        return [n for n in self.nodes if n.state == NodeState.OFFLINE]


    def allocate(self, job_id, num_nodes) -> list[Node] | None:
        """
        Allocates a num_nodes amount of nodes to some job_id.
        """
        avaliable_nodes = self.get_available()

        if len(avaliable_nodes) > num_nodes:
            return None
        
        alloc_nodes = random.sample(avaliable_nodes, num_nodes)

        for n in alloc_nodes:
            n.state = NodeState.BUSY
            n.job_id = job_id

        return alloc_nodes

    def deallocate(self, job_id) -> None:
        """
        Deallocates nodes for some job_id.
        """

        dealloc_nodes = self.get_busy(job_id)

        for n in dealloc_nodes:
            n.state = NodeState.AVAILABLE
            n.job_id = -1

    def events():
        """
        Returns a list of events that must be processed by simulator.
        """
        pass