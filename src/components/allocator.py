from dataclasses import dataclass
from enum import Enum
import random
from asynclogger import AsyncLogger

__metaclass__ = type

class ResourceState(Enum):
    BUSY = 1
    OFFLINE = 2
    AVAILABLE = 3


@dataclass
class Resource:

    # These stay the same
    id: int
    name: str
    cpus: int

    # These may change
    state: ResourceState
    job_id: int


class Allocator:
    def __init__(self, simulator, num_resources, log_dir):
        self.logger = AsyncLogger(f'{log_dir}/allocator.log')
        self.logger.write_log(f'Initialized Allocator')

        self.simulator = simulator

        self.resources: list[Resource] = []

        for i in range(0, num_resources):
            n = Resource(
                id = i,
                name = f'resource_{i}',
                cpus=1,
                state = ResourceState.AVAILABLE,
                job_id=-1
            )
            self.resources.append(n)

    
    def log(self, s):
        self.logger.write_log(f'{self.simulator.now()} {s}')


    def get_resource(self, resource_id) -> Resource:
        """
        Returns a resource given and id.
        """
        for n in self.resources:
            if n.id == resource_id:
                return n

    def get_available(self) -> list[Resource]:
        """
        Returns the available resource.
        """
        return [n for n in self.resources if n.state == ResourceState.AVAILABLE]
    
    def get_all_busy(self) -> list[Resource]:
        """
        Returns all busy resources.
        """
        return [n for n in self.resources if n.state == ResourceState.BUSY]
    
    def get_busy(self, job_id) -> list[Resource]:
        """
        Returns busy resources for some job.
        """
        return [n for n in self.resources if n.state == ResourceState.BUSY and n.job_id == job_id]
    
    def get_offline(self) -> list[Resource]:
        """
        Returns the offline resources.
        """
        return [n for n in self.resources if n.state == ResourceState.OFFLINE]


    def allocate(self, job_id, resources) -> list[int] | None:
        """
        Allocates a num_resources amount of resources to some job_id.
        Returns the ids of the resources allocated.
        """

        avaliable_resources = self.get_available()

        if resources > len(avaliable_resources):
            return None
        
        alloc_resources = random.sample(avaliable_resources, resources)

        for n in alloc_resources:
            n.state = ResourceState.BUSY
            n.job_id = job_id

        self.log(f'Job {job_id}: Allocated with {resources} resources.')
        return [n.id for n in alloc_resources]

    def deallocate(self, job_id) -> None:
        """
        Deallocates resources for some job_id.
        """

        dealloc_resources = self.get_busy(job_id)

        for n in dealloc_resources:
            n.state = ResourceState.AVAILABLE
            n.job_id = -1
        
        self.log(f'Job {job_id}: Deallocated {len(dealloc_resources)} resources.')


    def reserve_future(self, trm, job_id, resources, walltime) -> dict[int, int]:
        # print(f'\tReserve top job, {job_id}:')
        self.log(f'Job {job_id}: Trying to reserve {resources} resources for {walltime} in future.')

        reservation_time = -1
        # Iterate over the times when resources are getting freed up
        for t in trm:

            # Get the total resources available at this time
            resource_pool = [self.get_resource(resource_id) for resource_id in trm[t]]
            
            self.log(f'TRM: Time {t}; Available {len(resource_pool)}')

            # Once reourrces are available break
            if len(resource_pool) >= resources:
                reservation_time = t
                break

        self.log(f'Job {job_id}: Found reservation time of {reservation_time}.')


        # Only happens when a jobs are exceeding their walltime, or their end event has not occured
        if reservation_time == -1:
            return None

        # For the found reservation time get resources to reserve
        # NOTE: Cannot remove random ones because we want max of the same resource ids to be free at all times
        # reserved_resources = random.sample(time_resource_map[reservation_time], resources)
        # NOTE: Instead remove from the end of the list
        reserved_resources = trm[reservation_time][-resources:]
        end_time = reservation_time + walltime


        # print(f'\t\tReservation time: {reservation_time}')
        # print(f'\t\tReserved resources: {reserved_resources}')

        # Remove those resources from the time resource map
        for t in trm:
            # print(f'\t\tFor time: {t}')
            if reservation_time <= t and end_time >= t:
                self.log(f'Job {job_id}: Attempting to reserve at {t}. Avialable: {len(trm[t])}')
                for r in reserved_resources:
                    # print(f'\t\t\t\tRemoving: {r}')
                    trm[t].remove(r)

        # Return the updated time resource map
        return trm
    
    def reserve_now(self, trm, job_id, resources, walltime) -> dict[int, int]:
        self.log(f'Reserve Now: Job {job_id}: Trying to reserve {resources} resources for {walltime} starting now.')

        for t in trm:
            self.log(f'Resernve Now: Job {job_id}: Attempting to reserve {resources} resources for {walltime} starting now.')
            if t > self.simulator.now() + walltime:
                self.log(f'Reserve Now: Could not reserve')
                break

            reserved_resources = random.sample(trm[t], resources)
            # Remove those resources from the time resource map
            for r in reserved_resources:
                self.log(f'Reserve Now: Reserved {len(reserved_resources)} at {t}')
                trm[t].remove(r)

        return trm
        
    
    def resource_utilization(self):

        busy = len(self.get_all_busy())
        total = len(self.resources)

        utilization = busy/total

        return utilization