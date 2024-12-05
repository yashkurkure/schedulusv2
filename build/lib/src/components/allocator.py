from dataclasses import dataclass
from enum import Enum
import random


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
    def __init__(self, simulator, num_resources):
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
    
    def get_busy(self) -> list[Resource]:
        """
        Returns the available resources.
        """
        return [n for n in self.resources if n.state == ResourceState.BUSY]
    
    def get_busy(self, job_id) -> list[Resource]:
        """
        Returns the available resources.
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

        return [n.id for n in alloc_resources]

    def deallocate(self, job_id) -> None:
        """
        Deallocates resources for some job_id.
        """

        dealloc_resources = self.get_busy(job_id)

        for n in dealloc_resources:
            n.state = ResourceState.AVAILABLE
            n.job_id = -1


    def reserve_future(self, trm, job_id, resources, walltime) -> dict[int, int]:
        # print(f'\tReserve top job, {job_id}:')

        reservation_time = -1
        # Iterate over the times when resources are getting freed up
        for t in trm:

            # Get the total resources available at this time
            resource_pool = [self.get_resource(resource_id) for resource_id in trm[t]]
        
            # Once reourrces are available break
            if len(resource_pool) >= resources:
                reservation_time = t
                break

        # For the found reservation time get resources to reserve
        # NOTE: Cannot remove random ones because we want max of the same resources to be free at all times
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
                # print(f'\t\t\tResources: {time_resource_map[t]}')
                for r in reserved_resources:
                    # print(f'\t\t\t\tRemoving: {r}')
                    trm[t].remove(r)

        # Return the updated time resource map
        return trm
    
    def reserve_now(self, trm, job_id, resources, end) -> dict[int, int]:
        # print(f'\tReserve now, {job_id} with resources {resources}:')
        # print(f'\t\tUsing TRM:')
        # for t in trm:
        #     print(f'\t\t\t{t}: {trm[t]}')

        for t in trm:

            if t > end:
                break

            reserved_resources = random.sample(trm[t], resources)

            # Remove those resources from the time resource map
            for r in reserved_resources:
                trm[t].remove(r)


        return trm
        