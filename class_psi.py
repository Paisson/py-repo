import json
import hashlib
import numpy as np
from typing import List, Set, Tuple, Dict
from pybloom_live import BloomFilter

class Event:
    def __init__(self, id: str, uuid: str) -> None:
        self.id = id
        self.uuid = uuid
        self.attributes = []

    def add_attribute(self, attribute: 'Attributes') -> None:
        self.attributes.append(attribute)

    def get_attributes(self) -> List['Attributes']:
        return self.attributes

class Attributes:
    def __init__(self, type: str, value: str, uuid: str) -> None:
        self.type = type
        self.value = value
        self.uuid = uuid
        self.hash = self.hash_attribute(type, value)
        self.parents = []

    def add_event(self, event: Event) -> None:
        self.parents.append(event)

    @staticmethod
    def hash_attribute(type: str, value: str) -> str:
        """Hash a single attribute."""
        hash_object = hashlib.sha256(f"{type}:{value}".encode())
        return hash_object.hexdigest()

def read_json(file_path: str) -> List[Event]:
    """Read JSON file and return its events as Event objects."""
    with open(file_path, 'r') as file:
        data = json.load(file)['Event']

    events = []
    for event_dict in data:
        event = Event(id=event_dict['id'], uuid=event_dict['uuid'])
        for attr_dict in event_dict['Attribute']:
            attribute = Attributes(type=attr_dict['type'], value=attr_dict['value'], uuid=attr_dict['uuid'])
            event.add_attribute(attribute)
            attribute.add_event(event)
        events.append(event)

    return events

def find_intersection(hashes1: Set[str], hashes2: Set[str]) -> Set[str]:
    """Find the intersection of two sets of hashes."""
    return hashes1.intersection(hashes2)

def build_graph(events: List[Event], psi: Set[str]) -> Tuple[List[str], np.ndarray]:
    """Build a graph based on events and their attributes found in PSI step with an edge matrix."""
    # Filter events that have at least one attribute in psi
    filtered_events = [event for event in events if any(attr.hash in psi for attr in event.get_attributes())]

    # Extract UUIDs for the filtered events
    uuids = [event.uuid for event in filtered_events]

    # Initialize the edge matrix (event_count x event_count) with zeros
    event_count = len(filtered_events)
    edge_matrix = np.zeros((event_count, event_count), dtype=int)

    # Fill the edge matrix based on common attributes
    for i in range(event_count):
        for j in range(i + 1, event_count):
            event_i_attributes = {attr.hash for attr in filtered_events[i].get_attributes()}
            event_j_attributes = {attr.hash for attr in filtered_events[j].get_attributes()}

            # Check for common attributes
            if not event_i_attributes.isdisjoint(event_j_attributes):
                edge_matrix[i, j] = 1
                edge_matrix[j, i] = 1

    edge_matrix_enc = generate_event_correlations((uuids, edge_matrix))

    return filtered_events, edge_matrix_enc

def generate_event_correlations(graph: Tuple[List[str], np.ndarray]) -> Set[str]:
    """Generate a set of hashed event correlations for a given edge matrix and corresponding UUIDs."""
    uuids, matrix = graph
    correlations = set()

    # Iterate through the upper triangle of the matrix to avoid duplicate pairs
    for i in range(len(matrix)):
        for j in range(i + 1, len(matrix)):
            if matrix[i][j] == 1:
                uuid_i = uuids[i]
                uuid_j = uuids[j]

                # Create the string to hash
                correlation_string = f"{uuid_i}:{uuid_j}"
                print(f'Events that correlate: {uuid_i}:{uuid_j}')
                # Generate the 128-bit hash using hashlib
                hash_object = hashlib.md5(correlation_string.encode())
                hash_value = hash_object.hexdigest()

                # Add the hashed value to the set of correlations
                correlations.add(hash_value)

    return correlations

def compute_private_graph_intersection(correlations_p: Set[str], correlations_c: Set[str]) -> Set[str]:
    return correlations_p.intersection(correlations_c)

def build_bloom_filter_from_psi(
    psi1: Set[str], 
    psi2: Set[str], 
    events: List[Event], 
    capacity: int, 
    error_rate: float
) -> BloomFilter:
    bf = BloomFilter(capacity=capacity, error_rate=error_rate)  # Initialize bloom filter with capacity and error rate.

    for event in events:
        for attribute in event.get_attributes():
            if attribute.hash in psi1 and attribute.hash not in psi2:
                correlation_string = f"{attribute.type}:{attribute.value}:{event.uuid}"
                hash_value = hashlib.md5(correlation_string.encode()).hexdigest()
                bf.add(hash_value)

    return bf

def find_new_correlations(
    bloom_filter: BloomFilter, 
    psi1: Set[str], 
    events: List[Event]
) -> List[Tuple[str, str, str]]:
    new_correlations = []

    # First, gather existing correlations from consumer's events
    existing_correlations = set()
    for event in events:
        for attribute in event.get_attributes():
            correlation_string = f"{attribute.type}:{attribute.value}:{event.uuid}"
            hash_value = hashlib.md5(correlation_string.encode()).hexdigest()
            existing_correlations.add(hash_value) 

    # Create a list of all consumer attributes that are in psi1
    consumer_attributes_in_psi1 = [attr for event in events for attr in event.get_attributes() if attr.hash in psi1]

    # Iterate through the consumer's events and check for each attribute in psi1
    for attribute in consumer_attributes_in_psi1:
        for event in events:
            correlation_string = f"{attribute.type}:{attribute.value}:{event.uuid}"
            hash_value = hashlib.md5(correlation_string.encode()).hexdigest()
            if hash_value in bloom_filter and hash_value not in existing_correlations:
                print(f'Correlation {attribute.type}:{attribute.value}:{event.uuid} in Bloomfilter')
                new_correlations.append((attribute.type, attribute.value, event.uuid))

    return new_correlations

def compute_psi3(events_p: List[Event], events_c: List[Event]) -> Set[str]:
    """Compute the PSI3 for non-shared events' UUIDs."""
    uuid_set_p = {event.uuid for event in events_p}
    uuid_set_c = {event.uuid for event in events_c}

    # Compute PSI3
    psi3 = uuid_set_p.intersection(uuid_set_c)

    return psi3

def build_bloom_filter_for_non_shared_psi(
    psi1: Set[str],
    psi3: Set[str],
    events: List[Event],
    capacity: int,
    error_rate: float
) -> BloomFilter:
    """Build a Bloom filter for correlated events with non-shared attributes not in PSI1."""
    bf = BloomFilter(capacity=capacity, error_rate=error_rate)

    # Go through each event in the producer's set
    for event in events:
        if event.uuid in psi3:  # Check if event is part of psi3
            attributes_not_in_psi1 = {attr.hash for attr in event.get_attributes() if attr.hash not in psi1}
            
            for other_event in events:
                if other_event.uuid != event.uuid and other_event.uuid in psi3:  # Ensure different event and part of psi3
                    other_attributes_not_in_psi1 = {attr.hash for attr in other_event.get_attributes() if attr.hash not in psi1}

                    # Check for common attributes between event and other_event, both not shared in psi1
                    common_attributes = attributes_not_in_psi1.intersection(other_attributes_not_in_psi1)
                    if common_attributes:
                        correlation_string = f"{event.uuid}:{other_event.uuid}"
                        hash_value = hashlib.md5(correlation_string.encode()).hexdigest()
                        bf.add(hash_value)

    return bf

def find_new_correlations_for_psi3(
    bloom_filter: BloomFilter, 
    psi3: Set[str], 
    events: List[Event]
) -> List[Tuple[str, str]]:
    """Find new correlations by computing the pairwise hashes for event UUIDs in psi3 and checking against the Bloom filter."""
    new_correlations = []

    # Get the events that are in psi3
    events_in_psi3 = [event for event in events if event.uuid in psi3]

    # Check all pairs of events within psi3
    for i in range(len(events_in_psi3)):
        for j in range(i + 1, len(events_in_psi3)):
            event_i = events_in_psi3[i]
            event_j = events_in_psi3[j]
            
            correlation_string = f"{event_i.uuid}:{event_j.uuid}"
            hash_value = hashlib.md5(correlation_string.encode()).hexdigest()
            
            if hash_value in bloom_filter:
                new_correlations.append((event_i.uuid, event_j.uuid))

    return new_correlations

def main():
    # Paths to the JSON files
    file_path1 = 'producer copy.json'
    file_path2 = 'consumer copy.json'

    # Read JSON files
    events_p = read_json(file_path1)
    events_c = read_json(file_path2)

    # Compute PSI
    ps_p = {attr.hash for event in events_p for attr in event.get_attributes()}
    ps_c = {attr.hash for event in events_c for attr in event.get_attributes()}
    psi1 = find_intersection(ps_p, ps_c)

    # Build correlation graphs
    graph_p = build_graph(events_p, psi1)
    graph_c = build_graph(events_c, psi1)

    print("Producer Graph Correlations:")
    print(graph_p[1])

    print("Consumer Graph Correlations:")
    print(graph_c[1])

    psi2 = compute_private_graph_intersection(graph_p[1], graph_c[1])
    print("Common Correlations:")
    print(psi2)

    # Create and fill the Bloom filter with the appropriate attributes
    bloom_filter = build_bloom_filter_from_psi(psi1, psi2, events_p, capacity=len(ps_p) * 10, error_rate=0.01)
    print("Number of Attribtues in BloomFilter:")
    print(len(bloom_filter))

    # Find new correlations by iterating through the consumer's confidential attributes
    new_correlations = find_new_correlations(bloom_filter, psi1, events_c)

    print("New Correlations Discovered:")
    for attribute_type, attribute_value, event_uuid in new_correlations:
        print(f"Attribute Type: {attribute_type}, Attribute Value: {attribute_value}, Event UUID: {event_uuid}")
        

    psi3 = compute_psi3(events_p, events_c)
    # Build bloom filter for non-shared PSI
    non_shared_bloom_filter = build_bloom_filter_for_non_shared_psi(psi1, psi3, events_p, capacity=len(ps_p) * 10, error_rate=0.01)
    print("Number of Attributes in Non-Shared Bloom Filter:")
    print(len(non_shared_bloom_filter))
    
    # Find new correlations from the non-shared PSI Bloom filter
    new_non_shared_correlations = find_new_correlations_for_psi3(non_shared_bloom_filter, psi3, events_c)

    print("New Non-Shared Correlations Discovered:")
    for uuid_i, uuid_j in new_non_shared_correlations:
        print(f"Event UUID1: {uuid_i}, Event UUID2: {uuid_j}")


if __name__ == "__main__":
    main()