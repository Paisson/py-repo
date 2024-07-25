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
    events: List[Event],
    hash_to_attr_map: Dict[str, Attributes]
) -> List[Tuple[str, Attributes]]:
    new_correlations = []
    for event in events:
        for attribute in event.get_attributes():
            if attribute.hash in psi1:
                correlation_string = f"{attribute.type}:{attribute.value}:{event.uuid}"
                hash_value = hashlib.md5(correlation_string.encode()).hexdigest()
                if hash_value in bloom_filter:
                    if hash_value not in hash_to_attr_map:
                        hash_to_attr_map[hash_value] = attribute
                    new_correlations.append((hash_value, hash_to_attr_map[hash_value]))
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
    print("Bloom Filter with Attributes from PSI1 not in PSI2:")

    # Hash to attribute map for the consumer
    consumer_hash_to_attr_map = {}
    
    # Find new correlations by iterating through the consumer's confidential attributes
    new_correlations = find_new_correlations(bloom_filter, psi1, events_c, consumer_hash_to_attr_map)

    print("New Correlations Discovered:")
    for hash_value, attribute in new_correlations:
        print(f"Hash: {hash_value}, Attribute: {attribute.type}, Value: {attribute.value}, UUID: {attribute.uuid}")

    # Consumer can access its own hash-to-attribute map
    for hash_value, attribute in new_correlations:
        if hash_value in consumer_hash_to_attr_map:
            attribute = consumer_hash_to_attr_map[hash_value]
            print(f"Details for Hash {hash_value} - Attribute: {attribute.type}, Value: {attribute.value}, UUIDs: {[e.uuid for e in attribute.parents]}")

if __name__ == "__main__":
    main()