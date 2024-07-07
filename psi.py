import json
import hashlib
import numpy as np
from typing import List, Dict, Set, Tuple

def read_json(file_path: str) -> List[Dict]:
    """Read JSON file and return its content."""
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def hash_attribute(attributes: Dict) -> Set[str]:
    """Hash attributes of events."""
    hashed_attributes = set()
    for key, value in attributes.items():
        hash_object = hashlib.sha256(f"{key}:{value}".encode())
        hashed_attributes.add(hash_object.hexdigest())
    return hashed_attributes

def find_intersection(hashes1: Set[str], hashes2: Set[str]) -> Set[str]:
    """Find the intersection of two sets of hashes."""
    return hashes1.intersection(hashes2)

def build_graph(events: List[Dict], psi: Set[str]) -> Tuple[List[str], np.ndarray]:
    """Build a graph based on events and their attributes found in PSI step with an edge matrix."""
    # Filter events that have attributes in psi
    filtered_events = []
    for event in events:
        event_attributes = hash_attribute(event['attributes'])
        if not event_attributes.isdisjoint(psi):
            filtered_events.append(event)
    
    # Create a list of event UUIDs
    event_uuids = [event['uuid'] for event in filtered_events]

    # Initialize the edge matrix (event_count x event_count) with zeros
    event_count = len(event_uuids)
    edge_matrix = np.zeros((event_count, event_count), dtype=int)
    
    # Fill the edge matrix based on common attributes
    for i in range(event_count):
        for j in range(i + 1, event_count):
            event_i_attributes = hash_attribute(filtered_events[i]['attributes'])
            event_j_attributes = hash_attribute(filtered_events[j]['attributes'])
            
            # Check for common attributes
            if not event_i_attributes.isdisjoint(event_j_attributes):
                edge_matrix[i, j] = 1
                edge_matrix[j, i] = 1
    edge_matrix_enc = generate_event_correlations((event_uuids, edge_matrix))

    return event_uuids, edge_matrix_enc

def generate_event_correlations(graph: Tuple[List[str], np.ndarray]) -> Set[str]:
    """
    Generate a set of hashed event correlations for a given edge matrix and corresponding UUIDs.
    
    Parameters:
    graph (Tuple[List[str], np.ndarray]): Tuple containing a list of UUIDs and a 2D edge matrix.
    
    Returns:
    Set[str]: A set of hashed event correlations.
    """
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
                
                # Generate the 128-bit hash using hashlib
                hash_object = hashlib.md5(correlation_string.encode())
                hash_value = hash_object.hexdigest()
                
                # Add the hashed value to the set of correlations
                correlations.add(hash_value)
                    
    return correlations

def compute_private_graph_intersection(correlations_p: Set[str], correlations_c: Set[str]) -> Set[str]:
    return correlations_p.intersection(correlations_c)


def main():
    # Paths to the JSON files
    file_path1 = 'producer.json'
    file_path2 = 'consumer.json'

    # Read JSON files
    events_p = read_json(file_path1)
    events_c = read_json(file_path2)

    # Compute PSI
    ps_p = set()
    for event in events_p:
        ps_p.update(hash_attribute(event['attributes']))

    ps_c = set()
    for event in events_c:
        ps_c.update(hash_attribute(event['attributes']))
 
    psi = find_intersection(ps_p, ps_c)
    # Build correlation graphs
    graph_p = build_graph(events_p, psi)
    graph_c = build_graph(events_c, psi)

    print(graph_p)
    print(graph_c)

if __name__ == "__main__":
    main()