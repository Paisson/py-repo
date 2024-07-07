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

def build_graph(events: List[Dict], psi: Set[str]) -> Tuple[Dict[str, Set[str]], List[str]]:
    """Build a graph based on events and their attributes found in PSI step, and returns event map and UUIDs."""
    event_map = {}
    event_uuids = []

    for event in events:
        event_attributes = hash_attribute(event['attributes'])
        if not event_attributes.isdisjoint(psi):
            event_map[event['uuid']] = event_attributes
            event_uuids.append(event['uuid'])

    return event_map, event_uuids

def generate_event_correlations(uuids: List[str], event_map: Dict[str, Set[str]]) -> Set[str]:
    """Generate a set of hashed event correlations for corresponding UUIDs."""
    correlations = set()
    event_count = len(uuids)
    for i in range(event_count):
        for j in range(i + 1, event_count):
            uuid_i = uuids[i]
            uuid_j = uuids[j]

            if not event_map[uuid_i].isdisjoint(event_map[uuid_j]):
                sorted_uuids = sorted([uuid_i, uuid_j])
                correlation_string = f"{sorted_uuids[0]}:{sorted_uuids[1]}"
                hash_object = hashlib.md5(correlation_string.encode())
                hash_value = hash_object.hexdigest()
                correlations.add(hash_value)

    return correlations

def compute_graph_intersection(vertices_p: Dict[str, Set[str]], vertices_c: Dict[str, Set[str]]) -> Tuple[Set[str], Set[str]]:
    """Compute graph intersection of vertices and edges."""
    # Intersection of vertices by common attributes
    common_vertices = set()
    for v_p in vertices_p:
        for v_c in vertices_c:
            if not vertices_p[v_p].isdisjoint(vertices_c[v_c]):
                common_vertices.add(v_p)
                common_vertices.add(v_c)

    # Collect and hash the edges of the intersected vertices
    common_edges = set()
    cv_list = list(common_vertices)
    cv_count = len(cv_list)
    for i in range(cv_count):
        for j in range(i + 1, cv_count):
            cv_i = cv_list[i]
            cv_j = cv_list[j]
            if cv_i in vertices_p and cv_j in vertices_p:
                correlation_string = f"{min(cv_i, cv_j)}:{max(cv_i, cv_j)}"
                hash_object = hashlib.md5(correlation_string.encode())
                hash_value = hash_object.hexdigest()
                if hash_value in generate_event_correlations(list(vertices_p.keys()), vertices_p):
                    common_edges.add(hash_value)
            if cv_i in vertices_c and cv_j in vertices_c:
                correlation_string = f"{min(cv_i, cv_j)}:{max(cv_i, cv_j)}"
                hash_object = hashlib.md5(correlation_string.encode())
                hash_value = hash_object.hexdigest()
                if hash_value in generate_event_correlations(list(vertices_c.keys()), vertices_c):
                    common_edges.add(hash_value)

    return common_vertices, common_edges

def main():
    # Paths to the JSON files
    file_path1 = 'producer copy.json'
    file_path2 = 'consumer copy.json'

    events_p = read_json(file_path1)
    events_c = read_json(file_path2)

    # Compute PSI step
    ps_p = set()
    for event in events_p:
        ps_p.update(hash_attribute(event['attributes']))

    ps_c = set()
    for event in events_c:
        ps_c.update(hash_attribute(event['attributes']))

    psi = find_intersection(ps_p, ps_c)
    print("PSI (Private Set Intersection):", psi)
    if not psi:
        print("No common attributes found in PSI step.")
        return
    
    # Build correlation graphs
    vertices_p, graph_p_uuids = build_graph(events_p, psi)
    vertices_c, graph_c_uuids = build_graph(events_c, psi)
    
    print("Graph P UUIDs:", graph_p_uuids)
    print("Graph C UUIDs:", graph_c_uuids)

    common_vertices, common_edges = compute_graph_intersection(vertices_p, vertices_c)
    
    print("Common Vertices:", common_vertices)
    print("Common Edges (Encoded Correlations):", common_edges)

if __name__ == "__main__":
    main()