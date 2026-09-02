"""
PageRank implemented on top of the single-pass mapreduce_framework.

Design follows DESIGN.md. The framework only does one pass (Map -> Shuffle ->
Reduce), so the iteration loop and the dangling-node mass redistribution live
in this file, outside mapreduce().

Data shape carried between iterations:
    (node_id, (rank, [neighbor1, neighbor2, ...]))
"""

from collections import defaultdict
from typing import Dict, List, Tuple

from mapreduce_framework import mapreduce, print_results


# ---------------------------------------------------------------------------
# Graph I/O
# ---------------------------------------------------------------------------

def load_graph(path: str) -> Dict[str, List[str]]:
    """Parse a web graph file.

    Format: "node: neighbor1 neighbor2 ..."
    A line like "E:" (no neighbors) is a dangling node.
    Lines starting with '#' and blank lines are ignored.
    """
    graph: Dict[str, List[str]] = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            head, _, rest = line.partition(":")
            node = head.strip()
            neighbors = rest.split()
            graph[node] = neighbors
    return graph


def initial_state(graph: Dict[str, List[str]]) -> Dict[str, Tuple[float, List[str]]]:
    """Every node starts with rank = 1/N, preserving adjacency."""
    n = len(graph)
    init_rank = 1.0 / n
    return {node: (init_rank, list(neighbors)) for node, neighbors in graph.items()}


# ---------------------------------------------------------------------------
# MapReduce phase
# ---------------------------------------------------------------------------

# The state is a dict {node: (rank, [neighbors])}. Each value is one "item"
# passed to the mapper, matching the framework's "one item at a time" contract.

def pr_mapper(item: Tuple[str, Tuple[float, List[str]]]):
    """Yield one STRUCT message plus one RANK message per out-neighbor.

    The KEY is the node id (so the framework's shuffle groups everything
    that targets a given node together). The tag (STRUCT / RANK) is carried
    inside the value tuple.

    STRUCT -> keeps the adjacency list alive across iterations.
    RANK   -> the fraction of this node's rank that flows to a neighbor.
    """
    node, (rank, neighbors) = item

    # Always emit the structure so the reducer can reattach the adjacency list.
    yield (node, ("STRUCT", list(neighbors)))

    # Dangling nodes emit no RANK messages; their mass is handled outside.
    out_degree = len(neighbors)
    if out_degree == 0:
        return

    share = rank / out_degree
    for nbr in neighbors:
        yield (nbr, ("RANK", share))


def pr_reducer(key: str, values: list) -> Tuple[float, List[str]]:
    """Combine STRUCT + RANK messages for one node into its new state.

    `key` is the node id. `values` is a list of either:
        ("STRUCT", [neighbors])  -> exactly 1 per node
        ("RANK",   share)        -> 0..* per node, one per inbound link
    """
    adjacency: List[str] = []
    incoming: float = 0.0

    for tag, payload in values:
        if tag == "STRUCT":
            adjacency = list(payload)
        elif tag == "RANK":
            incoming += float(payload)
        # Unknown tags are ignored on purpose (forward-compatible).

    # The pure-RANK part of the update. The dangling mass + damping base are
    # applied by the iteration driver, which has access to N and dangling_mass.
    new_rank = incoming
    return (new_rank, adjacency)


# ---------------------------------------------------------------------------
# Iteration driver
# ---------------------------------------------------------------------------

def run_pagerank(
    graph: Dict[str, List[str]],
    d: float = 0.85,
    max_iter: int = 50,
    epsilon: float = 1e-6,
    verbose: bool = False,
) -> Tuple[Dict[str, float], int, List[float]]:
    """Run PageRank on `graph` until L1 change < epsilon or max_iter reached.

    Returns
    -------
    ranks        : {node: final_rank}
    iters_used   : number of iterations actually run
    l1_history   : L1 delta per iteration (for analysis)
    """
    n = len(graph)
    state: Dict[str, Tuple[float, List[str]]] = initial_state(graph)

    l1_history: List[float] = []
    iters_used = 0

    for it in range(1, max_iter + 1):
        iters_used = it

        # Dangling mass: rank held by nodes with no outlinks. It is NOT lost;
        # in the next iteration it is redistributed uniformly to all nodes.
        dangling_mass = sum(rank for rank, neighbors in state.values() if not neighbors)

        # The mapper only distributes rank along edges. The reducer returns the
        # raw incoming sum; the damping base + dangling redistribution are
        # folded in here, after mapreduce(), so they live in the driver.
        raw = mapreduce(list(state.items()), pr_mapper, pr_reducer)

        new_state: Dict[str, Tuple[float, List[str]]] = {}
        for node, (rank, neighbors) in state.items():
            incoming = raw.get(node, (0.0, neighbors))[0]
            new_rank = (1.0 - d) / n + d * (incoming + dangling_mass / n)
            new_state[node] = (new_rank, neighbors)

        # Convergence check: L1 norm of the rank delta.
        l1 = sum(abs(new_state[k][0] - state[k][0]) for k in state)
        l1_history.append(l1)

        if verbose:
            print(f"iter {it:>3}  L1={l1:.3e}  sum_ranks={sum(r for r,_ in new_state.values()):.6f}")

        state = new_state
        if l1 < epsilon:
            break

    ranks = {node: rank for node, (rank, _neighbors) in state.items()}
    return ranks, iters_used, l1_history


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def top_k(ranks: Dict[str, float], k: int = 15) -> List[Tuple[str, float]]:
    return sorted(ranks.items(), key=lambda kv: kv[1], reverse=True)[:k]


def in_degree(graph: Dict[str, List[str]]) -> Dict[str, int]:
    deg: Dict[str, int] = {node: 0 for node in graph}
    for _src, nbrs in graph.items():
        for n in nbrs:
            deg[n] = deg.get(n, 0) + 1
    return deg


if __name__ == "__main__":
    import sys
    import time

    path = sys.argv[1] if len(sys.argv) > 1 else "web_graph_sample.txt"
    graph = load_graph(path)
    t0 = time.time()
    ranks, iters, history = run_pagerank(graph, d=0.85, max_iter=50, epsilon=1e-6, verbose=True)
    elapsed = time.time() - t0

    print_results(ranks, title=f"PageRank on {path}", limit=15)
    print(f"iterations: {iters}")
    print(f"sum of ranks: {sum(ranks.values()):.10f}")
    print(f"elapsed: {elapsed:.3f}s")
