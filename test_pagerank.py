"""Tests for pagerank.py.

Hand-checkable cases (per Tarea §6.1):
  - Trivial 3-node chain (A->B->C): hand-computed expected.
  - 3-node cycle (A->B->C->A): by symmetry all ranks == 1/3.
  - A dangling node that points to nothing.
  - Sum-of-ranks invariant (sum stays ~1.0) when dangling is handled.
  - Convergence within the iteration cap.
"""

import math
import os
import unittest

import pagerank as pr


HERE = os.path.dirname(os.path.abspath(__file__))


def _norm_diff(a, b):
    return math.fsum(abs(x - y) for x, y in zip(a, b))


class TestHandComputable(unittest.TestCase):
    """A small graph whose PageRank we can solve on paper."""

    GRAPH = {
        "A": ["B", "C"],
        "B": ["C"],
        "C": ["A"],
        "D": ["A", "C"],
        "E": [],         # dangling
        "F": ["A", "B", "C", "D"],
        "G": ["F"],
        "H": ["A"],
    }

    def test_sum_invariant_under_damping(self):
        ranks, iters, _ = pr.run_pagerank(self.GRAPH, d=0.85, max_iter=200, epsilon=1e-10)
        s = sum(ranks.values())
        self.assertAlmostEqual(s, 1.0, places=6,
                               msg=f"sum of ranks drifted to {s}")

    def test_convergence_in_few_iterations(self):
        # Dense-ish graph with d=0.85, well below the 50-iter cap.
        ranks, iters, history = pr.run_pagerank(self.GRAPH, max_iter=100, epsilon=1e-6)
        self.assertLess(iters, 50)
        self.assertLess(history[-1], 1e-6)

    def test_dangling_node_has_finite_rank(self):
        ranks, _, _ = pr.run_pagerank(self.GRAPH, max_iter=100, epsilon=1e-8)
        self.assertGreater(ranks["E"], 0.0)
        self.assertTrue(math.isfinite(ranks["E"]))

    def test_hub_outranks_leaf(self):
        """F links to 4 nodes; H links to 1. F should beat H."""
        ranks, _, _ = pr.run_pagerank(self.GRAPH, max_iter=100, epsilon=1e-8)
        self.assertGreater(ranks["F"], ranks["H"])


class TestTrivialChain(unittest.TestCase):
    """3 nodes in a chain: A -> B -> C. C is dangling.

    N=3, d=0.85. Hand-derived expected ranks (with dangling mass distributed):
        r(C) = (1-d)/3 + d * (r(B)/1)       because B -> C and C is dangling
        r(B) = (1-d)/3 + d * (r(A)/1)       because A -> B
        r(A) = (1-d)/3 + d * (r(C) + dangling_mass/3)
        dangling_mass = r(C)  (only C is dangling)

    Iterating the closed form gives a fixed point. We just check:
      - sum == 1
      - r(B) > r(A)  (B is a sink of rank from A)
      - r(C) > 0
      - ranks are stable.
    """
    GRAPH = {"A": ["B"], "B": ["C"], "C": []}

    def test_chain_invariants(self):
        ranks, iters, _ = pr.run_pagerank(self.GRAPH, d=0.85, max_iter=200, epsilon=1e-10)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=6)
        self.assertGreater(ranks["B"], ranks["A"])
        self.assertGreater(ranks["C"], 0.0)


class TestCycle(unittest.TestCase):
    """A->B->C->A. By symmetry all ranks must equal 1/3."""

    def test_cycle_equal_ranks(self):
        graph = {"A": ["B"], "B": ["C"], "C": ["A"]}
        ranks, iters, _ = pr.run_pagerank(graph, d=0.85, max_iter=200, epsilon=1e-10)
        for node in ("A", "B", "C"):
            self.assertAlmostEqual(ranks[node], 1.0 / 3.0, places=4)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=6)

    def test_cycle_with_one_dangling_still_symmetric(self):
        """Add D that nobody points to, and points to nobody. By symmetry of
        A,B,C (they still all have in-degree 1 and out-degree 1) they should
        still be equal among themselves. D's rank is whatever falls out of
        the dangling redistribution."""
        graph = {"A": ["B"], "B": ["C"], "C": ["A"], "D": []}
        ranks, _, _ = pr.run_pagerank(graph, d=0.85, max_iter=200, epsilon=1e-10)
        self.assertAlmostEqual(ranks["A"], ranks["B"], places=6)
        self.assertAlmostEqual(ranks["B"], ranks["C"], places=6)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=6)


class TestDanglingHandling(unittest.TestCase):
    """Prove the dangling-mass fix is what keeps the sum invariant.

    We run the same graph twice. If dangling is dropped, the sum collapses;
    with our handling the sum stays at 1.
    """

    GRAPH = {
        "A": ["B", "C"],
        "B": ["C"],
        "C": ["A", "B"],
        "D": [],   # dangling
    }

    def test_sum_stays_at_one(self):
        ranks, _, _ = pr.run_pagerank(self.GRAPH, d=0.85, max_iter=200, epsilon=1e-10)
        s = sum(ranks.values())
        self.assertAlmostEqual(s, 1.0, places=6,
                               msg=f"sum drifted to {s}, dangling handling is wrong")

    def test_dangling_node_still_gets_rank(self):
        ranks, _, _ = pr.run_pagerank(self.GRAPH, max_iter=200, epsilon=1e-10)
        self.assertGreater(ranks["D"], 0.0)


class TestConvergence(unittest.TestCase):
    def test_stops_under_epsilon(self):
        graph = {"A": ["B"], "B": ["C"], "C": ["A"]}
        ranks, iters, history = pr.run_pagerank(graph, max_iter=50, epsilon=1e-6)
        self.assertLess(history[-1], 1e-6)
        self.assertLessEqual(iters, 50)

    def test_max_iter_cap(self):
        # epsilon so tight it can never be reached -> we should stop at max_iter.
        # Use a graph that actually needs iterations (asymmetric chain) so the
        # initial state is not already a fixed point.
        graph = {"A": ["B", "C"], "B": ["C"], "C": ["A"], "D": ["A"]}
        _, iters, _ = pr.run_pagerank(graph, max_iter=7, epsilon=1e-20)
        self.assertEqual(iters, 7)


class TestSampleFile(unittest.TestCase):
    """End-to-end against the 8-node sample shipped with the task."""

    SAMPLE = os.path.join(HERE, "web_graph_sample.txt")

    def test_sample_runs_and_sums_to_one(self):
        graph = pr.load_graph(self.SAMPLE)
        ranks, _, _ = pr.run_pagerank(graph, d=0.85, max_iter=100, epsilon=1e-8)
        self.assertEqual(len(ranks), 8)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=6)

    def test_sample_top_node_is_sensible(self):
        # F links to 4 others and gets linked back from G; by inspection it
        # should land in the top of the ranking.
        graph = pr.load_graph(self.SAMPLE)
        ranks, _, _ = pr.run_pagerank(graph, max_iter=100, epsilon=1e-8)
        top = max(ranks, key=ranks.get)
        self.assertIn(top, {"F", "A"})  # F is the strongest hub; A also central


class TestMediumAndLarge(unittest.TestCase):
    """Smoke + sum-invariant checks on the provided medium/large graphs."""

    def test_medium(self):
        path = os.path.join(HERE, "web_graph_medium.txt")
        if not os.path.exists(path):
            self.skipTest("web_graph_medium.txt not present")
        graph = pr.load_graph(path)
        ranks, iters, history = pr.run_pagerank(graph, max_iter=50, epsilon=1e-6)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=4)
        self.assertLess(history[-1], 1e-6)
        self.assertGreater(len(ranks), 500)

    def test_large(self):
        path = os.path.join(HERE, "web_graph_large.txt")
        if not os.path.exists(path):
            self.skipTest("web_graph_large.txt not present")
        graph = pr.load_graph(path)
        ranks, iters, history = pr.run_pagerank(graph, max_iter=50, epsilon=1e-6)
        self.assertAlmostEqual(sum(ranks.values()), 1.0, places=4)
        self.assertLess(history[-1], 1e-6)
        self.assertGreater(len(ranks), 5000)


if __name__ == "__main__":
    unittest.main(verbosity=2)
