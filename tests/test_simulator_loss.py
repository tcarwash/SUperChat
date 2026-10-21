import unittest

from scripts.simulate_mesh import Mesh


def new_mesh(model: str) -> Mesh:
    return Mesh(loss=0.35, bitrate=1200, propagation=0.05, interframe=0.25,
                framing_bytes=24, contention_window=1, time_scale=0.1, seed=19,
                topology="line", loss_model=model, burst_recovery=0.25,
                transport="broadcast", trace_sync=False,
                drop_first_history_fragment=False,
                drop_first_history_page_end=False,
                drop_first_image_fragment=False,
                drop_first_proof_fragment=False)


class SimulatorLossTests(unittest.TestCase):
    def test_bursty_loss_is_seeded_and_correlated_per_link(self):
        first, second = new_mesh("bursty"), new_mesh("bursty")
        first_samples = [first._drop_for_loss("A", "B")[0] for _ in range(500)]
        second_samples = [second._drop_for_loss("A", "B")[0] for _ in range(500)]
        self.assertEqual(first_samples, second_samples)
        self.assertLess(abs(sum(first_samples) / len(first_samples) - 0.35), 0.08)
        longest_run = current_run = 0
        for dropped in first_samples:
            current_run = current_run + 1 if dropped else 0
            longest_run = max(longest_run, current_run)
        self.assertGreaterEqual(longest_run, 3)

    def test_link_random_streams_are_independent(self):
        first, second = new_mesh("bursty"), new_mesh("bursty")
        first_a = [first._drop_for_loss("A", "B")[0] for _ in range(100)]
        first_c = [first._drop_for_loss("C", "B")[0] for _ in range(100)]
        second_c = [second._drop_for_loss("C", "B")[0] for _ in range(100)]
        self.assertEqual(first_c, second_c)
        self.assertNotEqual(first_a, first_c)


if __name__ == "__main__":
    unittest.main()
