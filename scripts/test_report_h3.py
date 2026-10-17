"""The report must reject incomplete or unmatched experiments."""
import copy
import unittest
from report_h3 import aggregate


class H3ReportTest(unittest.TestCase):
    def setUp(self):
        self.manifest = dict(rounds=4, seeds=2, budgets=[1], widths=[16], base_config=dict(seed=26))
        self.rows = [dict(scenario=scenario, budget=1, width=16, seed=seed, method=method,
                          releases=4, final_epsilon=1, max_epsilon_gap=0,
                          mean_linf=0.1 if method == 'hybrid' else 0.3,
                          mean_mse=0.01, mean_ncr=0.5, mean_processing_ms=1, total_bytes=128)
                     for scenario in ('stationary', 'scheduled', 'frequent')
                     for seed in (26, 27) for method in ('hybrid', 'local')]

    def test_paired_gap(self):
        cells = aggregate(self.rows, self.manifest)
        self.assertEqual(len(cells), 3)
        for cell in cells:
            self.assertAlmostEqual(cell['paired_linf_local_minus_hybrid']['mean'], 0.2)
            self.assertEqual(cell['paired_linf_local_minus_hybrid']['seed_sd'], 0)

    def test_reject_invalid_runs(self):
        variants = [self.rows[:-1], self.rows + [self.rows[0]]]
        for field, value in [('releases', 3), ('final_epsilon', 0.9), ('max_epsilon_gap', 0.01),
                             ('mean_linf', float('nan')), ('method', 'privsketch')]:
            rows = copy.deepcopy(self.rows)
            rows[0][field] = value
            variants.append(rows)
        for rows in variants:
            with self.assertRaises(ValueError):
                aggregate(rows, self.manifest)

    def test_budget_tolerance_is_absolute(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['budgets'] = [12]
        rows = copy.deepcopy(self.rows)
        for row in rows:
            row['budget'] = row['final_epsilon'] = 12
        rows[0]['final_epsilon'] += 1e-8
        with self.assertRaises(ValueError):
            aggregate(rows, manifest)

    def test_reject_empty_experiment(self):
        manifest = copy.deepcopy(self.manifest)
        manifest['seeds'] = 0
        with self.assertRaises(ValueError):
            aggregate([], manifest)


if __name__ == '__main__':
    unittest.main()
