#!/usr/bin/env python3
"""Aggregate paired H3 runs. Uses only the Python standard library."""
import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


METHODS = ('hybrid', 'local')
METRICS = ('mean_linf', 'mean_mse', 'mean_ncr', 'mean_processing_ms', 'total_bytes')
SCENARIOS = ('stationary', 'scheduled', 'frequent')


def summarize(values):
    return dict(mean=statistics.mean(values), seed_sd=statistics.stdev(values))


def aggregate(summaries, manifest):
    if (manifest['rounds'] < 1 or manifest['seeds'] < 2
            or not manifest['budgets'] or not manifest['widths']):
        raise ValueError('H3 requires rounds, budgets, widths and at least two seeds')
    groups = defaultdict(dict)
    expected = {
        (scenario, budget, width, manifest['base_config']['seed'] + seed)
        for scenario in SCENARIOS
        for budget in manifest['budgets'] for width in manifest['widths']
        for seed in range(manifest['seeds'])
    }
    for row in summaries:
        key = (row['scenario'], row['budget'], row['width'], row['seed'])
        method = row['method']
        if key not in expected or method not in METHODS or method in groups[key]:
            raise ValueError('unexpected or duplicate H3 run')
        matches_budget = math.isclose(
            row['final_epsilon'], row['budget'], rel_tol=0, abs_tol=1e-9,
        )
        if row['releases'] != manifest['rounds'] or not matches_budget:
            raise ValueError('H3 run did not release all rounds at the requested budget')
        if not 0 <= row['max_epsilon_gap'] <= 1e-9:
            raise ValueError('H3 cumulative privacy mismatch')
        for field in METRICS:
            if not math.isfinite(row[field]) or row[field] < 0:
                raise ValueError('invalid H3 measurement')
        groups[key][method] = row
    if set(groups) != expected or any(set(pair) != {'hybrid', 'local'} for pair in groups.values()):
        raise ValueError('missing H3 pair or run')
    cells = defaultdict(list)
    for (scenario, budget, width, seed), pair in sorted(groups.items()):
        cells[(scenario, budget, width)].append(pair)
    result = []
    for (scenario, budget, width), pairs in sorted(cells.items()):
        row = dict(scenario=scenario, budget=budget, width=width, seeds=len(pairs))
        for method in METHODS:
            row[method] = {}
            for field in METRICS:
                values = [pair[method][field] for pair in pairs]
                row[method][field] = summarize(values)
        # Paired differences preserve the shared stream/seed, rather than treating
        # individual overlapping windows as independent statistical samples.
        gaps = [p['local']['mean_linf'] - p['hybrid']['mean_linf'] for p in pairs]
        row['paired_linf_local_minus_hybrid'] = summarize(gaps)
        row['max_epsilon_gap'] = max(p[m]['max_epsilon_gap'] for p in pairs for m in METHODS)
        result.append(row)
    return result


def render_report(rows, manifest):
    text = '# H3: central versus local privacy\n\n'
    text += (
        f"{manifest['rounds']} rounds per run; {manifest['seeds']} paired seeds per cell. "
        "Equal sketch rows/widths, clipped event target, client groups, and cumulative epsilon "
        "at **every round**. Both methods release every round. Hybrid uses central "
        "(epsilon, delta)-DP; strict Count-Min uses pure local DP. Matching epsilon does "
        "not make these trust models or guarantees identical.\n\n"
    )
    config = manifest['base_config']
    text += (
        f"Central delta: {config['delta']:g}. Deterministic research noise: "
        f"{str(config['deterministic_noise']).lower()}. PrivSketch is excluded.\n\n"
    )
    text += (
        "Each error is the time mean of maximum absolute signed frequency error. "
        "SD is between-seed sample SD; the paired gap is local minus Hybrid. "
        "Windows overlap, so rounds are not independent samples. "
        "No significance or universal superiority claim is made.\n\n"
    )
    headers = (
        'Stream', 'Final epsilon', 'Width', 'Hybrid error ± SD', 'Local error ± SD',
        'Paired gap ± SD', 'Hybrid ms', 'Local ms', 'Hybrid MiB', 'Local MiB',
    )
    text += table_row(headers)
    text += table_row(['---'] * len(headers))
    for row in rows:
        hybrid, local = row['hybrid'], row['local']
        text += table_row([
            row['scenario'], f"{row['budget']:g}", str(row['width']),
            format_error(hybrid['mean_linf']), format_error(local['mean_linf']),
            format_error(row['paired_linf_local_minus_hybrid']),
            f"{hybrid['mean_processing_ms']['mean']:.3f}",
            f"{local['mean_processing_ms']['mean']:.3f}",
            f"{hybrid['total_bytes']['mean'] / 1024**2:.3f}",
            f"{local['total_bytes']['mean'] / 1024**2:.3f}",
        ])
    text += (
        "\nProcessing ms is per-method wall time in this in-process simulator, including "
        "encoding, aggregation, decoding and truth evaluation. It excludes source waiting, "
        "network round trips and distributed scheduling. Methods run in fixed Hybrid-then-local "
        "order. Communication counts modeled payload only, excluding framing and setup. "
        "Timing SD, MSE and NCR are in aggregate.json.\n\n"
        "The schedule uses constant central rho per round, calibrated to the final budget; "
        "each local report spends the increase in the central cumulative epsilon. Local "
        "per-report epsilon therefore varies over the public horizon. This controls cumulative "
        "epsilon at each prefix, rather than optimizing either protocol. Equal widths control "
        "a layout parameter; they do not equalize payload or computation. Raw signed estimates "
        "are not clamped, so local errors may exceed one.\n"
    )
    return text


def table_row(values):
    return '| ' + ' | '.join(values) + ' |\n'


def format_error(cell):
    return f"{cell['mean']:.4f} ± {cell['seed_sd']:.4f}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    root = parser.parse_args().directory
    manifest = json.loads((root / 'manifest.json').read_text())
    rows = aggregate(json.loads((root / 'summary.json').read_text()), manifest)
    (root / 'aggregate.json').write_text(json.dumps(rows, indent=2) + '\n')
    (root / 'report.md').write_text(render_report(rows, manifest))


if __name__ == '__main__':
    main()
