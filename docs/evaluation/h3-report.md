# H3: central versus local privacy

60 rounds per run; 5 paired seeds per cell. Equal sketch rows/widths, clipped event target, client groups, and cumulative epsilon at **every round**. Both methods release every round. Hybrid uses central (epsilon, delta)-DP; strict Count-Min uses pure local DP. Matching epsilon does not make these trust models or guarantees identical.

Central delta: 1e-05. Deterministic research noise: true. PrivSketch is excluded.

Each error is the time mean of maximum absolute signed frequency error. SD is between-seed sample SD; the paired gap is local minus Hybrid. Windows overlap, so rounds are not independent samples. No significance or universal superiority claim is made.

| Stream | Final epsilon | Width | Hybrid error ± SD | Local error ± SD | Paired gap ± SD | Hybrid ms | Local ms | Hybrid MiB | Local MiB |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| frequent | 1 | 16 | 0.5515 ± 0.0119 | 43.9830 ± 1.6313 | 43.4315 ± 1.6246 | 5.465 | 0.148 | 146.250 | 0.029 |
| frequent | 1 | 64 | 0.5444 ± 0.0050 | 88.8577 ± 2.1248 | 88.3133 ± 2.1220 | 18.493 | 0.159 | 585.000 | 0.059 |
| frequent | 6 | 16 | 0.1170 ± 0.0035 | 7.0445 ± 0.2020 | 6.9276 ± 0.2038 | 5.386 | 0.149 | 146.250 | 0.029 |
| frequent | 6 | 64 | 0.1025 ± 0.0015 | 14.2296 ± 0.3735 | 14.1271 ± 0.3744 | 18.774 | 0.150 | 585.000 | 0.059 |
| frequent | 12 | 16 | 0.0756 ± 0.0037 | 3.4132 ± 0.0845 | 3.3376 ± 0.0845 | 5.341 | 0.142 | 146.250 | 0.029 |
| frequent | 12 | 64 | 0.0577 ± 0.0011 | 6.9496 ± 0.1777 | 6.8920 ± 0.1780 | 19.142 | 0.155 | 585.000 | 0.059 |
| scheduled | 1 | 16 | 0.5510 ± 0.0117 | 43.9826 ± 1.5665 | 43.4316 ± 1.5599 | 5.254 | 0.139 | 146.250 | 0.029 |
| scheduled | 1 | 64 | 0.5450 ± 0.0070 | 88.8896 ± 2.1174 | 88.3446 ± 2.1132 | 18.900 | 0.159 | 585.000 | 0.059 |
| scheduled | 6 | 16 | 0.1161 ± 0.0023 | 7.0494 ± 0.1917 | 6.9333 ± 0.1917 | 5.521 | 0.148 | 146.250 | 0.029 |
| scheduled | 6 | 64 | 0.1026 ± 0.0024 | 14.2238 ± 0.3671 | 14.1212 ± 0.3674 | 19.330 | 0.156 | 585.000 | 0.059 |
| scheduled | 12 | 16 | 0.0732 ± 0.0041 | 3.4251 ± 0.0841 | 3.3519 ± 0.0843 | 5.485 | 0.148 | 146.250 | 0.029 |
| scheduled | 12 | 64 | 0.0576 ± 0.0011 | 6.9305 ± 0.1855 | 6.8729 ± 0.1855 | 18.694 | 0.156 | 585.000 | 0.059 |
| stationary | 1 | 16 | 0.5495 ± 0.0111 | 43.9951 ± 1.5749 | 43.4455 ± 1.5672 | 5.264 | 0.139 | 146.250 | 0.029 |
| stationary | 1 | 64 | 0.5447 ± 0.0058 | 88.8902 ± 2.1173 | 88.3455 ± 2.1139 | 18.066 | 0.147 | 585.000 | 0.059 |
| stationary | 6 | 16 | 0.1165 ± 0.0044 | 7.0323 ± 0.2053 | 6.9159 ± 0.2063 | 5.365 | 0.145 | 146.250 | 0.029 |
| stationary | 6 | 64 | 0.1027 ± 0.0011 | 14.2604 ± 0.3616 | 14.1577 ± 0.3616 | 19.602 | 0.166 | 585.000 | 0.059 |
| stationary | 12 | 16 | 0.0750 ± 0.0080 | 3.4239 ± 0.0799 | 3.3489 ± 0.0820 | 5.694 | 0.157 | 146.250 | 0.029 |
| stationary | 12 | 64 | 0.0577 ± 0.0012 | 6.9372 ± 0.1822 | 6.8795 ± 0.1825 | 18.638 | 0.154 | 585.000 | 0.059 |

Processing ms is per-method wall time in this in-process simulator, including encoding, aggregation, decoding and truth evaluation. It excludes source waiting, network round trips and distributed scheduling. Methods run in fixed Hybrid-then-local order. Communication counts modeled payload only, excluding framing and setup. Timing SD, MSE and NCR are in aggregate.json.

The schedule uses constant central rho per round, calibrated to the final budget; each local report spends the increase in the central cumulative epsilon. Local per-report epsilon therefore varies over the public horizon. This controls cumulative epsilon at each prefix, rather than optimizing either protocol. Equal widths control a layout parameter; they do not equalize payload or computation. Raw signed estimates are not clamped, so local errors may exceed one.
