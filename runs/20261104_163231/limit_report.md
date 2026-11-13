# Limite del database - run 20261104_163231

Motivo di fine test: `ramp_completed`  
Mix: `{'read_pk': 40.0, 'read_range': 15.0, 'join_report': 10.0, 'join_heavy': 10.0, 'join_extreme': 10.0, 'write_insert': 10.0, 'write_hot': 5.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 8 | 16.8 | 0.309 | 1.021 | 1.543 | 0.00 | 0.324 | 2 | 5.2 | 7.5 | 13.4 |
| 10 | 20.5 | 0.309 | 1.032 | 1.740 | 0.00 | 0.326 | 2 | 5.8 | 12.3 | 15.9 |
| 12 | 22.2 | 0.309 | 1.456 | 2.555 | 0.00 | 0.324 | 3 | 6.7 | 13.8 | 20.3 |
| 14 | 23.8 | 0.311 | 1.842 | 3.301 | 0.00 | 0.326 | 3 | 7.8 | 18.3 | 23.1 |
| 16 | 24.5 | 0.311 | 2.467 | 4.247 | 0.00 | 0.324 | 3 | 10.2 | 15.5 | 24.7 |
| 20 | 25.4 | 0.315 | 3.760 | 5.733 | 0.00 | 0.326 | 3 | 13.5 | 22.3 | 25.4 |

## Gradino candidato

**16 connessioni.** Motivi:
- p95 2.467 s (> 2 volte il primo gradino, 1.021 s)
- throughput +3% con +14% di connessioni

Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte e controlla che il recupero torni ai valori della baseline.

## Worker nell'ultimo gradino (20 connessioni)

| Worker | Connessioni | Thread running | CPU % | Lock ms/s |
|---|---|---|---|---|
| worker-1 | 1.0 | 1.0 | 0.8 | 0.0 |
| worker-2 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-3 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-4 | 22.0 | 10.5 | 99.9 | 22.3 |
