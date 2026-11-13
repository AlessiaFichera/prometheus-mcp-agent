# Limite del database - run 20261104_154217

Motivo di fine test: `p95_at_64_connections`  
Mix: `{'read_pk': 40.0, 'read_range': 15.0, 'join_report': 10.0, 'join_heavy': 10.0, 'join_extreme': 10.0, 'write_insert': 10.0, 'write_hot': 5.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 8 | 16.2 | 0.309 | 1.089 | 1.841 | 0.00 | 0.321 | 2 | 5.7 | 7.2 | 13.6 |
| 16 | 26.5 | 0.310 | 2.149 | 3.753 | 0.00 | 0.324 | 3 | 11.2 | 15.2 | 24.1 |
| 32 | 26.4 | 0.327 | 7.767 | 11.264 | 0.00 | 0.325 | 3 | 25.7 | 20.3 | 25.4 |
| 64 | 26.5 | 0.378 | 18.779 | 27.157 | 0.03 | 0.324 | 3 | 57.0 | 24.5 | 25.4 |

## Gradino candidato

**32 connessioni.** Motivi:
- p95 7.767 s (> 2 volte il primo gradino, 1.089 s)
- throughput -0% con +100% di connessioni

Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte e controlla che il recupero torni ai valori della baseline.

## Worker nell'ultimo gradino (64 connessioni)

| Worker | Connessioni | Thread running | CPU % | Lock ms/s |
|---|---|---|---|---|
| worker-1 | 1.0 | 1.0 | 0.8 | 0.0 |
| worker-2 | 1.2 | 1.0 | 0.4 | 0.0 |
| worker-3 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-4 | 66.0 | 54.0 | 100.0 | 24.5 |
