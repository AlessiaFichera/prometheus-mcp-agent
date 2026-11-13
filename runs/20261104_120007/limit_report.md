# Limite del database - run 20261104_120007

Motivo di fine test: `ramp_completed`  
Mix: `{'read_pk': 50.0, 'read_range': 20.0, 'join_report': 10.0, 'join_heavy': 5.0, 'write_insert': 10.0, 'write_hot': 5.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.4 | 0.615 | 1.229 | 1.332 | 0.00 | 0.636 | 0 | 4.0 | 0.0 | 0.8 |
| 2 | 2.7 | 0.615 | 1.329 | 1.433 | 0.00 | 0.637 | 1 | 4.0 | 0.0 | 0.9 |
| 4 | 5.5 | 0.615 | 1.244 | 1.437 | 0.00 | 0.637 | 1 | 4.2 | 0.4 | 1.2 |
| 8 | 11.1 | 0.615 | 1.238 | 1.345 | 0.00 | 0.633 | 2 | 4.0 | 1.8 | 1.7 |
| 16 | 22.5 | 0.615 | 1.246 | 1.407 | 0.00 | 0.630 | 3 | 4.0 | 8.9 | 2.3 |
| 32 | 49.4 | 0.558 | 1.141 | 1.263 | 0.00 | 0.571 | 4 | 4.3 | 58.9 | 4.0 |
| 64 | 97.2 | 0.565 | 1.155 | 1.394 | 0.00 | 0.553 | 10 | 4.2 | 211.7 | 6.3 |

## Gradino candidato

- Nessun limite raggiunto: il test non ha saturato il sistema.

## Worker nell'ultimo gradino (64 connessioni)

| Worker | Connessioni | Thread running | CPU % | Lock ms/s |
|---|---|---|---|---|
| worker-1 | 1.0 | 1.0 | 2.0 | 0.0 |
| worker-2 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-3 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-4 | 67.0 | 1.2 | 22.3 | 211.7 |
