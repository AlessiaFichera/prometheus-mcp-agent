# Limite del database - run 20261104_193045

Motivo di fine test: `ramp_completed`  
Mix: `{'read_pk': 40.0, 'read_range': 15.0, 'join_report': 10.0, 'join_heavy': 10.0, 'join_extreme': 10.0, 'write_insert': 10.0, 'write_hot': 5.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 8 | 16.4 | 0.309 | 1.047 | 1.535 | 0.00 | 0.327 | 2 | 5.5 | 2.7 | 13.5 |
| 10 | 20.4 | 0.309 | 1.103 | 1.771 | 0.00 | 0.328 | 2 | 6.0 | 5.3 | 15.8 |
| 12 | 22.3 | 0.309 | 1.436 | 2.602 | 0.00 | 0.320 | 2 | 7.7 | 16.7 | 20.4 |
| 14 | 23.8 | 0.312 | 1.891 | 3.175 | 0.00 | 0.323 | 2 | 9.3 | 11.3 | 23.3 |
| 16 | 24.5 | 0.311 | 2.559 | 4.298 | 0.00 | 0.325 | 3 | 12.2 | 26.1 | 24.4 |
| 20 | 25.3 | 0.314 | 3.806 | 5.832 | 0.00 | 0.324 | 3 | 14.3 | 17.2 | 25.4 |

## Gradino candidato

**16 connessioni.** Motivi:
- p95 2.559 s (> 2 volte il primo gradino, 1.047 s)
- throughput +3% con +14% di connessioni

Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte e controlla che il recupero torni ai valori della baseline.

## Worker nell'ultimo gradino (20 connessioni)

| Worker | Connessioni | Thread running | CPU % | Lock ms/s |
|---|---|---|---|---|
| worker-1 | 1.0 | 1.0 | 0.8 | 0.0 |
| worker-2 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-3 | 1.0 | 1.0 | 0.5 | 0.0 |
| worker-4 | 22.0 | 11.3 | 99.8 | 17.2 |
