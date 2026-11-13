# Limite del database - run 20261104_182411

Motivo di fine test: `ramp_completed`  
Mix: `{'write_hot': 60.0, 'read_pk': 40.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 8 | 11.6 | 0.832 | 1.195 | 1.612 | 0.00 | 0.293 | 1 | 4.3 | 518.4 | 3.0 |
| 16 | 20.0 | 0.856 | 1.678 | 2.062 | 0.00 | 0.282 | 2 | 6.8 | 3388.9 | 4.1 |
| 32 | 25.4 | 0.963 | 3.754 | 5.046 | 0.00 | 0.282 | 2 | 20.8 | 15940.1 | 4.3 |
| 64 | 27.3 | 1.363 | 7.512 | 8.732 | 0.00 | 0.276 | 3 | 51.2 | 46900.3 | 4.4 |

## Gradino candidato

**32 connessioni.** Motivi:
- p95 3.754 s (> 2 volte il primo gradino, 1.195 s)

Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte e controlla che il recupero torni ai valori della baseline.

## Worker nell'ultimo gradino (64 connessioni)

| Worker | Connessioni | Thread running | CPU % | Lock ms/s |
|---|---|---|---|---|
| worker-1 | 1.0 | 1.0 | 1.0 | 0.0 |
| worker-2 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-3 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-4 | 66.0 | 48.2 | 15.8 | 46900.3 |
