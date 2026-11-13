# Limite del database - run 20261104_190132

Motivo di fine test: `ramp_completed`  
Mix: `{'read_pk': 70.0, 'read_range': 30.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 64 | 225.7 | 0.281 | 0.297 | 0.342 | 0.00 | 0.276 | 12 | 4.0 | 0.0 | 1.8 |
| 128 | 443.2 | 0.283 | 0.307 | 0.376 | 0.00 | 0.278 | 32 | 4.0 | 0.0 | 2.6 |
| 160 | 516.2 | 0.285 | 0.313 | 0.391 | 1.60 | 0.277 | 41 | 3.2 | 0.0 | 3.0 |

## Gradino candidato

**160 connessioni.** Motivi:
- errori 1.604% (> 1%)

Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte e controlla che il recupero torni ai valori della baseline.

## Worker nell'ultimo gradino (160 connessioni)

| Worker | Connessioni | Thread running | CPU % | Lock ms/s |
|---|---|---|---|---|
| worker-1 | 1.0 | 1.0 | 3.2 | 0.0 |
| worker-2 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-3 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-4 | 152.0 | 1.0 | 8.0 | n/d |
