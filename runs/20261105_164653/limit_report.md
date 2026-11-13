# Limite del database - run 20261105_164653

Motivo di fine test: `p95_at_32_connections`  
Mix: `{'join_extreme': 25.0, 'write_hot': 25.0, 'read_pk': 50.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 16 | 11.2 | 0.819 | 5.573 | 6.465 | 0.00 | 0.277 | 1 | 16.2 | 108.9 | 25.4 |
| 32 | 11.8 | 0.828 | 12.661 | 14.530 | 0.00 | 0.272 | 1 | 30.8 | 116.5 | 25.4 |

## Gradino candidato

**32 connessioni.** Motivi:
- p95 12.661 s (> 2 volte il primo gradino, 5.573 s)
- throughput +5% con +100% di connessioni

Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte e controlla che il recupero torni ai valori della baseline.

## Worker nell'ultimo gradino (32 connessioni)

| Worker | Connessioni | Thread running | CPU % | Lock ms/s |
|---|---|---|---|---|
| worker-1 | 1.0 | 1.0 | 0.7 | 0.0 |
| worker-2 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-3 | 1.0 | 1.0 | 0.4 | 0.0 |
| worker-4 | 36.0 | 27.8 | 100.0 | 116.5 |
