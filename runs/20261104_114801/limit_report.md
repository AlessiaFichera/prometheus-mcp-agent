# Limite del database - run 20261104_114801

Motivo di fine test: `ramp_completed`  
Mix: `{'read_pk': 70.0, 'read_range': 30.0}`

| Conn. | QPS | p50 s | p95 s | p99 s | Errori % | SELECT 1 p50 s | CPU client % | Thread running | Lock ms/s | CPU DB % |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1.6 | 0.615 | 0.729 | 0.819 | 0.00 | 0.638 | 0 | 4.0 | 0.0 | 0.6 |
| 2 | 3.2 | 0.614 | 0.717 | 0.820 | 0.00 | 0.639 | 0 | 4.0 | 0.0 | 0.6 |

## Gradino candidato

- Nessun limite raggiunto: il test non ha saturato il sistema.

Il gradino e' un candidato: confermalo ripetendo il test almeno 3 volte e controlla che il recupero torni ai valori della baseline.
