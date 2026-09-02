# PageRank con MapReduce (Python puro)

Implementación de PageRank sobre un framework MapReduce de un solo proceso
escrito a mano (`mapreduce_framework.py`).

## Estructura

- `pagerank.py` — algoritmo de PageRank: carga de grafo, mapper, reducer y loop de iteraciones.
- `mapreduce_framework.py` — `mapreduce(items, mapper, reducer)` que hace MAP -> SHUFFLE -> REDUCE.
- `test_pagerank.py` — tests unitarios (cadena, ciclo, dangling, convergencia, archivo de muestra).
- `web_graph_sample.txt` — grafo de 8 nodos usado por `TestSampleFile`.
- `DESIGN.md` — diseño del esquema clave-valor y el manejo de dangling.
- `ANALYSIS.md` — resultados sobre `web_graph_large.txt` y discusión de costos / Spark.
- `AI_LOG.md` — bitácora de uso de IA y errores corregidos.

## Uso

```bash
python pagerank.py web_graph_sample.txt
```

## Tests

```bash
python -m unittest test_pagerank -v
```
