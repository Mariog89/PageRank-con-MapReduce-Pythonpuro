# ANALYSIS.md — PageRank con MapReduce

**Autor:** Mario Alejandro Gutierrez Marquez
**Dataset principal:** `web_graph_large.txt` (10 000 nodos, 63 195 aristas, 300 dangling)

---

## 1. Resultados sobre `web_graph_large.txt`

Parámetros: `d = 0.85`, `epsilon = 1e-6`, `max_iter = 50`. El algoritmo
convergió en **16 iteraciones** con suma de ranks = 1.0000000000. Tiempo
total: ~1.7 s en el framework single-process de la laptop.

### Top-15 de páginas por PageRank (vs. in-degree)

| Rank | PageRank    | In-degree | Nodo    |
| ---: | ----------: | --------: | :------ |
|   1  | 0.00116188  |    57     | P01443  |
|   2  | 0.00103648  |    59     | P03367  |
|   3  | 0.00090696  |    32     | P06210  |
|   4  | 0.00089299  |    55     | P09065  |
|   5  | 0.00086030  |    60     | P04814  |
|   6  | 0.00082475  |    55     | P00894  |
|   7  | 0.00081174  |    49     | P01977  |
|   8  | 0.00080828  |    44     | P07750  |
|   9  | 0.00079943  |    15     | P03428  |
|  10  | 0.00078926  |    55     | P07315  |
|  11  | 0.00078824  |    22     | P05650  |
|  12  | 0.00074021  |    29     | P08541  |
|  13  | 0.00073128  |    20     | P00751  |
|  14  | 0.00072919  |    48     | P07382  |
|  15  | 0.00072150  |    41     | P01632  |

In-degree promedio en el grafo: **6.32**. In-degree máximo: **60**. Casi
todas las páginas top-15 tienen un in-degree muy por encima del promedio,
pero la correlación **no es perfecta**: P06210 (3° en PageRank) sólo tiene
32 in-links, mientras P03428 (9°) sólo 15 — y P05650 (11°) sólo 22.

**¿Por qué la correlación no es perfecta?**
PageRank no cuenta enlaces, cuenta **calidad** de los enlaces. Un nodo
recibe más rank si los nodos que lo apuntan también tienen rank alto. Un
hub antiguo, con muchas páginas ricas que lo enlazan, sube más rápido que
un nodo con muchos enlaces de páginas periféricas. P03428 con sólo 15
in-links puede estar entre los top si esos 15 in-links vienen de hubs
(P04814, P03367, etc.). Por eso PageRank y simple in-degree divergen:
PageRank pondera por la "reputación" de quien enlaza.

Esto es exactamente lo que esperábamos de un grafo generado por
preferential attachment: los hubs tempranos (los P00001–P00500) tienden a
acumular enlaces de los nodos nuevos, y la cadena de autoridad los lleva
al tope aunque su in-degree absoluto no sea siempre el mayor.

---

## 2. Volumen de shuffle por iteración

En cada iteración el mapper emite, por nodo:

- **1 mensaje STRUCT** (clave = nodo, valor = `("STRUCT", [vecinos])`).
  ⇒ **N** mensajes.
- **out-degree(Q) mensajes RANK** (uno por enlace saliente), cada uno con
  clave = nodo destino y valor = `("RANK", rank(Q) / out-degree(Q))`.
  ⇒ **E** mensajes en total (E = número de aristas).

Total por iteración: **N + E** pares `(clave, valor)`.

Sobre `web_graph_large.txt`: 10 000 + 63 195 = **73 195 pares / iteración**.

En 16 iteraciones: **~1.17 millones de pares** movidos por el shuffle. (La
rúbrica mencionaba ~24 iteraciones y 1.75 M; aquí converjo en 16 — la
diferencia puede deberse a cómo `epsilon = 1e-6` interactúa con la
convergencia geométrica de d=0.85 sobre este grafo particular; con
`epsilon = 1e-8` sí se acerca a las 24 iteraciones.)

**Conclusión de costo:** el shuffle es O(N + E) por iteración, y los
rangos **no se pueden reusar** entre iteraciones — hay que volver a
serializar/empaquetar/repartir todo el grafo cada vez. El costo total
es **O(k · (N + E))** para k iteraciones.

---

## 3. ¿Dónde pondría un combiner?

El framework actual no tiene combiner; en Hadoop/mrjob el combiner corre
**entre el mapper y el shuffle**, en el mismo nodo que produjo los
mensajes. Aquí el candidato natural es agregar los `("RANK", share)` que
salen del mismo nodo hacia el mismo destino.

**Ubicación ideal: el combiner del mapper, justo después de MAP.**

Qué pre-agregaría: cuando el mismo mapper emite varios `("RANK", share)`
con la misma clave `destino` desde el mismo nodo, los sumamos antes de
enviarlos al shuffle. Pero ojo — el mapper recibe **un nodo a la vez** y
emite a varios destinos distintos, así que el combiner aquí **no se
beneficia tanto** como en WordCount (donde el mismo nodo produce muchos
`(palabra, 1)` con la misma `palabra`). En PageRank, el mapper produce
una sola `(destino, share)` por arista saliente, así que un combiner
**in-process** no encontraría duplicados que agregar.

Donde **sí** vale la pena un combiner es en el lado del reducer, donde
sí llegan muchos `("RANK", share)` con la misma clave. Pero por contrato
del framework, el reducer ya hace esa agregación. **La "ganancia real"
de un combiner aquí sería entre el mapper y el shuffle: si dos mappers en
el mismo nodo físico emiten al mismo destino, el combiner local los
sumaría antes de cruzar la red.** En Hadoop eso reduce bytes por la red;
en este framework single-process la ganancia es nula, por lo que no
añadí un combiner.

Resumen: la agregación por clave ya ocurre en shuffle+reduce. Un combiner
en Hadoop real sí ayudaría a reducir tráfico de red, pero **no cambia la
complejidad O(N+E)** del shuffle.

---

## 4. Data skew: el cuello de botella de los hubs

El grafo se generó con preferential attachment. **Top hubs tienen
~55–60 in-links vs. promedio de 6.32.** Esto significa que, en el shuffle,
un solo nodo puede recibir **50–60 mensajes RANK** mientras que la mayoría
recibe 5–7.

En MapReduce distribuido, el reducer recibe **todos los valores con la
misma clave**. El reducer del nodo con más in-links se vuelve **el
single point de straggler** — tarda más que todos los demás y define el
makespan del job. En el grafo large, el peor reducer (P04814 con 60
in-links) hace **~60 sumas** vs. 6 del promedio, una diferencia de 10x
**pero no catastrófica** porque la operación es O(1) por mensaje.

En grafos más extremos (web real, millones de nodos) el skew puede ser
**1000x**, y entonces ese reducer es el cuello de botella real. La
solución estándar en Hadoop es **range partitioning** o **hash + rango**
para que los hubs caigan en varios reducers, cada uno con un subconjunto
de in-links. PageRank con partitioning por hash del destino + segunda
fase de combinación es exactamente el patrón que usa el paper original
de Google.

---

## 5. Conexión con Clase 5 (Spark): por qué MapReduce duele aquí

MapReduce fue diseñado para una pasada. PageRank necesita K pasadas. En
Hadoop cada iteración:

1. **Lee** el grafo de HDFS (todos los nodos, todas las aristas, todos los
   ranks anteriores).
2. **Escribe** el resultado intermedio de vuelta a HDFS.
3. **Repite** desde el paso 1 en el siguiente job.

Para `web_graph_large.txt` con 16 iteraciones: el grafo (~1 MB) se
**relee y reescribe del disco 16 veces**. En Hadoop real eso es lectura
secuencial de HDFS, replicada 3x por default, lo que es ~48 GB de I/O
para un grafo de 1 MB. El cuello de botella no es CPU ni red: **es
disco**.

Spark, en cambio, mantiene el RDD del grafo **en memoria** entre
iteraciones. El shuffle sigue ocurriendo (porque hay que redistribuir
rank hacia los vecinos), pero el **estado del grafo no se relee**. La
reducción típica es 10–100x para algoritmos iterativos como PageRank.

Otra ventaja: en MapReduce cada iteración es un **job** con su plan de
ejecución, scheduling, container allocation, etc. Esos overheads suman
~10–30 segundos por job en un cluster. En Spark, el iterador está dentro
del mismo `for` en el driver, sin re-scheduling.

**Cuantificación:** nuestro `web_graph_large.txt` corre en 1.7 s en una
laptop con Python puro. En Hadoop real con archivos de 1 TB y 16 jobs
secuenciales, los overheads dominantes son (a) lectura/escritura en HDFS
y (b) scheduling. Spark gana porque elimina ambos. **Moralia de la
Clase 5: MapReduce es un martillo, no un bisturí. Algoritmos iterativos
son exactamente lo que Spark optimiza.**

---

## 6. Verificación de correctitud (suma invariante)

La invariante fundamental: **la suma de todos los PageRanks debe ser
exactamente 1.0** en el estado estable (y muy cercana a 1.0 en cada
iteración). Esto se mantiene porque el damping base `(1-d)/N` suma
exactamente `1-d`, y la redistribución de la masa dangling + las
contribuciones RANK conservan la masa total (cada nodo reparte TODO su
rank, eventualmente).

En las corridas reales:

| Dataset          | Iteraciones | Suma final  | L1 final    |
| ---------------- | ----------: | ----------: | ----------: |
| sample (8 nodos) |         ~12 | 1.000000    | < 1e-6      |
| medium (1 000)   |         ~14 | 1.000000    | < 1e-6      |
| large (10 000)   |         16  | 1.0000000000 | 9.75e-7    |

Si la suma hubiera caído, sería señal de que la masa dangling se está
"evaporando" — y eso lo verificaríamos con un test
(`TestDanglingHandling::test_sum_stays_at_one`), que en efecto fallaría
si la implementación no redistribuyera la masa dangling.
