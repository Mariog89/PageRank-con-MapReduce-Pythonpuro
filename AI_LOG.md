# AI_LOG.md — Bitácora de uso de IA

**Autor:** Mario Alejandro Gutierrez Marquez
**Tarea:** PageRank con MapReduce (framework `mapreduce_framework.py`)
**Asistentes usados:** Copilot (sugerencias inline en VS Code) + razonamiento propio

---

## Resumen honesto

Sí usé IA como asistente. La bitácora de **qué le pedí, qué me dio
mal, y qué corregí yo** está abajo. La parte que la IA no hizo por mí
fue: entender el contrato del framework (el mapper recibe *un item a la
vez*, no una lista), decidir cómo preservar la adyacencia entre
iteraciones, decidir el manejo de dangling nodes, escribir los casos de
prueba hand-computables, y conectar el análisis con la motivación de
Spark en Clase 5. Esas son las partes que cuentan para la nota
(70% de la rúbrica, ver §8 de la tarea).

---

## 1. Primer intento: esquema clave-valor incorrecto

### Lo que generé (mal)
Mi `pr_mapper` original emitía:

```python
yield ("STRUCT", (node, list(neighbors)))         # MAL
yield ("RANK",   (nbr, share))                    # MAL
```

Es decir, la **clave** era el literal `"STRUCT"` o `"RANK"`, y el nodo
viajaba dentro del valor. Esto seguía literalmente la tabla del DESIGN
inicial que yo mismo había escrito.

### Por qué estaba mal
El `mapreduce()` agrupa valores por **clave**. Si todos los `STRUCT`
tienen clave `"STRUCT"` y todos los `RANK` tienen clave `"RANK"`, el
shuffle produce **sólo dos grupos**: uno para todos los STRUCTs del
grafo y otro para todos los RANKs. El reducer de `"STRUCT"` recibe la
lista de adyacencia de *todos* los nodos mezclada. Imposible reconstruir
el estado nodo por nodo.

Lo detecté corriendo los tests: el `test_sum_invariant_under_damping`
reportaba una suma de ranks de **0.167** (≈ 1/6) en un grafo de 6 nodos
— síntoma clásico de "el reducer nunca recibió las contribuciones RANK,
asignó `incoming = 0` a todos, y el único término sobreviviente fue
`(1-d)/N = 0.15/N`".

### Cómo lo corregí
El **grupo** en el shuffle debe ser **por nodo**, no por tipo de mensaje.
Entonces el tag (`STRUCT` / `RANK`) tiene que ir **dentro del value**, y
la **clave** tiene que ser el id del nodo:

```python
yield (node,   ("STRUCT", list(neighbors)))
yield (nbr,    ("RANK",   share))
```

El reducer recibe para cada nodo su `STRUCT` + todas las `RANK` que le
llegan. Y entonces sí puede sumar y devolver el nuevo rank.

Actualicé también `DESIGN.md` para que el esquema clave-valor reflejara
esta corrección — porque la versión original del documento era
literalmente la fuente del bug. La moralea: si el diseño dice una cosa y
el código dice otra, normalmente **ambos están mal**; lo correcto es
arreglar el diseño, no solo el código.

### Por qué esto es importante para la defensa oral
Si me preguntan "¿dónde preservas la adyacencia?", la respuesta es:

> "En el mapper emito un mensaje `("STRUCT", [vecinos])` con clave
> igual al id del nodo, para que el shuffle agrupe ese STRUCT en el
> mismo grupo que las contribuciones RANK que le llegan. Sin ese STRUCT,
> el reducer no podría reconstruir la lista de adyacencia en la
> siguiente iteración y PageRank no podría continuar."

Y la prueba: **comentar la línea del `STRUCT`** y volver a correr
`python -m unittest test_pagerank` — falla casi todo, porque después de
la primera iteración los nodos pierden sus enlaces.

---

## 2. Manejo de dangling nodes: lo que casi se me olvida

### El error tentador
Mi primer borrador del reducer hacía:

```python
new_rank = (1 - d) / n + d * incoming
```

Sin redistribuir la masa dangling. Funciona *casi*: la suma de ranks se
mantiene cerca de 1, pero **drift down** en cada iteración, porque cada
nodo dangling emite `incoming = 0` para la próxima ronda — la masa se
evapora. En la práctica, en el grafo `web_graph_large.txt` con 300
dangling, después de 16 iteraciones la suma se quedaría en ~0.95, no
1.0. El test `TestDanglingHandling::test_sum_stays_at_one` lo cazaba
con `assertAlmostEqual(s, 1.0, places=6)`.

### La decisión que tomé (y por qué)
Calcular `dangling_mass = sum(rank for rank, nbrs in state.values() if
not nbrs)` **antes** de cada `mapreduce()`, y redistribuirlo en el
driver (fuera del framework) sumándolo al término `incoming`:

```python
new_rank = (1 - d) / n + d * (incoming + dangling_mass / n)
```

Esto preserva la masa total exactamente y mantiene la invariante de
suma = 1.0. Es la misma fórmula que usa el paper original de PageRank.

### La alternativa que descarté
Hubiera podido emitir un mensaje RANK especial desde cada dangling node
hacia un nodo "sumidero" y luego redistribuir en un job extra. Eso es
más "MapReduce puro" pero requiere un segundo job, **rompe la
simplicidad del loop en Python**, y no aporta nada. La opción
"calcular dangling_mass en el driver" es la correcta para este
framework: vive en una sola iteración, es exacta, y deja el mapper
trivial.

---

## 3. Test que estaba mal escrito: `test_max_iter_cap`

Mi test original usaba el grafo `{A:[B], B:[C], C:[A]}` (un ciclo de 3)
con `epsilon = 1e-20` esperando que no convergiera en 7 iteraciones. **El
grafo es un punto fijo de PageRank**: si partes con `1/3` en cada
nodo, la primera iteración ya da `1/3` exacto (por simetría), así que
`L1 = 0 < 1e-20` y termina en **1 iteración**, no en 7. Mi test
asumía mal que el ciclo "necesita" iteraciones para converger.

Lo corregí cambiando a un grafo asimétrico
(`{A:[B,C], B:[C], C:[A], D:[A]}`) que de verdad necesita iteraciones.
Moralea: cuando un test falla, **puede ser culpa del test, no del
código bajo prueba**. Hay que mirar el test con la misma sospecha que
el código.

---

## 4. Lo que NO le pedí a la IA (y por qué)

- **El diseño de la preservación del grafo** lo pensé yo. La pista de
  "el mapper puede emitir dos tipos de mensajes" la da la tarea, pero
  la decisión específica de *qué* es la clave y *qué* es el value
  (clave = nodo, tag = value) es mía, y es la que casi me sale mal en
  el primer intento.

- **Los casos de prueba hand-computables** los escribí a mano
  pensando en la rúbrica: cadena trivial, ciclo, dangling aislado,
  invariante de suma, cap de iteraciones. La IA no te dice "deberías
  probar un grafo asimétrico con un cap de iteraciones" — eso es
  criterio de testing.

- **El análisis de costo y la conexión con Spark** lo redacté yo,
  porque depende de entender qué hace el framework (relee el grafo
  entero cada iteración) y de conectar eso con lo visto en Clase 5.
  La IA generaría texto genérico; aquí se evalúa que el alumno
  *conecte* los conceptos.

- **El top-15 con in-degree** lo calculé con un script corto mío, no
  con la IA. Y la observación de que "P06210 está 3° con sólo 32
  in-links pero P03428 está 9° con sólo 15, mientras P04814 está 5°
  con 60" es la discusión que vale la pena hacer, no un análisis
  cuantitativo cualquiera.

---

## 5. Prompt-style resumen de las preguntas que me hice

> "¿Qué pasa si la clave es el tipo de mensaje en vez del nodo?"

→ Mala idea. El shuffle no puede reconstruir el estado. (Descubierto
corriendo el test, no pensando.)

> "¿Calculo el dangling mass dentro de mapreduce o fuera?"

→ Fuera. Mapreduce no tiene un mecanismo para "todos los nodos con
cierta propiedad". El driver itera sobre el estado y lo calcula en una
pasada O(N).

> "¿Por qué mi cycle test no respeta el max_iter?"

→ Porque el ciclo es un punto fijo, no necesita iteraciones. Mala
elección de fixture, no bug del código.

> "¿Cómo preservo la adyacencia entre iteraciones?"

→ STRUCT message con clave = nodo. La estructura se re-emite cada
iteración. Si no, se pierde en la primera pasada.

---

## 6. Cierre

Lo que la IA sí me dio bien fue:

- Sintaxis de `unittest` (cosas que ya sabía pero recordé más rápido
  con autocompletar).
- Plantilla para `print_results` con sort por valor.

Lo que la IA me dio mal, o me hubiera dado mal si le hubiera pedido:

- El esquema clave-valor. Si le hubiera pedido a Copilot "dame el
  esquema para PageRank en MapReduce", probablemente me daba algo
  parecido a WordCount (un solo tipo de mensaje, clave = nodo destino)
  — y entonces se hubiera perdido la adyacencia. **Ese es exactamente
  el error que la tarea advierte que la IA comete "casi siempre".**

Conclusión: el código pasó, pero más importante, **entiendo por qué
pasó** y dónde se rompería si le quitara el `STRUCT`, si le quitara la
redistribución de dangling mass, o si cambiara la clave. Eso es lo que
la defensa oral va a probar.
