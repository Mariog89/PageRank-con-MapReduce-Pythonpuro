# DESIGN.md — PageRank con MapReduce

**Autor(es):** Mario Alejandro Gutierrez Marquez   
**Fecha:** 28 de agosto de 2026

---

## 1. Representación de los datos

Cada nodo del grafo va a guardar tres cosas:

- el identificador del nodo;
- su PageRank actual;
- la lista de nodos a los que apunta.

La idea es representar cada nodo así:

```text
(node_id, (rank, [vecinos]))
```

Por ejemplo:

```text
("A", (0.125, ["B", "C"]))
```

Si un nodo no tiene vecinos, la lista queda vacía:

```text
("E", (0.125, []))
```

Al comienzo todos los nodos tendrán el mismo rank:

```text
rank_inicial = 1 / N
```

donde `N` es la cantidad total de nodos.

---

## 2. Esquema clave-valor por fase

### MAP

Por cada nodo, el mapper va a emitir dos tipos de mensajes. La **clave** del
par `(key, value)` siempre es el nodo (o el vecino destino), de modo que el
shuffle agrupe por nodo y el reducer reciba, para cada nodo, su `STRUCT` y
todas las contribuciones `RANK` que le llegaron. El **tipo** del mensaje
(`STRUCT` o `RANK`) va dentro del `value`, no es la clave.

El primero sirve para no perder la estructura del grafo:

```text
(origen, ("STRUCT", lista_adyacencia))
```

El segundo sirve para enviar la parte del rank que le corresponde a cada vecino:

```text
(destino, ("RANK", contribucion))
```

Por ejemplo, si tenemos:

```text
A -> B, C
rank(A) = 0.125
```

entonces `A` tiene 2 enlaces salientes, así que reparte:

```text
0.125 / 2 = 0.0625
```

El mapper emitiría:

```text
A -> ("STRUCT", ["B", "C"])
B -> ("RANK", 0.0625)
C -> ("RANK", 0.0625)
```

La tabla queda así:

| Mensaje      | Clave    | Valor                  | Para qué sirve                                            |
| ---          | ---      | ---                    | ---                                                       |
| `STRUCT`     | origen   | `("STRUCT", [vecinos])` | Mantener los enlaces del nodo para la siguiente iteración |
| `RANK`       | destino  | `("RANK", contribucion)` | Enviar PageRank al vecino destino                         |

Si un nodo no tiene enlaces salientes, no emite mensajes `RANK`, pero sí emite su `STRUCT`.

---

### SHUFFLE

El shuffle agrupa todos los mensajes que tengan la misma clave (es decir, por nodo).

Por ejemplo, para el nodo `A` podría quedar algo así:

```text
A -> [
    ("STRUCT", ["B", "C"]),
    ("RANK", 0.03),
    ("RANK", 0.07)
]
```

Entonces el reducer de `A` recibe:

- su lista de adyacencia;
- todas las contribuciones de rank que llegaron desde otros nodos.

---

### REDUCE

El reducer hace principalmente tres cosas:

1. recupera la lista de adyacencia;
2. suma todas las contribuciones `RANK`;
3. calcula el nuevo PageRank.

La suma de contribuciones sería:

```text
incoming_rank = suma de todos los mensajes RANK
```

El nuevo rank se calcula con:

```text
new_rank =
    (1 - d) / N
    +
    d * (incoming_rank + dangling_mass / N)
```

donde:

```text
d = 0.85
```

Después el reducer devuelve otra vez el nodo con su nuevo rank y con su misma lista de adyacencia:

```text
(node_id, (new_rank, [vecinos]))
```

Esto permite usar la salida de una iteración como entrada de la siguiente.

---

## 3. Preservación de la estructura del grafo

La lista de adyacencia no puede perderse porque PageRank necesita varias iteraciones.

Por eso el mapper manda un mensaje `STRUCT` hacia el mismo nodo:

```text
(origen=A, ("STRUCT", ["B", "C"]))
```

El reducer recibe ese mensaje y vuelve a guardar la lista junto con el nuevo rank.

La idea es esta:

```text
(A, rank_actual, [B, C])
        |
        v
       MAP
        |
        +--> STRUCT
        +--> RANK
        |
        v
      REDUCE
        |
        v
(A, nuevo_rank, [B, C])
```

Si no se enviara `STRUCT`, después de la primera iteración tendríamos los nuevos ranks, pero se perdería la información de los enlaces.

En la siguiente iteración ya no sabríamos hacia qué nodos repartir el rank.

---

## 4. Manejo de dangling nodes

Un dangling node es un nodo que no tiene enlaces salientes.

Por ejemplo:

```text
E:
```

Como no tiene vecinos, no puede repartir su rank normalmente.

Si simplemente ignoramos ese rank, parte del PageRank se perdería y la suma de todos los ranks dejaría de ser aproximadamente 1.

Por eso, antes de cada iteración, se calcula:

```text
dangling_mass =
    suma del rank de todos los nodos sin enlaces salientes
```

Luego esa masa se reparte por igual entre todos los nodos:

```text
dangling_mass / N
```

Así, el cálculo del nuevo rank queda:

```text
new_rank =
    (1 - d) / N
    +
    d * (incoming_rank + dangling_mass / N)
```

La idea es mantener:

```text
suma de ranks ≈ 1.0
```

El `dangling_mass` se calcula fuera de `mapreduce()` antes de ejecutar cada iteración.

---

## 5. Iteración y convergencia

La función `mapreduce()` solo hace una pasada:

```text
MAP -> SHUFFLE -> REDUCE
```

Pero PageRank necesita repetir ese proceso varias veces.

Por eso el ciclo de iteraciones va fuera de `mapreduce()`.

La idea general sería:

```text
crear ranks iniciales

repetir:

    calcular dangling_mass

    ejecutar mapreduce()

    comparar ranks viejos con ranks nuevos

    si ya casi no cambian:
        terminar

    si no:
        seguir con otra iteración
```

Para saber si ya convergió se usa la norma L1:

```text
L1 = suma |rank_nuevo - rank_anterior|
```

Si:

```text
L1 < epsilon
```

el algoritmo termina.

Se puede usar:

```text
epsilon = 1e-6
max_iter = 50
```

`max_iter` sirve para evitar que el algoritmo se quede iterando indefinidamente.

---

## 6. Diagrama de una iteración

```text
Estado actual del grafo
          |
          v
Calcular dangling_mass
          |
          v
         MAP
          |
          |---- STRUCT
          |
          |---- RANK
          |
          v
       SHUFFLE
          |
          v
       REDUCE
          |
          v
    Nuevo estado
          |
          v
     Calcular L1
          |
      +---+---+
      |       |
      |       |
   converge   no converge
      |       |
      v       |
     FIN      |
              |
              +----> siguiente iteración
```

---

## Resumen

El diseño se basa en estas ideas:

- Cada nodo guarda su rank y su lista de vecinos.
- El mapper emite mensajes `STRUCT` y `RANK`.
- `STRUCT` sirve para no perder la estructura del grafo.
- `RANK` sirve para enviar las contribuciones de PageRank.
- Los dangling nodes se manejan calculando su masa total y repartiéndola entre todos los nodos.
- El ciclo de iteraciones se hace fuera de `mapreduce()`.
- El algoritmo termina cuando la diferencia entre dos iteraciones es suficientemente pequeña o se alcanza el máximo de iteraciones.
