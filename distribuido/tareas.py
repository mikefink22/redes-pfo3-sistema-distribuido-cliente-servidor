"""Tareas que saben ejecutar los workers (lista blanca: nunca se usa eval/exec)."""
import time


def _numero(datos, minimo, maximo, entero=True):
    tipos = (int,) if entero else (int, float)
    if isinstance(datos, bool) or not isinstance(datos, tipos):
        raise ValueError("se esperaba un " + ("entero" if entero else "número"))
    if not minimo <= datos <= maximo:
        raise ValueError(f"el valor debe estar entre {minimo} y {maximo}")
    return datos


def fibonacci(datos):
    """n-ésimo número de Fibonacci (tarea que consume CPU)."""
    n = _numero(datos, 0, 1000)
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def es_primo(datos):
    """Primalidad por división de prueba (tarea que consume CPU)."""
    n = _numero(datos, 0, 10**12)
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def invertir_texto(datos):
    if not isinstance(datos, str):
        raise ValueError("se esperaba un texto")
    if len(datos) > 10_000:
        raise ValueError("texto demasiado largo (máximo 10000 caracteres)")
    return datos[::-1]


def dormir(datos):
    """Simula una tarea de E/S lenta (consulta a una base, llamada a otra API...)."""
    segundos = _numero(datos, 0, 10, entero=False)
    time.sleep(segundos)
    return f"dormí {segundos} s"


TAREAS = {
    "fibonacci": fibonacci,
    "es_primo": es_primo,
    "invertir_texto": invertir_texto,
    "dormir": dormir,
}


def ejecutar(tipo, datos):
    funcion = TAREAS.get(tipo) if isinstance(tipo, str) else None
    if funcion is None:
        raise ValueError(f"tarea desconocida: {tipo!r} (disponibles: {', '.join(TAREAS)})")
    return funcion(datos)
