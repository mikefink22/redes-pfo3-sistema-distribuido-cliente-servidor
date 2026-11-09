"""Cliente: envía tareas al servidor por socket y recibe los resultados."""
import argparse
import itertools
import random
import socket
import threading
import time
from collections import Counter

from .comun import Conexion

NUMERICOS = {"fibonacci": int, "es_primo": int, "dormir": float}


class ClienteTareas:
    """Envía tareas sin esperar: los resultados llegan asincrónicamente (pueden
    volver en otro orden), y se asocian a cada tarea por su id."""

    def __init__(self, host="127.0.0.1", puerto=9000, al_recibir=None):
        self.con = Conexion(socket.create_connection((host, puerto), timeout=10))
        self.con.sock.settimeout(None)
        self.al_recibir = al_recibir  # callback(tarea_enviada, respuesta, segundos)
        self.resultados = []          # [(tarea_enviada, respuesta, segundos)]
        self.cerrado = False
        self._ids = itertools.count(1)
        self._pendientes = {}         # id -> (tipo, datos, instante de envío)
        self._cv = threading.Condition()
        threading.Thread(target=self._recibir, daemon=True).start()

    def enviar(self, tipo, datos=None):
        id_ = next(self._ids)
        with self._cv:
            self._pendientes[id_] = (tipo, datos, time.time())
        self.con.enviar({"id": id_, "tipo": tipo, "datos": datos})
        return id_

    def esperar(self, timeout=30):
        """Espera a que lleguen todos los resultados. True si no queda ninguno pendiente."""
        with self._cv:
            self._cv.wait_for(lambda: not self._pendientes or self.cerrado, timeout)
            return not self._pendientes

    def _recibir(self):
        try:
            while True:
                respuesta = self.con.recibir()
                if respuesta is None:
                    break
                with self._cv:
                    info = self._pendientes.pop(respuesta.get("id"), None)
                if info is not None:
                    tipo, datos, t0 = info
                    registro = ((tipo, datos), respuesta, time.time() - t0)
                    self.resultados.append(registro)
                    if self.al_recibir:
                        self.al_recibir(*registro)
                with self._cv:
                    self._cv.notify_all()
        except (OSError, ValueError):
            pass
        finally:
            with self._cv:
                self.cerrado = True
                self._cv.notify_all()

    def cerrar(self):
        self.con.cerrar()


def formatear(enviada, respuesta, segundos):
    tipo, datos = enviada
    texto = repr(datos) if len(repr(datos)) <= 30 else repr(datos)[:27] + "..."
    if respuesta.get("ok"):
        valor = repr(respuesta.get("resultado"))
        valor = valor if len(valor) <= 60 else valor[:57] + "..."
        return f"  #{respuesta['id']:<3} {tipo}({texto}) = {valor}  [{respuesta.get('worker')}, {segundos:.2f}s]"
    return f"  #{respuesta.get('id')} {tipo}({texto}) ERROR: {respuesta.get('error')}"


def enviar_lote(cliente, cantidad, semilla=None):
    """Envía `cantidad` tareas variadas y espera todos los resultados."""
    azar = random.Random(semilla)
    for _ in range(cantidad):
        tipo = azar.choice(["fibonacci", "es_primo", "invertir_texto", "dormir"])
        if tipo == "fibonacci":
            datos = azar.randint(100, 900)
        elif tipo == "es_primo":
            datos = azar.randint(10**9, 10**10)
        elif tipo == "invertir_texto":
            datos = azar.choice(["sistemas distribuidos", "redes y comunicaciones", "hola mundo"])
        else:
            datos = round(azar.uniform(0.3, 1.2), 1)
        cliente.enviar(tipo, datos)
    return cliente.esperar(60)


def resumen(cliente):
    # solo las respuestas que realmente ejecutó un worker (el comando "estado" no tiene worker)
    por_worker = Counter(r["worker"] for _, r, _ in cliente.resultados if r.get("ok") and r.get("worker"))
    errores = sum(1 for _, r, _ in cliente.resultados if not r.get("ok"))
    print("\nResumen: " + ", ".join(f"{w}: {n} tarea(s)" for w, n in sorted(por_worker.items()))
          + (f" | errores: {errores}" if errores else ""))


AYUDA = """Comandos:
  fibonacci N        ej: fibonacci 300
  es_primo N         ej: es_primo 1000000007
  invertir_texto T   ej: invertir_texto hola mundo
  dormir S           ej: dormir 2        (simula una tarea lenta)
  lote N             envía N tareas variadas de una vez
  estado             muestra los workers conectados y su carga
  ayuda | salir"""


def interactivo(cliente):
    print(AYUDA)
    while True:
        try:
            linea = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not linea:
            continue
        comando, _, resto = linea.partition(" ")
        if comando in ("salir", "exit", "quit"):
            break
        if comando == "ayuda":
            print(AYUDA)
        elif comando == "estado":
            cliente.enviar("estado")
        elif comando == "lote":
            try:
                n = int(resto)
            except ValueError:
                print("  uso: lote N")
                continue
            enviar_lote(cliente, n)
            resumen(cliente)
        elif comando in NUMERICOS:
            try:
                cliente.enviar(comando, NUMERICOS[comando](resto))
            except ValueError:
                print(f"  {comando} requiere un número")
        elif comando == "invertir_texto":
            cliente.enviar(comando, resto)
        else:
            print("  comando desconocido (escribí 'ayuda')")
    if not cliente.esperar(10):
        print("(quedaron tareas sin respuesta)")


def mostrar(enviada, respuesta, segundos):
    if enviada[0] == "estado":
        print("  estado:", respuesta.get("resultado"))
    else:
        print(formatear(enviada, respuesta, segundos))


def main():
    p = argparse.ArgumentParser(description="Cliente del sistema distribuido")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--puerto", type=int, default=9000, help="puerto de clientes del servidor")
    p.add_argument("--lote", type=int, metavar="N", help="enviar N tareas variadas y salir")
    args = p.parse_args()
    try:
        cliente = ClienteTareas(args.host, args.puerto, al_recibir=mostrar)
    except OSError as error:
        print(f"No se pudo conectar a {args.host}:{args.puerto} ({error}). ¿Está corriendo el servidor?")
        return
    if args.lote:
        enviar_lote(cliente, args.lote)
        resumen(cliente)
    else:
        interactivo(cliente)
    cliente.cerrar()


if __name__ == "__main__":
    main()
