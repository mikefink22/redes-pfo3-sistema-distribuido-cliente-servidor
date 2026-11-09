"""Worker: se conecta al servidor por socket y ejecuta tareas con un pool de hilos."""
import argparse
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .comun import Conexion
from .tareas import ejecutar

log = logging.getLogger("worker")


class Worker:
    def __init__(self, host="127.0.0.1", puerto=9001, nombre=None, hilos=4):
        self.host = host
        self.puerto = puerto
        self.nombre = nombre or f"worker-{id(self) % 10000}"
        self.hilos = hilos
        self.con = None
        self.pool = ThreadPoolExecutor(max_workers=hilos, thread_name_prefix=self.nombre)

    def correr(self):
        """Bloquea atendiendo tareas hasta que el servidor cierra o se llama detener()."""
        sock = socket.create_connection((self.host, self.puerto), timeout=10)
        sock.settimeout(None)
        self.con = Conexion(sock)
        self.con.enviar({"rol": "worker", "nombre": self.nombre, "hilos": self.hilos})
        respuesta = self.con.recibir()
        if not isinstance(respuesta, dict) or not respuesta.get("ok"):
            raise ConnectionError(f"el servidor rechazó el registro: {respuesta}")
        self.nombre = respuesta["nombre"]
        log.info("registrado como %s con %d hilos", self.nombre, self.hilos)
        # La lectura del socket va en otro hilo: en Windows, Ctrl+C no interrumpe
        # un recv() bloqueado, así que el hilo principal espera con timeout.
        lector = threading.Thread(target=self._leer, daemon=True)
        lector.start()
        try:
            while lector.is_alive():
                lector.join(0.5)
        except KeyboardInterrupt:
            log.info("cerrando %s (Ctrl+C)", self.nombre)
            self.detener()
            lector.join(2)

    def _leer(self):
        """Recibe tareas y las manda al pool hasta que se cierra la conexión."""
        try:
            while True:
                tarea = self.con.recibir()
                if tarea is None:
                    break
                self.pool.submit(self._procesar, tarea)  # no bloquea el hilo lector
        except (OSError, ValueError):
            pass
        finally:
            log.info("%s desconectado", self.nombre)
            self.pool.shutdown(wait=False, cancel_futures=True)
            self.con.cerrar()

    def _procesar(self, tarea):
        if not isinstance(tarea, dict):
            tarea = {}
        id_tarea = tarea.get("id")
        hilo = threading.current_thread().name  # ej: worker-1_0 (hilo 0 del pool)
        datos = repr(tarea.get("datos"))
        datos = datos if len(datos) <= 30 else datos[:27] + "..."
        log.info("[%s] tarea %s: %s(%s) ...", hilo, id_tarea, tarea.get("tipo"), datos)
        inicio = time.perf_counter()
        try:
            resultado = ejecutar(tarea.get("tipo"), tarea.get("datos"))
            respuesta = {"id": id_tarea, "ok": True, "resultado": resultado}
            log.info("[%s] tarea %s lista en %.2f s", hilo, id_tarea, time.perf_counter() - inicio)
        except Exception as error:  # un error de la tarea no debe tumbar al worker
            respuesta = {"id": id_tarea, "ok": False, "error": str(error)}
            log.warning("[%s] tarea %s falló: %s", hilo, id_tarea, error)
        try:
            self.con.enviar(respuesta)
        except OSError:
            pass  # el servidor se fue; la tarea se reasignará

    def detener(self):
        if self.con:
            self.con.cerrar()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Worker del sistema distribuido")
    p.add_argument("--host", default="127.0.0.1", help="dirección del servidor")
    p.add_argument("--puerto", type=int, default=9001, help="puerto de workers del servidor")
    p.add_argument("--nombre", default=None)
    p.add_argument("--hilos", type=int, default=4, help="tamaño del pool de hilos")
    args = p.parse_args()
    worker = Worker(args.host, args.puerto, args.nombre, args.hilos)
    try:
        worker.correr()
    except (ConnectionError, OSError) as error:
        log.error("no se pudo trabajar con el servidor: %s", error)
    except KeyboardInterrupt:
        worker.detener()


if __name__ == "__main__":
    main()
