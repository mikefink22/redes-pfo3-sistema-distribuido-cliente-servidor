"""Worker: se conecta al servidor por socket y ejecuta tareas con un pool de hilos."""
import argparse
import logging
import socket
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
        id_tarea = tarea.get("id") if isinstance(tarea, dict) else None
        try:
            resultado = ejecutar(tarea.get("tipo"), tarea.get("datos"))
            respuesta = {"id": id_tarea, "ok": True, "resultado": resultado}
        except Exception as error:  # un error de la tarea no debe tumbar al worker
            respuesta = {"id": id_tarea, "ok": False, "error": str(error)}
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
