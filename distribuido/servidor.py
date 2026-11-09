"""Servidor de despacho: recibe tareas de los clientes por socket y las reparte
entre los workers conectados.

Dos puertos TCP:
  - puerto de clientes (9000): los clientes envían tareas y reciben resultados.
  - puerto de workers  (9001): los workers se registran y reciben tareas.

Política de reparto: se elige el worker con menor carga relativa
(tareas pendientes / hilos del pool). Si un worker se desconecta, sus tareas
pendientes se reasignan a otro (semántica "al menos una vez": una tarea podría
ejecutarse dos veces, por eso conviene que las tareas sean idempotentes).
"""
import argparse
import itertools
import logging
import socket
import threading

from .comun import Conexion

log = logging.getLogger("servidor")

MAX_INTENTOS = 3  # veces que se reasigna una tarea si los workers se caen


class Worker:
    """Estado de un worker conectado, visto desde el servidor."""

    def __init__(self, nombre, hilos, con):
        self.nombre = nombre
        self.hilos = hilos
        self.con = con
        self.pendientes = set()  # ids internos de tareas en curso en este worker
        self.listo = False       # True cuando ya recibió la confirmación de registro

    def carga(self):
        return len(self.pendientes) / self.hilos


class Servidor:
    def __init__(self, host="127.0.0.1", puerto_clientes=9000, puerto_workers=9001):
        self.host = host
        self.puerto_clientes = puerto_clientes
        self.puerto_workers = puerto_workers
        self._lock = threading.Lock()  # protege workers y tareas
        self.workers = {}              # nombre -> Worker
        self.tareas = {}               # id interno -> dict con el estado de la tarea
        self._ids = itertools.count(1)
        self._cerrar = threading.Event()
        self._escuchas = []

    # ------------------------------------------------------------ arranque
    def iniciar(self):
        """Abre los puertos y lanza los hilos de aceptación (no bloquea)."""
        s_clientes = self._escuchar(self.puerto_clientes)
        s_workers = self._escuchar(self.puerto_workers)
        self.puerto_clientes = s_clientes.getsockname()[1]  # útil si se pidió el puerto 0
        self.puerto_workers = s_workers.getsockname()[1]
        self._escuchas = [s_clientes, s_workers]
        for sock, atender in ((s_clientes, self._atender_cliente), (s_workers, self._atender_worker)):
            threading.Thread(target=self._aceptar, args=(sock, atender), daemon=True).start()
        log.info("escuchando clientes en %s:%d y workers en %s:%d",
                 self.host, self.puerto_clientes, self.host, self.puerto_workers)

    def _escuchar(self, puerto):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, puerto))
        sock.listen()
        return sock

    def _aceptar(self, escucha, atender):
        while not self._cerrar.is_set():
            try:
                sock, direccion = escucha.accept()
            except OSError:
                break  # se cerró el socket de escucha
            threading.Thread(target=atender, args=(Conexion(sock), direccion), daemon=True).start()

    def servir(self):
        self.iniciar()
        try:
            while not self._cerrar.wait(1):
                pass
        except KeyboardInterrupt:
            log.info("apagando servidor")
        finally:
            self.detener()

    def detener(self):
        self._cerrar.set()
        for sock in self._escuchas:
            sock.close()
        with self._lock:
            workers = list(self.workers.values())
        for w in workers:
            w.con.cerrar()

    # ------------------------------------------------------------- workers
    def _atender_worker(self, con, direccion):
        worker = None
        try:
            con.sock.settimeout(10)  # el registro debe llegar rápido
            hola = con.recibir()
            con.sock.settimeout(None)
            if not isinstance(hola, dict) or hola.get("rol") != "worker":
                con.enviar({"ok": False, "error": "se esperaba el registro de un worker"})
                return
            hilos = hola.get("hilos")
            if isinstance(hilos, bool) or not isinstance(hilos, int) or not 1 <= hilos <= 64:
                hilos = 1
            worker = self._registrar_worker(str(hola.get("nombre") or f"worker-{direccion[1]}")[:40],
                                            hilos, con)
            con.enviar({"ok": True, "nombre": worker.nombre})
            worker.listo = True  # recién ahora puede recibir tareas (después de la confirmación)
            log.info("worker %s conectado (%d hilos)", worker.nombre, worker.hilos)
            while True:
                mensaje = con.recibir()
                if mensaje is None:
                    break
                self._resultado(worker, mensaje)
        except (OSError, ValueError):
            pass
        finally:
            if worker:
                self._quitar_worker(worker)
            con.cerrar()

    def _registrar_worker(self, nombre, hilos, con):
        with self._lock:
            final, n = nombre, 1
            while final in self.workers:  # nombres repetidos: se agrega un sufijo
                n += 1
                final = f"{nombre}-{n}"
            worker = Worker(final, hilos, con)
            self.workers[final] = worker
            return worker

    def _quitar_worker(self, worker):
        with self._lock:
            if self.workers.get(worker.nombre) is not worker:
                return  # otro hilo ya lo quitó
            del self.workers[worker.nombre]
            huerfanas = list(worker.pendientes)
            worker.pendientes.clear()
            for gid in huerfanas:
                if gid in self.tareas:
                    self.tareas[gid]["worker"] = None
        log.warning("worker %s desconectado; reasignando %d tarea(s)", worker.nombre, len(huerfanas))
        for gid in huerfanas:
            self.despachar(gid)

    # ------------------------------------------------------------ despacho
    def despachar(self, gid):
        """Asigna la tarea al worker menos cargado y se la envía."""
        error = None
        with self._lock:
            tarea = self.tareas.get(gid)
            if tarea is None:
                return  # el cliente se fue o ya se respondió
            tarea["intentos"] += 1
            disponibles = [w for w in self.workers.values() if w.listo]
            if tarea["intentos"] > MAX_INTENTOS:
                error = "la tarea falló demasiadas veces (workers caídos)"
            elif not disponibles:
                error = "no hay workers disponibles"
            else:
                worker = min(disponibles, key=Worker.carga)
                tarea["worker"] = worker
                worker.pendientes.add(gid)
            if error:
                del self.tareas[gid]
        if error:
            self._responder(tarea, {"ok": False, "error": error})
            return
        try:
            worker.con.enviar({"id": gid, "tipo": tarea["tipo"], "datos": tarea["datos"]})
            log.info("tarea %d (%s) -> %s", gid, tarea["tipo"], worker.nombre)
        except OSError:
            self._quitar_worker(worker)  # reasigna también esta tarea

    def _resultado(self, worker, mensaje):
        gid = mensaje.get("id") if isinstance(mensaje, dict) else None
        if not isinstance(gid, int):
            return
        with self._lock:
            tarea = self.tareas.get(gid)
            if tarea is None or tarea["worker"] is not worker:
                return  # resultado viejo o duplicado: se ignora
            del self.tareas[gid]
            worker.pendientes.discard(gid)
        respuesta = {"ok": bool(mensaje.get("ok")), "worker": worker.nombre}
        if respuesta["ok"]:
            respuesta["resultado"] = mensaje.get("resultado")
        else:
            respuesta["error"] = str(mensaje.get("error", "error desconocido"))
        self._responder(tarea, respuesta)

    @staticmethod
    def _responder(tarea, respuesta):
        respuesta["id"] = tarea["id_cliente"]
        try:
            tarea["cliente"].enviar(respuesta)
        except OSError:
            pass  # el cliente ya no está

    # ------------------------------------------------------------ clientes
    def _atender_cliente(self, con, direccion):
        log.info("cliente conectado: %s:%d", *direccion)
        try:
            while True:
                try:
                    mensaje = con.recibir()
                except ValueError:
                    con.enviar({"id": None, "ok": False, "error": "JSON inválido"})
                    continue
                if mensaje is None:
                    break
                if not isinstance(mensaje, dict) or "id" not in mensaje or not isinstance(mensaje.get("tipo"), str):
                    id_ = mensaje.get("id") if isinstance(mensaje, dict) else None
                    con.enviar({"id": id_, "ok": False, "error": "formato inválido: se requieren 'id' y 'tipo'"})
                    continue
                if mensaje["tipo"] == "estado":
                    con.enviar({"id": mensaje["id"], "ok": True, "resultado": self.estado()})
                    continue
                gid = next(self._ids)
                with self._lock:
                    self.tareas[gid] = {"cliente": con, "id_cliente": mensaje["id"],
                                        "tipo": mensaje["tipo"], "datos": mensaje.get("datos"),
                                        "worker": None, "intentos": 0}
                self.despachar(gid)
        except OSError:
            pass
        finally:
            self._limpiar_cliente(con)
            con.cerrar()
            log.info("cliente desconectado: %s:%d", *direccion)

    def _limpiar_cliente(self, con):
        """Descarta las tareas de un cliente que se fue (sus resultados ya no importan)."""
        with self._lock:
            for gid, tarea in list(self.tareas.items()):
                if tarea["cliente"] is con:
                    del self.tareas[gid]
                    if tarea["worker"] is not None:
                        tarea["worker"].pendientes.discard(gid)

    def estado(self):
        with self._lock:
            return {
                "workers": [{"nombre": w.nombre, "hilos": w.hilos, "pendientes": len(w.pendientes)}
                            for w in self.workers.values() if w.listo],
                "tareas_en_curso": len(self.tareas),
            }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description="Servidor de despacho del sistema distribuido")
    p.add_argument("--host", default="127.0.0.1", help="usar 0.0.0.0 para aceptar conexiones de otras máquinas")
    p.add_argument("--puerto-clientes", type=int, default=9000)
    p.add_argument("--puerto-workers", type=int, default=9001)
    args = p.parse_args()
    Servidor(args.host, args.puerto_clientes, args.puerto_workers).servir()


if __name__ == "__main__":
    main()
