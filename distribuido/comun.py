"""Protocolo común: mensajes JSON, uno por línea ("JSON lines"), sobre TCP.

Un socket TCP es un flujo de bytes sin límites de mensaje, así que se usa el
salto de línea como delimitador. Cada mensaje es un objeto JSON en UTF-8.
"""
import json
import socket
import threading

MAX_LINEA = 1_000_000  # bytes; protege al servidor de mensajes gigantes


class Conexion:
    """Envuelve un socket: enviar() es seguro entre hilos, recibir() lee una línea."""

    def __init__(self, sock):
        self.sock = sock
        self._lector = sock.makefile("rb")
        self._lock_envio = threading.Lock()

    def enviar(self, objeto):
        datos = (json.dumps(objeto, ensure_ascii=False) + "\n").encode("utf-8")
        with self._lock_envio:  # varios hilos pueden responder por el mismo socket
            self.sock.sendall(datos)

    def recibir(self):
        """Devuelve un dict/valor JSON, o None si el otro extremo cerró.

        Lanza ValueError si la línea no es JSON válido (la conexión sigue
        usable) y OSError si el mensaje excede MAX_LINEA (hay que cerrar).
        """
        linea = self._lector.readline(MAX_LINEA + 1)
        if not linea:
            return None
        if len(linea) > MAX_LINEA:
            raise OSError("mensaje demasiado largo")
        return json.loads(linea)

    def cerrar(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)  # despierta a quien esté leyendo
        except OSError:
            pass
        self.sock.close()
