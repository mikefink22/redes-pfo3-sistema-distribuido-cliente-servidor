"""Pruebas de integración: levantan servidor, workers y cliente en el mismo proceso
(puertos efímeros, sin tocar los puertos reales). Ejecutar: python -m unittest -v"""
import threading
import time
import unittest

from distribuido.cliente import ClienteTareas
from distribuido.servidor import Servidor
from distribuido.tareas import ejecutar
from distribuido.worker import Worker


def esperar(condicion, timeout=5):
    limite = time.time() + timeout
    while time.time() < limite:
        if condicion():
            return True
        time.sleep(0.02)
    return False


class Entorno(unittest.TestCase):
    def setUp(self):
        self.servidor = Servidor("127.0.0.1", 0, 0)
        self.servidor.iniciar()
        self.workers = []
        self.clientes = []

    def tearDown(self):
        for c in self.clientes:
            c.cerrar()
        for w in self.workers:
            w.detener()
        self.servidor.detener()

    def agregar_worker(self, nombre, hilos=2):
        w = Worker("127.0.0.1", self.servidor.puerto_workers, nombre, hilos)
        threading.Thread(target=w.correr, daemon=True).start()
        self.workers.append(w)
        return w

    def esperar_workers(self, n):
        self.assertTrue(esperar(lambda: len(self.servidor.estado()["workers"]) == n),
                        f"se esperaban {n} workers registrados")

    def cliente(self):
        c = ClienteTareas("127.0.0.1", self.servidor.puerto_clientes)
        self.clientes.append(c)
        return c


class TestTareas(unittest.TestCase):
    def test_resultados(self):
        self.assertEqual(ejecutar("fibonacci", 10), 55)
        self.assertTrue(ejecutar("es_primo", 1_000_000_007))
        self.assertFalse(ejecutar("es_primo", 1_000_000_008))
        self.assertEqual(ejecutar("invertir_texto", "hola"), "aloh")

    def test_entradas_invalidas(self):
        for tipo, datos in [("fibonacci", -1), ("fibonacci", "x"), ("fibonacci", True),
                            ("dormir", 999), ("invertir_texto", 5), ("borrar_todo", None)]:
            with self.assertRaises(ValueError, msg=f"{tipo}({datos!r})"):
                ejecutar(tipo, datos)


class TestSistema(Entorno):
    def test_tarea_simple(self):
        self.agregar_worker("w1")
        self.esperar_workers(1)
        c = self.cliente()
        c.enviar("fibonacci", 20)
        self.assertTrue(c.esperar(5))
        _, respuesta, _ = c.resultados[0]
        self.assertTrue(respuesta["ok"])
        self.assertEqual(respuesta["resultado"], 6765)
        self.assertEqual(respuesta["worker"], "w1")

    def test_error_de_tarea_no_tumba_al_worker(self):
        self.agregar_worker("w1")
        self.esperar_workers(1)
        c = self.cliente()
        c.enviar("fibonacci", "no es un número")
        c.enviar("invertir_texto", "sigue vivo")
        self.assertTrue(c.esperar(5))
        por_id = {r["id"]: r for _, r, _ in c.resultados}
        self.assertFalse(por_id[1]["ok"])
        self.assertEqual(por_id[2]["resultado"], "sigue vivo"[::-1])

    def test_sin_workers(self):
        c = self.cliente()
        c.enviar("fibonacci", 5)
        self.assertTrue(c.esperar(5))
        _, respuesta, _ = c.resultados[0]
        self.assertFalse(respuesta["ok"])
        self.assertIn("no hay workers", respuesta["error"])

    def test_reparte_la_carga_entre_workers(self):
        for nombre in ("w1", "w2", "w3"):
            self.agregar_worker(nombre, hilos=2)
        self.esperar_workers(3)
        c = self.cliente()
        for _ in range(12):
            c.enviar("dormir", 0.3)
        self.assertTrue(c.esperar(10))
        usados = {r["worker"] for _, r, _ in c.resultados}
        self.assertEqual(usados, {"w1", "w2", "w3"})
        self.assertEqual(len(c.resultados), 12)

    def test_los_hilos_del_pool_trabajan_en_paralelo(self):
        self.agregar_worker("w1", hilos=4)
        self.esperar_workers(1)
        c = self.cliente()
        inicio = time.time()
        for _ in range(4):
            c.enviar("dormir", 0.5)
        self.assertTrue(c.esperar(10))
        self.assertLess(time.time() - inicio, 1.5, "4 tareas de 0.5 s deberían tardar ~0.5 s, no 2 s")

    def test_reasigna_tareas_si_un_worker_se_cae(self):
        w1 = self.agregar_worker("w1", hilos=2)
        self.agregar_worker("w2", hilos=2)
        self.esperar_workers(2)
        c = self.cliente()
        for _ in range(4):
            c.enviar("dormir", 1.0)
        time.sleep(0.3)      # las tareas ya están repartidas
        w1.detener()         # se cae un worker con tareas en curso
        self.assertTrue(c.esperar(15))
        self.assertEqual(len(c.resultados), 4)
        self.assertTrue(all(r["ok"] for _, r, _ in c.resultados))
        self.assertEqual({r["worker"] for _, r, _ in c.resultados}, {"w2"})

    def test_mensajes_invalidos(self):
        c = self.cliente()
        c.con.sock.sendall(b"esto no es json\n")
        c.con.sock.sendall(b'{"tipo": 5}\n')
        time.sleep(0.2)  # el servidor responde con errores y no cierra la conexión
        # el servidor sigue vivo y responde al comando de estado
        c.enviar("estado")
        self.assertTrue(c.esperar(5))
        estado = [r for e, r, _ in c.resultados if e[0] == "estado"][0]
        self.assertTrue(estado["ok"])

    def test_resumen_con_comando_estado(self):
        # regresión: "estado" no tiene worker y rompía el ordenamiento del resumen
        from contextlib import redirect_stdout
        from io import StringIO
        from distribuido.cliente import resumen
        self.agregar_worker("w1")
        self.esperar_workers(1)
        c = self.cliente()
        c.enviar("estado")
        c.enviar("fibonacci", 7)
        self.assertTrue(c.esperar(5))
        salida = StringIO()
        with redirect_stdout(salida):
            resumen(c)
        self.assertIn("w1: 1 tarea(s)", salida.getvalue())

    def test_nombres_de_worker_repetidos(self):
        self.agregar_worker("mismo")
        self.agregar_worker("mismo")
        self.esperar_workers(2)
        nombres = {w["nombre"] for w in self.servidor.estado()["workers"]}
        self.assertEqual(len(nombres), 2)


if __name__ == "__main__":
    unittest.main()
