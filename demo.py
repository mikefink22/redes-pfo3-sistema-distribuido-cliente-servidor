"""Demo todo-en-uno: levanta el servidor, 3 workers y un cliente en un solo comando.

    python demo.py            # 12 tareas repartidas entre 3 workers
    python demo.py --falla    # además, un worker se cae a mitad y sus tareas se reasignan

Útil para sacar las capturas de pantalla del trabajo. Para correr cada pieza por
separado (como en un despliegue real) ver el README.
"""
import argparse
import logging
import threading
import time

from distribuido.cliente import ClienteTareas, enviar_lote, mostrar, resumen
from distribuido.servidor import Servidor
from distribuido.worker import Worker


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tareas", type=int, default=12)
    p.add_argument("--falla", action="store_true", help="simula la caída de un worker")
    args = p.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
    servidor = Servidor("127.0.0.1", 0, 0)  # puertos libres, no choca con nada
    servidor.iniciar()

    workers = []
    for i in range(1, 4):
        w = Worker("127.0.0.1", servidor.puerto_workers, f"worker-{i}", hilos=2)
        threading.Thread(target=w.correr, daemon=True).start()
        workers.append(w)
    while len(servidor.estado()["workers"]) < 3:
        time.sleep(0.05)

    print("\n=== Estado inicial ===")
    print(servidor.estado())

    print(f"\n=== Enviando {args.tareas} tareas ===")
    cliente = ClienteTareas("127.0.0.1", servidor.puerto_clientes, al_recibir=mostrar)
    if args.falla:
        threading.Timer(0.5, lambda: (print("\n*** se cae worker-2 ***\n"), workers[1].detener())).start()
    ok = enviar_lote(cliente, args.tareas, semilla=1)
    resumen(cliente)
    print("Todas las tareas respondidas." if ok else "Faltaron respuestas.")
    cliente.cerrar()
    servidor.detener()


if __name__ == "__main__":
    main()
