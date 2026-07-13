import os
import subprocess
import sys
import time
import threading
import queue
import re

ROOT = os.path.dirname(os.path.abspath(__file__))

# Станции ждут 2с перед каждой пересылкой (см. station.py, FORWARD_DELAY_SECONDS) —
# на полном маршруте pk1->pk7 (6 хопов) это 12+ секунд на одну отправку. В тесте это
# ни на что не влияет по сути, только замедляет прогон, поэтому здесь отключаем.
STATION_ENV = dict(os.environ, PEREHVAT_FORWARD_DELAY="0")

procs = {}
out_queues = {}


def reader(name, proc):
    for line in proc.stdout:
        out_queues[name].put(line.rstrip("\n"))


def start(name, cmd, env=None):
    p = subprocess.Popen(
        cmd, cwd=ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, bufsize=1, env=env,
        encoding="utf-8",  # без этого декодирование pipe зависит от локали хоста (на Windows не UTF-8)
    )
    procs[name] = p
    out_queues[name] = queue.Queue()
    threading.Thread(target=reader, args=(name, p), daemon=True).start()
    return p


def send(name, line):
    procs[name].stdin.write(line + "\n")
    procs[name].stdin.flush()


def drain(name, wait=0.3):
    time.sleep(wait)
    lines = []
    while not out_queues[name].empty():
        lines.append(out_queues[name].get())
    return lines


def restart_cipher_pair(pk2_cfg, pk6_cfg):
    procs["pk2"].terminate(); procs["pk6"].terminate()
    time.sleep(0.3)
    start("pk2", [sys.executable, "station.py", "--config", f"configs/{pk2_cfg}"], env=STATION_ENV)
    start("pk6", [sys.executable, "station.py", "--config", f"configs/{pk6_cfg}"], env=STATION_ENV)
    time.sleep(1)
    print("PK2 после перезапуска:", drain("pk2", 0.2))


try:
    start("master", [sys.executable, "master.py", "--port", "7000"])
    time.sleep(1)

    configs = {
        "pk1": "pk1.json", "pk2": "pk2_open.json", "pk3": "pk3.json",
        "pk4": "pk4.json", "pk5": "pk5.json", "pk6": "pk6_open.json", "pk7": "pk7.json",
    }
    for pk, cfg in configs.items():
        start(pk, [sys.executable, "station.py", "--config", f"configs/{cfg}"], env=STATION_ENV)
        time.sleep(0.3)

    time.sleep(1.5)
    print("=== все процессы стартовали ===")

    # --- Фаза 0: связь в обе стороны (пока шифр не активирован) ---
    send("pk1", "send ping-right")
    time.sleep(0.5)
    out = drain("pk7")
    assert any("ping-right" in l for l in out), "pk1->pk7 ping не дошёл!"
    print("OK: pk1 -> pk7 (direction=right) прошло")

    send("pk7", "send ping-left")
    time.sleep(0.5)
    out = drain("pk1")
    assert any("ping-left" in l for l in out), "pk7->pk1 ping не дошёл!"
    print("OK: pk7 -> pk1 (direction=left) прошло — та же цепочка, обратная сторона")

    # --- Ручной режим крота: сообщение должно застрять на pk4, пока его не отпустят ---
    send("pk4", "manual on")
    time.sleep(0.2)
    send("pk1", "send держи меня")
    time.sleep(0.5)
    out = drain("pk7")
    assert not any("держи меня" in l for l in out), \
        "сообщение прошло автоматически, хотя ручной режим на pk4 должен был его задержать!"
    drain("pk4")
    send("pk4", "pending")
    time.sleep(0.3)
    pending_out = drain("pk4")
    msg_id = next((l.strip().split()[0].split("=")[1] for l in pending_out if l.strip().startswith("id=")), None)
    assert msg_id, f"не нашли id ожидающего сообщения в pending: {pending_out}"
    send("pk4", f"release {msg_id} перехвачено и подменено вручную")
    time.sleep(0.6)
    out = drain("pk7")
    print("PK7 после ручного release:", out)
    assert any("перехвачено и подменено вручную" in l for l in out), "ручной release с подменой не дошёл!"
    assert not any("держи меня" in l for l in out), "оригинальный текст всё же дошёл — release не подменил!"
    print("OK: ручной режим крота — сообщение задержано и отпущено с подменой вручную")

    send("pk4", "manual off")
    time.sleep(0.2)
    send("pk1", "send снова автоматом")
    time.sleep(0.5)
    out = drain("pk7")
    assert any("снова автоматом" in l for l in out), "после manual off автоматическая пересылка не восстановилась!"
    print("OK: manual off восстанавливает автоматическую пересылку")

    # --- Фаза 2: Цезарь, проверяем ОБЕ стороны через один и тот же конфиг pk2/pk6 ---
    restart_cipher_pair("pk2_caesar_example.json", "pk6_caesar_example.json")

    send("pk1", "phase cipher1")
    send("pk1", "send Привет от точки А")
    time.sleep(0.6)
    out = drain("pk7")
    print("PK7 (Цезарь, A->B):", out)
    assert any("Привет от точки А" in l for l in out), "Цезарь A->B не совпал!"
    print("OK: Цезарь A->B round-trip совпал")

    send("pk7", "phase cipher1")
    send("pk7", "send Привет от точки Б")
    time.sleep(0.6)
    out = drain("pk1")
    print("PK1 (Цезарь, B->A):", out)
    assert any("Привет от точки Б" in l for l in out), "Цезарь B->A не совпал!"
    print("OK: Цезарь B->A round-trip совпал — те же станции, тот же конфиг, оба направления работают")

    # импостор должен видеть шифротекст в обе стороны
    send("pk4", "list 5")
    time.sleep(0.3)
    pk4_out = drain("pk4")
    print("PK4 видит (обе стороны):", pk4_out)

    # --- Фаза 3: XOR, снова обе стороны ---
    restart_cipher_pair("pk2_xor_example.json", "pk6_xor_example.json")

    send("pk1", "phase cipher2")
    send("pk1", "send 55.7558, 37.6173 объект Дельта")
    time.sleep(0.6)
    out = drain("pk7")
    assert any("55.7558, 37.6173 объект Дельта" in l for l in out), "XOR A->B не совпал!"
    print("OK: XOR A->B round-trip совпал")

    send("pk7", "phase cipher2")
    send("pk7", "send подтверждение получения координат")
    time.sleep(0.6)
    out = drain("pk1")
    assert any("подтверждение получения координат" in l for l in out), "XOR B->A не совпал!"
    print("OK: XOR B->A round-trip совпал")

    # --- Проверка hash-логов на БД в обе стороны ---
    send("pk3", "log 10")
    send("pk5", "log 10")
    time.sleep(0.4)
    print("PK3 log:", drain("pk3"))
    print("PK5 log:", drain("pk5"))

    # --- crack_caesar на перехваченном сообщении ---
    msg_id = None
    for l in pk4_out:
        m = re.search(r"id=(\w+)", l)
        if m:
            msg_id = m.group(1)
    if msg_id:
        send("pk4", f"crack_caesar {msg_id} 13")
        time.sleep(0.3)
        print("PK4 crack:", drain("pk4"))

    # --- tamper_next всё ещё работает при двунаправленной схеме ---
    restart_cipher_pair("pk2_caesar_example.json", "pk6_caesar_example.json")
    send("pk4", "tamper_next ПОДМЕНА")
    time.sleep(0.2)
    send("pk1", "send оригинал")
    time.sleep(0.6)
    out = drain("pk7")
    print("PK7 после tamper:", out)
    assert any("ПОЛУЧЕНО" in l for l in out) and not any("оригинал" in l for l in out), \
        "tamper_next не сработал в двунаправленной схеме!"
    print("OK: tamper_next по-прежнему работает")

    # --- msg: pk3<->pk5 напрямую (в обход цепочки), свободный текст, без автосверки ---
    send("pk3", "msg хэш для id=abc123 у меня 74949de281c3, сверь у себя")
    time.sleep(0.5)
    out = drain("pk5")
    print("PK5 после msg от pk3:", out)
    assert any("СООБЩЕНИЕ ОТ pk3" in l for l in out) and any("74949de281c3" in l for l in out), \
        "сообщение от pk3 не дошло до pk5!"
    print("OK: pk3 -> pk5 прямое сообщение (в обход цепочки) дошло")

    send("pk5", "msg у меня тоже 74949de281c3, совпадает")
    time.sleep(0.5)
    out = drain("pk3")
    print("PK3 после msg от pk5:", out)
    assert any("СООБЩЕНИЕ ОТ pk5" in l for l in out), "ответное сообщение от pk5 не дошло до pk3!"
    print("OK: pk5 -> pk3 прямое сообщение дошло — канал двусторонний")

    print("\n=== ВСЕ ПРОВЕРКИ ПРОШЛИ УСПЕШНО (включая оба направления) ===")

finally:
    for name, p in procs.items():
        try:
            p.terminate()
        except Exception:
            pass
    time.sleep(0.5)
    for name, p in procs.items():
        try:
            p.kill()
        except Exception:
            pass
