#!/usr/bin/env python3
"""
Станция игры "Перехват". Запуск:
    python3 station.py --config configs/pk2.json

Только стандартная библиотека — на станциях ничего дополнительно ставить не нужно.

Топология — симметричная линия из 7 узлов:
    pk1(endpoint) - pk2(cipher) - pk3(database) - pk4(impostor) - pk5(database) - pk6(cipher) - pk7(endpoint)

Зеркальные пары (pk1/pk7, pk2/pk6, pk3/pk5) используют РОВНО ОДИН И ТОТ ЖЕ код роли —
разница только в конфиге (какая сторона "своя", какая "чужая"). Каждое сообщение несёт
поле direction: "right" (движется от pk1 к pk7) или "left" (движется от pk7 к pk1).
Любой узел получает сообщение с одной стороны и пересылает его дальше в ТУ ЖЕ сторону,
на противоположного соседа.
"""
import argparse
import json
import os
import queue
import socket
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import protocol, crypto

# Сообщения из фоновых потоков (приём по цепочке, связь с мастером) должны долетать
# до консоли сразу, а не только когда input() в главном потоке промптует заново —
# при выводе не в терминал (пайп, тестовый харнесс) Python иначе буферизует блоками.
# encoding="utf-8" — без этого на Windows (кодировка консоли по умолчанию cp866/cp1251)
# print() символов вроде «‼», «…» роняет станцию с UnicodeEncodeError; на Linux это no-op.
sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")

# Задержка перед пересылкой сообщения на следующий узел — чтобы участники видели,
# как сообщение реально идёт по цепочке шаг за шагом, а не телепортируется мгновенно.
# Оверрайд переменной окружения — иначе test_harness.py гонял бы полный маршрут
# (6 хопов pk1->pk7) по 12+ секунд на каждую отправку.
FORWARD_DELAY_SECONDS = float(os.environ.get("PEREHVAT_FORWARD_DELAY", "2"))

state_lock = threading.Lock()


def preview(text, n=60):
    text = text if isinstance(text, str) else str(text)
    return text if len(text) <= n else text[:n] + "…"


# ---------------------------------------------------------------------------
# Связь с мастером игры
# ---------------------------------------------------------------------------
class MasterLink:
    def __init__(self, config):
        self.config = config
        self.sock = None
        self.out_q = queue.Queue()
        self.connected = False

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True).start()
        threading.Thread(target=self._heartbeat_loop, daemon=True).start()

    def _heartbeat_loop(self):
        while True:
            threading.Event().wait(4)
            self.out_q.put(protocol.make_master_report(
                self.config["station_id"], self.config["role"], "heartbeat",
            ))

    def _connect_loop(self):
        while True:
            try:
                self.sock = socket.create_connection(
                    (self.config["master_host"], self.config["master_port"]), timeout=5
                )
                self.sock.settimeout(None)  # таймаут нужен только на этапе connect, не на постоянное чтение
                self.connected = True
                print(f"[master] подключено к панели мастера "
                      f"{self.config['master_host']}:{self.config['master_port']}")
                protocol.send_json(self.sock, protocol.make_master_report(
                    self.config["station_id"], self.config["role"], "register",
                    display_name=self.config.get("display_name", self.config["station_id"]),
                ))
                threading.Thread(target=self._reader, daemon=True).start()
                self._writer()
            except Exception as e:
                self.connected = False
                if self.sock:
                    try:
                        self.sock.close()
                    except Exception:
                        pass
                print(f"[master] нет связи с мастером ({e}), повтор через 3с...")
                threading.Event().wait(3)

    def _writer(self):
        while self.connected:
            item = self.out_q.get()
            try:
                protocol.send_json(self.sock, item)
            except Exception:
                self.connected = False
                self.out_q.put(item)  # вернуть в очередь, попробуем после реконнекта
                try:
                    self.sock.close()
                except Exception:
                    pass
                return

    def _reader(self):
        for evt in protocol.recv_json_lines(self.sock):
            if evt.get("type") == "event":
                print(f"\n########## СОБЫТИЕ ОТ МАСТЕРА ##########\n{evt.get('text')}\n#########################################\n")
        self.connected = False
        try:
            self.sock.close()
        except Exception:
            pass

    def log(self, **fields):
        report = protocol.make_master_report(
            self.config["station_id"], self.config["role"], "log", **fields
        )
        self.out_q.put(report)


# ---------------------------------------------------------------------------
# Куда пересылать: единая логика для всех ролей с "соседями"
# ---------------------------------------------------------------------------
def get_target(config, direction):
    """Вернуть (host, port) следующего узла для данного направления."""
    role = config["role"]
    if role == "endpoint":
        return config.get("peer_host"), config.get("peer_port")
    if direction == "right":
        return config.get("peer_right_host"), config.get("peer_right_port")
    else:
        return config.get("peer_left_host"), config.get("peer_left_port")


def forward(config, direction, msg_id, origin, phase, content, meta):
    if direction is None:
        return
    host, port = get_target(config, direction)
    if not host or not port:
        return
    time.sleep(FORWARD_DELAY_SECONDS)
    out = protocol.make_chain_message(
        msg_id=msg_id, origin=origin, hop_from=config["station_id"],
        direction=direction, phase=phase, content=content, meta=meta,
    )
    try:
        protocol.send_chain_message(host, port, out)
    except Exception as e:
        print(f"[!] Не удалось переслать узлу {host}:{port}: {e}")


# ---------------------------------------------------------------------------
# Обработчики входящих сообщений по ролям
# возвращают (outgoing_content_or_None, extra_meta, action_label, forward_direction_or_None)
# ---------------------------------------------------------------------------
def h_endpoint(msg, state, config):
    state.setdefault("inbox", []).append(msg)
    print(f"\n>>> ПОЛУЧЕНО СООБЩЕНИЕ: {msg['content']}\n")
    return None, {}, "доставлено (конец цепочки для этого сообщения)", None


def h_cipher(msg, state, config):
    content = msg["content"]
    direction = msg["direction"]
    algo, key = state.get("algorithm"), state.get("key")
    if not algo:
        return content, {}, "passthrough (шифр не активирован)", direction

    is_encrypt = (direction == config.get("encrypt_direction"))
    if algo == "caesar":
        out = crypto.caesar_encrypt(content, key) if is_encrypt else crypto.caesar_decrypt(content, key)
    elif algo == "xor":
        out = crypto.xor_encrypt(content, key) if is_encrypt else crypto.xor_decrypt(content, key)
    else:
        out = content
        action = "неизвестный алгоритм, passthrough"
        return out, {}, action, direction

    action = f"{'зашифровано' if is_encrypt else 'расшифровано'} ({algo})"
    return out, {"algorithm": algo}, action, direction


def h_database(msg, state, config):
    h = crypto.content_hash(msg["content"])
    state.setdefault("log", []).append({
        "msg_id": msg["msg_id"], "hash": h, "content": msg["content"],
        "phase": msg.get("phase"), "direction": msg.get("direction"),
    })
    return msg["content"], {"hash": h}, f"залогировано, hash={h}", msg["direction"]


# ---------------------------------------------------------------------------
# Прямая связь между двумя БД (pk3/pk5), в обход цепочки — свободный обмен
# текстовыми сообщениями (например, чтобы продиктовать друг другу хэш конкретного
# msg_id и сверить его глазами, а не автоматически). Никакой логики сравнения тут
# нет специально — сверяют сами операторы, читая сообщения друг друга.
# ---------------------------------------------------------------------------
def db_message_send(config, text):
    host, port = config.get("db_peer_host"), config.get("db_peer_port")
    if not host or not port:
        print("db_peer_host/db_peer_port не заданы в конфиге — сообщение соседней БД не отправить")
        return
    payload = {"type": "db_message", "from": config["station_id"], "text": text}
    try:
        protocol.send_chain_message(host, port, payload)
        print(f"[{protocol.now_iso()}] -> соседней БД: «{text}»")
    except Exception as e:
        print(f"[!] не удалось связаться с соседней БД {host}:{port}: {e}")


def handle_db_message(msg):
    print(f"\n>>> СООБЩЕНИЕ ОТ {msg.get('from')}: {msg.get('text')}\n")


def h_impostor(msg, state, config):
    state.setdefault("captured", []).append({
        "msg_id": msg["msg_id"], "content": msg["content"],
        "phase": msg.get("phase"), "direction": msg.get("direction"),
    })
    if state.get("manual_mode"):
        # В ручном режиме сообщение НЕ пересылается автоматически — оседает в pending,
        # оператор сам решает через release <id> [текст], когда и в каком виде его отпустить.
        state.setdefault("pending", {})[msg["msg_id"]] = msg
        return None, {}, "получено, ждёт решения оператора (ручной режим)", None
    outgoing = msg["content"]
    action = "перехвачено, переслано без изменений"
    tamper = state.get("tamper_next")
    if tamper is not None:
        outgoing = tamper
        state["tamper_next"] = None
        action = "‼ ПОДМЕНЕНО перед пересылкой"
    return outgoing, {}, action, msg["direction"]


ROLE_HANDLERS = {
    "endpoint": h_endpoint,
    "cipher": h_cipher,
    "database": h_database,
    "impostor": h_impostor,
}


# ---------------------------------------------------------------------------
# Приём по цепочке
# ---------------------------------------------------------------------------
def process_incoming(msg, config, state, master_link):
    handler = ROLE_HANDLERS[config["role"]]
    with state_lock:
        outgoing, meta, action, out_direction = handler(msg, state, config)
    # На консоль — содержимое целиком (без preview): оператору, особенно кроту, важно видеть
    # сообщение полностью, чтобы решить, подменять ли его. Урезаем только то, что уходит на
    # панель мастера (см. master_link.log ниже) — там компактность нужна намеренно.
    if outgoing is not None and outgoing != msg["content"]:
        print(f"[{protocol.now_iso()}] от {msg.get('hop_from')} | до: «{msg['content']}» "
              f"после: «{outgoing}» -> {action}")
    else:
        print(f"[{protocol.now_iso()}] от {msg.get('hop_from')} | «{msg['content']}» -> {action}")
    master_link.log(
        action=action, in_preview=preview(msg["content"]),
        out_preview=preview(outgoing) if outgoing is not None else None,
    )
    if outgoing is not None and out_direction is not None:
        combined_meta = {**msg.get("meta", {}), **meta}
        forward(config, out_direction, msg["msg_id"], msg.get("origin", config["station_id"]),
                msg.get("phase", "?"), outgoing, combined_meta)


def chain_server(config, state, master_link):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((config["listen_host"], config["listen_port"]))
    srv.listen(50)
    print(f"[chain] слушаю {config['listen_host']}:{config['listen_port']}")
    while True:
        conn, _addr = srv.accept()
        threading.Thread(target=_handle_conn, args=(conn, config, state, master_link), daemon=True).start()


def _handle_conn(conn, config, state, master_link):
    with conn:
        for msg in protocol.recv_json_lines(conn):
            if msg.get("type") == "db_message":
                handle_db_message(msg)
            else:
                process_incoming(msg, config, state, master_link)


# ---------------------------------------------------------------------------
# Консольные команды (человек-оператор станции)
# ---------------------------------------------------------------------------
def cmd_send(args, state, config, master_link):
    if not args:
        print("использование: send <текст сообщения>")
        return
    text = " ".join(args)
    phase = state.get("phase", "open")
    direction = config["send_direction"]
    msg_id = protocol.new_msg_id()
    print(f"[{protocol.now_iso()}] отправляю: «{text}»")
    master_link.log(action="отправлено оператором", in_preview=None, out_preview=preview(text))
    forward(config, direction, msg_id, config["station_id"], phase, text, {})


def cmd_phase(args, state, config, master_link):
    if not args:
        print(f"текущая фаза: {state.get('phase', 'open')}")
        return
    state["phase"] = args[0]
    print(f"фаза установлена: {args[0]}")


def cmd_db_log(args, state, config, master_link):
    log = state.get("log", [])
    n = int(args[0]) if args else 10
    print(f"--- последние {n} записей журнала ({len(log)} всего) ---")
    for entry in log[-n:]:
        print(f"  id={entry['msg_id']} hash={entry['hash']} [{entry['phase']}/{entry['direction']}] "
              f"«{preview(entry['content'])}»")


def cmd_db_message(args, state, config, master_link):
    if not args:
        print("использование: msg <текст> — отправить сообщение соседней БД (pk3<->pk5)")
        return
    db_message_send(config, " ".join(args))


def cmd_impostor_list(args, state, config, master_link):
    cap = state.get("captured", [])
    n = int(args[0]) if args else 10
    print(f"--- перехвачено {len(cap)} сообщений, последние {n} ---")
    for entry in cap[-n:]:
        print(f"  id={entry['msg_id']} [{entry['phase']}/{entry['direction']}] «{entry['content']}»")


def cmd_impostor_show(args, state, config, master_link):
    if not args:
        print("использование: show <msg_id>")
        return
    for entry in state.get("captured", []):
        if entry["msg_id"] == args[0]:
            print(f"id={entry['msg_id']} [{entry['phase']}/{entry['direction']}]\n{entry['content']}")
            return
    print("не найдено")


def cmd_crack_caesar(args, state, config, master_link):
    if not args:
        print("использование: crack_caesar <msg_id> [сдвиг]")
        return
    entry = next((e for e in state.get("captured", []) if e["msg_id"] == args[0]), None)
    if not entry:
        print("сообщение с таким id не перехвачено")
        return
    if len(args) >= 2:
        shift = int(args[1])
        print(f"сдвиг {shift}: {crypto.caesar_decrypt(entry['content'], shift)}")
    else:
        print("перебор сдвигов 1..32:")
        for shift in range(1, 33):
            print(f"  {shift:>2}: {crypto.caesar_decrypt(entry['content'], shift)}")


def cmd_crack_xor(args, state, config, master_link):
    if len(args) < 2:
        print("использование: crack_xor <msg_id> <предполагаемый ключ>")
        return
    entry = next((e for e in state.get("captured", []) if e["msg_id"] == args[0]), None)
    if not entry:
        print("сообщение с таким id не перехвачено")
        return
    print(f"ключ '{args[1]}': {crypto.xor_decrypt(entry['content'], args[1])}")


def cmd_tamper_next(args, state, config, master_link):
    if not args:
        print("использование: tamper_next <новый текст> (подменит СЛЕДУЮЩЕЕ пришедшее сообщение)")
        return
    if state.get("manual_mode"):
        print("сейчас включён ручной режим (manual on) — используйте release <id> <текст> "
              "вместо tamper_next, там видно содержимое до решения")
        return
    state["tamper_next"] = " ".join(args)
    print("подмена подготовлена, сработает на следующем входящем сообщении")


def cmd_impostor_manual(args, state, config, master_link):
    if not args:
        print(f"ручной режим: {'ВКЛЮЧЁН' if state.get('manual_mode') else 'выключен'}")
        return
    val = args[0].lower()
    if val in ("on", "вкл"):
        state["manual_mode"] = True
        print("ручной режим ВКЛЮЧЁН: входящие сообщения теперь оседают в pending "
              "и ждут вашей команды release, автоматической пересылки больше нет")
    elif val in ("off", "выкл"):
        state["manual_mode"] = False
        pending = state.get("pending", {})
        if pending:
            print(f"ручной режим ВЫКЛЮЧЕН. Внимание: {len(pending)} сообщений всё ещё в pending "
                  f"и НЕ дойдут до адресата сами — отпустите их (release <id> или release_all)")
        else:
            print("ручной режим ВЫКЛЮЧЕН: входящие снова пересылаются автоматически")
    else:
        print("использование: manual on | manual off")


def cmd_impostor_pending(args, state, config, master_link):
    pending = state.get("pending", {})
    if not pending:
        print("нет сообщений, ожидающих решения")
        return
    print(f"--- ожидают решения ({len(pending)}) ---")
    for mid, m in pending.items():
        print(f"  id={mid} [{m.get('phase')}/{m.get('direction')}] «{m['content']}»")


def _impostor_release_one(mid, state, config, master_link, replacement=None):
    m = state.get("pending", {}).pop(mid, None)
    if not m:
        print(f"нет id={mid} среди ожидающих")
        return
    if replacement is not None:
        outgoing = replacement
        action = "‼ ПОДМЕНЕНО вручную перед пересылкой"
    else:
        outgoing = m["content"]
        action = "переслано вручную без изменений"
    print(f"[{protocol.now_iso()}] отпускаю id={mid}: «{outgoing}» -> {action}")
    master_link.log(action=action, in_preview=None, out_preview=preview(outgoing))
    forward(config, m["direction"], mid, m.get("origin", config["station_id"]),
            m.get("phase", "?"), outgoing, m.get("meta", {}))


def cmd_impostor_release(args, state, config, master_link):
    if not args:
        print("использование: release <msg_id> [новый текст вместо оригинала]")
        return
    mid, replacement = args[0], (" ".join(args[1:]) if len(args) > 1 else None)
    _impostor_release_one(mid, state, config, master_link, replacement)


def cmd_impostor_release_all(args, state, config, master_link):
    pending = list(state.get("pending", {}).keys())
    if not pending:
        print("нет сообщений, ожидающих решения")
        return
    for mid in pending:
        _impostor_release_one(mid, state, config, master_link)
    print(f"отпущено без изменений: {len(pending)}")


def cmd_inbox(args, state, config, master_link):
    inbox = state.get("inbox", [])
    n = int(args[0]) if args else 10
    print(f"--- входящие ({len(inbox)} всего), последние {n} ---")
    for m in inbox[-n:]:
        print(f"  [{m.get('phase')}/{m.get('direction')}] «{m['content']}»")


ROLE_COMMANDS = {
    "endpoint": {"send": cmd_send, "phase": cmd_phase, "inbox": cmd_inbox},
    "cipher": {},
    "database": {"log": cmd_db_log, "msg": cmd_db_message},
    "impostor": {
        "list": cmd_impostor_list, "show": cmd_impostor_show,
        "crack_caesar": cmd_crack_caesar, "crack_xor": cmd_crack_xor,
        "tamper_next": cmd_tamper_next, "manual": cmd_impostor_manual,
        "pending": cmd_impostor_pending, "release": cmd_impostor_release,
        "release_all": cmd_impostor_release_all,
    },
}

ROLE_HELP = {
    "endpoint": ("send <текст>   — отправить сообщение соседу\n"
                 "phase <имя>    — пометить текущую фазу (open/cipher1/keyexchange/cipher2)\n"
                 "inbox [n]      — показать последние n полученных сообщений"),
    "cipher": ("Шифр задаётся только конфигом при запуске (cipher_type/cipher_key/encrypt_direction).\n"
               "Чтобы сменить шифр: остановите станцию (quit), отредактируйте config.json, "
               "запустите заново:\n  python3 station.py --config config.json"),
    "database": ("log [n]      — показать последние n записей журнала\n"
                 "msg <текст>  — отправить сообщение соседней БД (pk3<->pk5, напрямую, в обход цепочки) "
                 "— например, продиктовать хэш конкретного msg_id, чтобы сверить его вручную"),
    "impostor": ("list [n]                — перехваченные сообщения\n"
                 "show <msg_id>           — показать сообщение целиком\n"
                 "crack_caesar <id> [сдвиг] — расшифровать/перебрать сдвиги Цезаря\n"
                 "crack_xor <id> <ключ>    — попробовать XOR-ключ\n"
                 "tamper_next <текст>     — подменить следующее пришедшее сообщение (только в автоматическом режиме)\n"
                 "manual [on|off]         — ручной режим: входящие не пересылаются сами, ждут вашего release\n"
                 "pending                 — показать сообщения, ожидающие решения (в ручном режиме)\n"
                 "release <id> [текст]    — отпустить сообщение: без текста — как есть, с текстом — подмена\n"
                 "release_all             — отпустить все ожидающие без изменений"),
}


def console_loop(config, state, master_link):
    role = config["role"]
    print(f"\n=== СТАНЦИЯ {config['station_id']} ({config.get('display_name', role)}) ===")
    print(f"Роль: {role}")
    print("Команды: state | help | quit")
    print(f"Подсказка у мастера — отдельным скриптом в соседнем окне: "
          f"python3 ask_master.py --config {config.get('_config_path', 'config.json')}")
    if role in ROLE_HELP:
        print(ROLE_HELP[role])
    print()
    commands = ROLE_COMMANDS.get(role, {})
    while True:
        try:
            line = input(f"{config['station_id']}> ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split()
        cmd, args = parts[0].lower(), parts[1:]
        if cmd == "quit":
            break
        if cmd == "help":
            print(ROLE_HELP.get(role, "нет команд для этой роли"))
            continue
        if cmd == "state":
            with state_lock:
                print({k: v for k, v in state.items() if k not in ("log", "captured", "inbox")})
            continue
        handler = commands.get(cmd)
        if not handler:
            print(f"неизвестная команда '{cmd}'. Наберите help.")
            continue
        with state_lock:
            handler(args, state, config, master_link)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()
    with open(args.config, encoding="utf-8") as f:
        config = json.load(f)
    config["_config_path"] = args.config

    cipher_type = config.get("cipher_type", "open")
    algorithm = None if cipher_type in (None, "open") else cipher_type
    key = config.get("cipher_key")
    state = {"algorithm": algorithm, "key": key, "phase": "open"}

    if config["role"] == "cipher":
        if algorithm:
            print(f"[cipher] активный режим из конфига: {algorithm}, ключ={key!r}, "
                  f"шифрует направление={config.get('encrypt_direction')}")
        else:
            print("[cipher] активный режим из конфига: open (шифрование выключено, passthrough)")

    master_link = MasterLink(config)
    master_link.start()

    threading.Thread(target=chain_server, args=(config, state, master_link), daemon=True).start()

    console_loop(config, state, master_link)


if __name__ == "__main__":
    main()
