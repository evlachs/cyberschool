#!/usr/bin/env python3
"""
Панель мастера игры "Перехват".
Запуск:  python3 master.py --port 7000
Требует: pip install rich
"""
import argparse
import os
import socket
import sys
import threading
import time
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import protocol

# На Windows консоль по умолчанию не в UTF-8 (cp866/cp1251) — символы вроде «‼», «…»
# роняют print() с UnicodeEncodeError. На Linux/уже-UTF-8 консолях это no-op.
sys.stdout.reconfigure(encoding="utf-8")

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()
lock = threading.Lock()
stations = {}          # station_id -> dict(role, display_name, status, socket, last_action, last_ts)
ask_sockets = {}        # station_id -> [socket, ...] окон ask_master.py, открытых для этой станции
events = deque(maxlen=300)
hint_requests = deque(maxlen=50)
dirty = threading.Event()


def handle_station(conn, addr):
    station_id = None
    ask_for = None
    for msg in protocol.recv_json_lines(conn):
        mtype = msg.get("type")
        changed = False
        with lock:
            if mtype == "register":
                station_id = msg["station"]
                old = stations.get(station_id)
                if old and old.get("socket") is not None and old["socket"] is not conn:
                    try:
                        old["socket"].close()
                    except Exception:
                        pass
                stations[station_id] = {
                    "role": msg.get("role", "?"),
                    "display_name": msg.get("display_name", station_id),
                    "status": "online",
                    "socket": conn,
                    "addr": addr,
                    "last_action": "-",
                    "last_ts": msg.get("ts", "-"),
                }
                changed = True
            elif mtype == "register_ask":
                # ask_master.py — отдельное окно "спросить мастера", НЕ сама станция.
                # Регистрируется в своём реестре, не в stations — поэтому не может
                # вытеснить настоящее соединение станции (и не показывается в таблице статусов).
                ask_for = msg["station"]
                ask_sockets.setdefault(ask_for, []).append(conn)
            elif mtype == "heartbeat":
                sid = msg.get("station")
                if sid in stations and stations[sid]["socket"] is conn:
                    # heartbeat подтверждает "online" каждые 4с — панель перерисовываем,
                    # только если статус реально был другим (см. HANDOFF про flapping),
                    # а не на каждый heartbeat, иначе панель дёргается непрерывно.
                    if stations[sid]["status"] != "online":
                        changed = True
                    stations[sid]["status"] = "online"
                    stations[sid]["last_ts"] = msg.get("ts", "-")
            elif mtype == "log":
                sid = msg.get("station")
                if sid in stations and stations[sid]["socket"] is conn:
                    stations[sid]["last_action"] = msg.get("action", "-")
                    stations[sid]["last_ts"] = msg.get("ts", "-")
                events.append(msg)
                changed = True
            elif mtype == "hint_request":
                hint_requests.append(msg)
                changed = True
        if changed:
            dirty.set()
    # соединение закрыто — гасим "online" только если это всё ещё ТА САМАЯ (актуальная)
    # регистрация станции, а не протухший старый коннект, вытесненный новым
    with lock:
        if station_id and station_id in stations and stations[station_id]["socket"] is conn:
            stations[station_id]["status"] = "offline"
        if ask_for and conn in ask_sockets.get(ask_for, []):
            ask_sockets[ask_for].remove(conn)
            if not ask_sockets[ask_for]:
                del ask_sockets[ask_for]
    dirty.set()


def accept_loop(host, port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(50)
    console.print(f"[bold green]слушаю подключения станций на {host}:{port}[/bold green]")
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_station, args=(conn, addr), daemon=True).start()


def render():
    table = Table(title="Станции", expand=True)
    table.add_column("ID")
    table.add_column("Роль")
    table.add_column("Статус")
    table.add_column("Последнее действие")
    table.add_column("Время")
    with lock:
        for sid, info in sorted(stations.items()):
            style = "green" if info["status"] == "online" else "red"
            table.add_row(
                sid, info["role"], f"[{style}]{info['status']}[/{style}]",
                str(info["last_action"]), str(info["last_ts"]),
            )
        recent = list(events)[-16:]
        recent_hints = list(hint_requests)[-10:]

    lines = []
    for e in reversed(recent):
        in_p = e.get("in_preview") or "—"
        out_p = e.get("out_preview") or "—"
        lines.append(f"{e.get('station')}: «{in_p}» -> «{out_p}»")
    panel = Panel("\n".join(lines) or "пока нет событий", title="Живая лента трафика (последние 16)")

    hint_lines = [f"[{h.get('ts')}] {h.get('station')}: {h.get('text')}" for h in reversed(recent_hints)]
    hint_panel = Panel(
        "\n".join(hint_lines) or "пока нет запросов",
        title="Запросы подсказок (последние 10)", border_style="yellow",
    )

    console.clear()
    console.print(table)
    console.print(panel)
    console.print(hint_panel)
    console.print("Команды: event all <текст> | event <id_станции> <текст> | list | quit")


def redraw_loop():
    render()
    while True:
        dirty.wait()
        dirty.clear()
        time.sleep(0.3)
        render()


def send_event(target, text):
    msg = protocol.make_master_event(text, target)
    with lock:
        if target == "all":
            # "all" — это широковещательное объявление станциям (например тревога об утечке),
            # окна "спросить мастера" его не получают, это отдельный личный канал вопрос-ответ.
            items = list(stations.items())
            ask_conns = []
        else:
            ask_conns = list(ask_sockets.get(target, []))
            # если для этой станции сейчас открыто окно ask_master.py — ответ уходит ТУДА
            # и только туда, а не в консоль самой станции: весь диалог должен остаться в
            # одном окне. Если окно не открыто — обычное поведение, событие идёт станции.
            items = [] if ask_conns else [(target, stations.get(target))]
    sent = 0
    for sid, info in items:
        if not info or info["status"] != "online":
            continue
        try:
            protocol.send_json(info["socket"], msg)
            sent += 1
        except Exception:
            pass
    for conn in ask_conns:
        try:
            protocol.send_json(conn, msg)
            sent += 1
        except Exception:
            pass
    console.print(f"[master] событие отправлено {sent} станциям/окнам")


def command_loop():
    while True:
        try:
            line = input("master> ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()
        if cmd == "quit":
            break
        elif cmd == "list":
            with lock:
                for sid, info in stations.items():
                    print(f"  {sid}: {info['role']} [{info['status']}]")
        elif cmd == "event":
            if len(parts) < 3:
                print("использование: event all <текст>  или  event <id_станции> <текст>")
                continue
            send_event(parts[1], parts[2])
        else:
            print("неизвестная команда. доступно: event, list, quit")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=7000)
    args = ap.parse_args()
    threading.Thread(target=accept_loop, args=(args.host, args.port), daemon=True).start()
    threading.Thread(target=redraw_loop, daemon=True).start()
    command_loop()


if __name__ == "__main__":
    main()
