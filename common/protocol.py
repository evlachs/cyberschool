"""
Общий протокол связи для игры "Перехват".
Формат сообщений: JSON, один объект на строку (newline-delimited JSON) поверх TCP.
"""
import json
import socket
import uuid
from datetime import datetime


def now_iso():
    return datetime.now().strftime("%H:%M:%S")


def new_msg_id():
    return uuid.uuid4().hex[:8]


def send_json(sock: socket.socket, obj: dict):
    """Отправить один JSON-объект, завершённый переводом строки."""
    data = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    sock.sendall(data)


def recv_json_lines(sock: socket.socket):
    """
    Генератор: читает сокет и отдаёт распарсенные JSON-объекты по мере поступления строк.
    Завершается, когда соединение закрыто.
    """
    buf = b""
    while True:
        try:
            chunk = sock.recv(4096)
        except OSError:
            break
        if not chunk:
            break
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                continue


def make_chain_message(msg_id, origin, hop_from, direction, phase, content, meta=None):
    """Сообщение, идущее по цепочке узлов. direction: 'right' (от pk1 к pk7) или 'left' (от pk7 к pk1)."""
    return {
        "msg_id": msg_id,
        "origin": origin,
        "hop_from": hop_from,
        "direction": direction,
        "phase": phase,
        "content": content,
        "meta": meta or {},
        "ts": now_iso(),
    }


def make_master_report(station_id, role, kind, **fields):
    """Отчёт станции мастеру (для дашборда)."""
    report = {
        "type": kind,  # register | log | heartbeat
        "station": station_id,
        "role": role,
        "ts": now_iso(),
    }
    report.update(fields)
    return report


def make_master_event(text, target="all"):
    """Событие, которое мастер вбрасывает станциям."""
    return {"type": "event", "target": target, "text": text, "ts": now_iso()}


def send_chain_message(host, port, msg, timeout=3.0):
    """Открыть короткое соединение к следующему узлу и отправить одно сообщение."""
    with socket.create_connection((host, port), timeout=timeout) as s:
        send_json(s, msg)
