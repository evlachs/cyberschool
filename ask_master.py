#!/usr/bin/env python3
"""
"Спросить мастера" — отдельный инструмент, не часть станции. Запускается СВОИМ процессом
в соседнем окне терминала, рядом с уже работающей станцией:
    python3 ask_master.py --config configs/pk4.json
или без конфига, если известны параметры напрямую:
    python3 ask_master.py --station pk4 --master-host 127.0.0.1 --master-port 7000

Раньше `ask` был встроенной командой в консоли самой станции (station.py) — но там она
перемешивалась с живой лентой входящих сообщений, которая печатается из фонового потока
в любой момент, в том числе пока вы набираете вопрос. Вынесено в отдельный скрипт именно
для того, чтобы весь диалог с мастером — и вопрос, и ответ — оставался в ОДНОМ окне.

Для этого скрипт держит СВОЁ отдельное постоянное соединение с мастером и регистрируется
там как "окно вопросов" для station_id (`type=register_ask`), а не как сама станция —
поэтому не может вытеснить или оборвать настоящее соединение станции с мастером (у мастера
это два независимых реестра: `stations` и `ask_sockets`). Ответ мастера (`event <id> <текст>`)
теперь приходит именно в это окно, а не в консоль станции.
"""
import argparse
import json
import os
import queue
import socket
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import protocol

# На Windows консоль по умолчанию не в UTF-8 — без этого print() кириллицы может упасть.
# line_buffering=True — иначе ответ мастера, напечатанный из фонового потока (_reader),
# долетит до экрана только когда вы сами что-то введёте (тот же баг, что чинили в station.py).
sys.stdout.reconfigure(line_buffering=True, encoding="utf-8")


class AskLink:
    """Постоянное соединение с мастером для одного окна "спросить мастера"."""

    def __init__(self, station_id, role, master_host, master_port):
        self.station_id = station_id
        self.role = role
        self.master_host = master_host
        self.master_port = master_port
        self.sock = None
        self.out_q = queue.Queue()
        self.connected = False

    def start(self):
        threading.Thread(target=self._connect_loop, daemon=True).start()

    def _connect_loop(self):
        while True:
            try:
                self.sock = socket.create_connection((self.master_host, self.master_port), timeout=5)
                self.sock.settimeout(None)
                self.connected = True
                print(f"[связь] подключено к мастеру {self.master_host}:{self.master_port}\n")
                protocol.send_json(self.sock, protocol.make_master_report(
                    self.station_id, self.role, "register_ask",
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
                print(f"[связь] нет связи с мастером ({e}), повтор через 3с...")
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
                print(f"\n########## ОТВЕТ МАСТЕРА ##########\n{evt.get('text')}\n####################################\n")
        self.connected = False
        try:
            self.sock.close()
        except Exception:
            pass

    def ask(self, text):
        report = protocol.make_master_report(self.station_id, self.role, "hint_request", text=text)
        self.out_q.put(report)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="config.json станции — возьмём station_id/master_host/master_port оттуда")
    ap.add_argument("--station", help="station_id (если не передаёте --config)")
    ap.add_argument("--master-host", help="адрес мастера (если не передаёте --config)")
    ap.add_argument("--master-port", type=int, help="порт мастера (если не передаёте --config)")
    args = ap.parse_args()

    if args.config:
        with open(args.config, encoding="utf-8") as f:
            config = json.load(f)
        station_id = config["station_id"]
        role = config.get("role", "?")
        master_host = config["master_host"]
        master_port = config["master_port"]
    elif args.station and args.master_host and args.master_port:
        station_id, role = args.station, "?"
        master_host, master_port = args.master_host, args.master_port
    else:
        print("нужно указать либо --config config.json, либо все три: "
              "--station --master-host --master-port")
        sys.exit(1)

    print(f"=== Спросить мастера (от имени {station_id}) ===")
    link = AskLink(station_id, role, master_host, master_port)
    link.start()
    print("Наберите текст вопроса и нажмите Enter. Пустая строка или Ctrl+D — выход.\n")

    try:
        while True:
            try:
                text = input("вопрос> ").strip()
            except EOFError:
                break
            if not text:
                break
            link.ask(text)
            print("отправлено мастеру, ответ появится здесь же\n")
    except KeyboardInterrupt:
        pass
    print("Выход.")


if __name__ == "__main__":
    main()
