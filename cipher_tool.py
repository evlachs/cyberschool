#!/usr/bin/env python3
"""
Шифратор/дешифратор для крота (роль impostor) — отдельный ручной инструмент,
не часть станции. Запуск:
    python3 cipher_tool.py

Прогоняет текст через тот же Цезарь/XOR, что используют шифроузлы pk2/pk6
(common/crypto.py — тот же код, тот же результат). Нужен для ручного режима
крота (station.py, роль impostor, команда `manual on`): сначала подготовить
здесь зашифрованную или расшифрованную подмену, потом вставить её в
`release <id> <текст>` на самой станции.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import crypto

# На Windows консоль по умолчанию не в UTF-8 — без этого print() кириллицы может упасть.
sys.stdout.reconfigure(encoding="utf-8")

ALGORITHMS = {"1": ("caesar", "Цезарь"), "2": ("xor", "XOR")}


def pick_algorithm():
    while True:
        print("\nАлгоритм:")
        print("  1) Цезарь (сдвиг по алфавиту, ключ — целое число)")
        print("  2) XOR (ключ — произвольная строка)")
        choice = input("Выбор [1/2]: ").strip()
        if choice in ALGORITHMS:
            return ALGORITHMS[choice]
        print("Не понял, введите 1 или 2.")


def pick_key(algorithm):
    if algorithm == "caesar":
        while True:
            raw = input("Ключ (сдвиг, целое число, например 13): ").strip()
            try:
                return int(raw)
            except ValueError:
                print("Нужно целое число.")
    else:
        while True:
            raw = input("Ключ (любая непустая строка): ").strip()
            if raw:
                return raw
            print("Ключ не может быть пустым.")


def pick_mode():
    while True:
        choice = input("Зашифровать или расшифровать? [e/d]: ").strip().lower()
        if choice in ("e", "з", "encrypt", "зашифровать"):
            return "encrypt"
        if choice in ("d", "р", "decrypt", "расшифровать"):
            return "decrypt"
        print("Введите e (зашифровать) или d (расшифровать).")


def transform(algorithm, mode, key, text):
    if algorithm == "caesar":
        return crypto.caesar_encrypt(text, key) if mode == "encrypt" else crypto.caesar_decrypt(text, key)
    return crypto.xor_encrypt(text, key) if mode == "encrypt" else crypto.xor_decrypt(text, key)


def main():
    print("=== Шифратор/дешифратор крота ===")
    print("Считает тем же шифром, что и шифроузлы pk2/pk6 — готовьте здесь подмену, "
          "вставляйте результат в `release <id> <текст>` на станции ПК4.")
    try:
        while True:
            algo_key, algo_name = pick_algorithm()
            key = pick_key(algo_key)
            print(f"\nАлгоритм: {algo_name}, ключ: {key!r}")
            while True:
                mode = pick_mode()
                text = input("Сообщение: ")
                result = transform(algo_key, mode, key, text)
                print(f"\nРезультат: {result}\n")

                choice = input(
                    "Enter — ещё сообщение этим же ключом, "
                    "s — сменить алгоритм/ключ, q — выход: "
                ).strip().lower()
                if choice == "q":
                    return
                if choice == "s":
                    break
    except (EOFError, KeyboardInterrupt):
        print("\nВыход.")


if __name__ == "__main__":
    main()
