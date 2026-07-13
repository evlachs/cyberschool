"""
Крипто-утилиты для игры "Перехват": шифр Цезаря, XOR, хэш целостности.
Цезарь поддерживает кириллицу, латиницу и цифры; остальные символы не трогает.
"""
import base64
import hashlib

RU_UPPER = "АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"
RU_LOWER = RU_UPPER.lower()
EN_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
EN_LOWER = EN_UPPER.lower()
DIGITS = "0123456789"


def _shift_in_alphabet(ch, shift, alphabet):
    idx = alphabet.index(ch)
    return alphabet[(idx + shift) % len(alphabet)]


def caesar_transform(text: str, shift: int) -> str:
    out = []
    for ch in text:
        if ch in RU_UPPER:
            out.append(_shift_in_alphabet(ch, shift, RU_UPPER))
        elif ch in RU_LOWER:
            out.append(_shift_in_alphabet(ch, shift, RU_LOWER))
        elif ch in EN_UPPER:
            out.append(_shift_in_alphabet(ch, shift, EN_UPPER))
        elif ch in EN_LOWER:
            out.append(_shift_in_alphabet(ch, shift, EN_LOWER))
        elif ch in DIGITS:
            out.append(_shift_in_alphabet(ch, shift, DIGITS))
        else:
            out.append(ch)
    return "".join(out)


def caesar_encrypt(text: str, shift: int) -> str:
    return caesar_transform(text, shift)


def caesar_decrypt(text: str, shift: int) -> str:
    return caesar_transform(text, -shift)


def xor_encrypt(text: str, key: str) -> str:
    """XOR потока байт с повторяющимся ключом, результат в base64 (чтобы влезть в JSON)."""
    data = text.encode("utf-8")
    kb = key.encode("utf-8")
    out = bytes(b ^ kb[i % len(kb)] for i, b in enumerate(data))
    return base64.b64encode(out).decode("ascii")


def xor_decrypt(b64text: str, key: str) -> str:
    try:
        data = base64.b64decode(b64text.encode("ascii"))
    except Exception:
        return "<ошибка base64>"
    kb = key.encode("utf-8")
    out = bytes(b ^ kb[i % len(kb)] for i, b in enumerate(data))
    return out.decode("utf-8", errors="replace")


def content_hash(text: str) -> str:
    """Короткий хэш содержимого для проверки целостности на станциях БД."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
