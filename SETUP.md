# Роли станций — раздаточные инструкции по командам

Инструкция для каждой станции — в отдельном файле, чтобы можно было выдать команде ровно её
собственную страницу и ничего лишнего. Каждый файл: легенда роли, практическая задача, шаги
запуска и список команд (для cipher — вместо команд раздел «Смена шифра»).

| Станция | Роль | Файл | Конфиг |
|---|---|---|---|
| ПК1 | endpoint («Точка А») | [`setup/pk1.md`](setup/pk1.md) | `configs/endpoint.json` |
| ПК2 | cipher («Шифроузел А») | [`setup/pk2.md`](setup/pk2.md) | `configs/cipher.json` |
| ПК3 | database («База данных А») | [`setup/pk3.md`](setup/pk3.md) | `configs/database.json` |
| ПК4 | impostor («Крот») | [`setup/pk4.md`](setup/pk4.md) | `configs/impostor.json` |
| ПК5 | database («База данных Б») | [`setup/pk5.md`](setup/pk5.md) | `configs/database.json` |
| ПК6 | cipher («Шифроузел Б») | [`setup/pk6.md`](setup/pk6.md) | `configs/cipher.json` |
| ПК7 | endpoint («Точка Б») | [`setup/pk7.md`](setup/pk7.md) | `configs/endpoint.json` |

Конфиг называется по роли, не по номеру станции — у каждой команды он свой локальный файл на
своём ПК, совпадение имён между разными командами не проблема.

**Важно:** сами шаблоны `configs/endpoint.json` / `cipher.json` / `database.json` / `impostor.json`
(с пропущенными полями под IP-адреса) в репозитории пока не созданы — их нужно подготовить
отдельно и выдать командам до занятия.

Ведущему для запуска панели мастера — см. README.md.
