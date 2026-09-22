# Admin Review removal — PR-3 (docs)

**Дата:** 2026-09-22
**Ветка:** `cleanup/remove-admin-review`
**База:** PR-2 `8186156`

## Scope

Только документация — **без** изменений Python, конфигов, тестов.

## Обновлённые файлы

| Файл | Изменения |
|------|-----------|
| `docs/FUNCTIONAL_CONTRACT.md` | Удалён § Admin Review; renumber 14–20; окно 46–60 без «review off»; убрана строка матрицы Admin review; legacy `approved_by_admin` / state keys в monitor+persist |
| `docs/ARCHITECTURE_AUDIT.md` | Убраны review daemons; main loop без review bypass; legacy EVAL lambda; follow-up без review daemons |
| `docs/CORRECTNESS_FINDINGS.md` | C1/H4 **Resolved** + исторический текст; колонка Status; M1/H4 audit note; приоритеты без C1 |
| `docs/ADMIN_REVIEW_REMOVAL_AUDIT.md` | журнал PR-1…PR-3 |

## Не затронуто

- `wide_research` `TERMINAL_REVIEW_*` (отдельная feature, см. `WIDE_RESEARCH.md`)
- Канал: daily stats, permanent instruction, ordinary Telegram (§13 contract)
- Исторические «Сигнал от админа» в monitor при `approved_by_admin`

## CORRECTNESS — C1 / H4

| ID | Resolution |
|----|------------|
| **C1** | Unreachable review band — **устранено удалением** feature (не band fix) |
| **H4** | Second publish path — **устранено удалением** `publish_signal_to_channel` |

Первоначальные формулировки Actual/Risk сохранены под пометкой *historical*.

## Проверки

```text
git diff --check  →  OK
PYTHONPATH=. python3 -m pytest -q --tb=short  →  894 passed
```

## Коммит

`docs: complete admin review removal documentation` (см. `git log -1` на ветке). Push / restart бота — нет.
