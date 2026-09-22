# Admin Review removal — PR-1 (реализовано, без коммита)

**Дата:** 2026-09-22
**Ветка:** `cleanup/remove-admin-review` (от `cleanup/correctness-fixes`)
**Статус git:** изменения не закоммичены; push не выполнялся; бот не перезапускался.

**Правило проекта:** все аудиты, планы и отчёты по удалению Admin Review дублируются в `docs/ADMIN_REVIEW_REMOVAL_*.md` (этот файл — журнал PR-1).

---

## Scope PR-1 (из `docs/ADMIN_REVIEW_REMOVAL_AUDIT.md`)

Только wiring:

- убрать ветку Admin Review из `main_loop`;
- убрать обход окна 46–60 ради review;
- убрать запуск `review_timeout_daemon`;
- убрать callbacks `review_send` / `review_skip`.

**Не входило в PR-1:** удаление определений `send_review_to_admin`, `publish_signal_to_channel`, `can_auto_post_admin`, daemons, env, docs (PR-2+).

---

## Git до работы

```text
On branch cleanup/correctness-fixes
nothing to commit, working tree clean
```

Создана ветка: `cleanup/remove-admin-review`.

---

## Изменённые файлы

| Файл | Изменение |
|------|-----------|
| `NanoTest.py` | −155 / +2 строк (~157 net) |
| `tests/test_admin_review_removal_pr1.py` | новый (untracked) |

---

## `NanoTest.py` — что удалено

### 1. Startup в `main_loop()`

- `if ENABLE_ADMIN_REVIEW_SIGNALS: start_review_timeout_daemon()`
- `else: logger.info("[REVIEW_DISABLED] ...")`
- закомментированный блок про `start_admin_review_daemon` (удалён вместе с соседним блоком)

### 2. Окно 46–60 — всегда как при «review off»

- `minute_i < REGULAR_SIGNAL_MIN_MINUTE`: убрано `and not ENABLE_ADMIN_REVIEW_SIGNALS`
- `minute_i > REGULAR_SIGNAL_MAX_MINUTE`: убрано `and not ENABLE_ADMIN_REVIEW_SIGNALS` и комментарий про review
- После `collect_match_all`: удалён дублирующий bypass (`minute` вне 46–60 при включённом review)

### 3. Review band в main_loop

Удалён блок:

- `minute >= REVIEW_PIPELINE_MIN_MINUTE and REVIEW_MIN_THRESHOLD <= prob_actual < PROB_SEND_THRESHOLD`
- `send_review_to_admin(...)`, `continue`
- `[REVIEW_DISABLED]` fall-through

**Сохранено:** `compute_lambda_and_probability`, EVAL-логи, `prob_goal_either_to75`; ordinary 45+ path (BASE, champion, snapshots, `send_to_telegram`) без изменений.

### 4. `handle_callback_query`

Удалён весь блок `if callback_data.startswith("review_"):` (~118 строк), включая вызов `publish_signal_to_channel` из `review_send`.

**Сохранено:** analysis UI callbacks, stats/instruction callbacks, monitor/historical `is_admin_approved` в других функциях.

---

## Что намеренно оставлено (PR-2)

- `send_review_to_admin`, `publish_signal_to_channel`, `can_auto_post_admin`
- `review_timeout_daemon`, `admin_review_daemon`, `update_review_tracking`
- константы `ENABLE_ADMIN_REVIEW_SIGNALS`, `REVIEW_*`, state keys `review_queue`, …
- `wide_research` `TERMINAL_REVIEW_*` — не трогалось

---

## Тесты

**Файл:** `tests/test_admin_review_removal_pr1.py`

| Тест | Назначение |
|------|------------|
| `test_main_loop_does_not_call_send_review_to_admin` | AST `main_loop`: нет `send_review_to_admin`, `ENABLE_ADMIN_REVIEW_SIGNALS` |
| `test_main_startup_does_not_start_review_timeout_daemon` | AST `main_loop`: нет `start_review_timeout_daemon` |
| `test_legacy_review_send_callback_does_not_publish` | `review_send` не вызывает `publish_signal_to_channel` / `send_to_telegram` |
| `test_ordinary_resolve_signal_header_unchanged` | обычный заголовок «🚨 Сигнал» |
| `test_ordinary_telegram_delivery_decision_unchanged` | `resolve_final_decision_after_telegram_delivery` |
| `test_historical_admin_header_still_renders` | «🚨 Сигнал от админа» для истории |

---

## Результаты проверок

```text
PYTHONPATH=. pytest -q  →  881 passed in ~30s
git diff --check          →  OK
```

Запуск: из корня репозитория с `PYTHONPATH=.`.

---

## Техническая заметка (CRLF)

Первые правки через editor нормализовали `\r\n` → `\n` на **весь** `NanoTest.py` (ложный diff ~29k строк).
Исправление: `git checkout HEAD -- NanoTest.py`, затем точечные замены через Python с сохранением `\r\n` из git.
Итоговый diff: **157 строк**, `git diff --check` чистый.

---

## Поведение после PR-1

| Было (гипотетически review ON) | Стало |
|--------------------------------|--------|
| DM / review band / callback publish | Невозможно через wiring |
| Обход 46–60 для review pipeline | Нет |
| `review_timeout_daemon` at startup | Нет |

На prod с `ENABLE_ADMIN_REVIEW_SIGNALS=false` и C1 пользовательский эффект минимален; код review-path отключён на входах.

---

## Неожиданные зависимости

**Не обнаружены.**

---

## Следующие шаги (не выполнены)

- PR-2: удаление мёртвых функций, env, daemons
- PR-3: docs (`FUNCTIONAL_CONTRACT`, `ARCHITECTURE_AUDIT`, `CORRECTNESS_FINDINGS`)
- Коммит PR-1 по запросу

---

*Дублирует отчёт ассистента в чате от 2026-09-22.*
