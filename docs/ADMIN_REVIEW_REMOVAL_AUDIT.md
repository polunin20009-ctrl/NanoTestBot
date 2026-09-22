# Аудит полного удаления Admin Review

**Статус:** продуктовое решение — feature снимается целиком; **C1 не исправлять**.

**Журнал реализации (всегда в файлах):**

| PR | Документ | Статус |
|----|----------|--------|
| PR-1 | [ADMIN_REVIEW_REMOVAL_PR1.md](./ADMIN_REVIEW_REMOVAL_PR1.md) | реализовано, не закоммичено |
| PR-2 | *(план ниже)* | не начато |
| PR-3 | docs contract/findings | не начато |

---

## Краткий scope

- **Удаляем:** DM модерация (`ADMIN_USER_ID`), `review_send`/`review_skip`, auto-admin publish, `review_queue`, timeout daemon, обход окна 46–60 для review.
- **Не путаем с:** `wide_research` `TERMINAL_REVIEW_*`.
- **Сохраняем:** BASE, champion, 45+ ordinary send, формулы, канал/monitor, чтение `approved_by_admin` / «Сигнал от админа» для истории.

---

## Ключевые символы (до PR-2)

| Символ | Вызовы до PR-1 |
|--------|----------------|
| `send_review_to_admin` | был: `main_loop` → **убран в PR-1** |
| `publish_signal_to_channel` | был: auto-admin + `review_send` → **убран в PR-1** |
| `can_auto_post_admin` | только из `send_review_to_admin` (определение остаётся) |

**H4:** второй publish-path; закрывается полным removal (PR-2 удалит `publish_signal_to_channel`).

---

## Категории кода

1. **Только Admin Review** — удалить в PR-2+: функции review UI, daemons, env, orphan `should_send_signal`, …
2. **Shared** — BASE/champion/45+, `validate_match_context_before_send`, `resolve_signal_header_title(is_admin_approved=…)`, …
3. **История** — tolerant load `review_*` state; не purge `bot_state.json` без решения

---

## План PR (из аудита)

### PR-1 ✅ (см. [PR1 doc](./ADMIN_REVIEW_REMOVAL_PR1.md))

main_loop wiring, callbacks, timeout startup; тесты `test_admin_review_removal_pr1.py`.

### PR-2

Удалить определения ~11609–12984, daemons, константы/env; orphans.

### PR-3

`FUNCTIONAL_CONTRACT.md` §14, `ARCHITECTURE_AUDIT.md`, `CORRECTNESS_FINDINGS.md` C1/H4 → removed.

### Regression

Полный `pytest`; `test_admin_review_removal_pr1.py` + characterization production flow.

---

## C1 (справка)

`REVIEW_MIN_THRESHOLD <= prob_actual < PROB_SEND_THRESHOLD` с defaults → всегда false. Не чинить; feature удаляется.

---

*Полный первоначальный аудит (карта потока, таблицы entry points) восстановлен в сокращённом виде; детали PR-1 — только в [ADMIN_REVIEW_REMOVAL_PR1.md](./ADMIN_REVIEW_REMOVAL_PR1.md). При каждом PR добавлять новый `docs/ADMIN_REVIEW_REMOVAL_PRn.md` и строку в таблицу выше.*
