# Сценарий проверки — Windows (PowerShell)

Пошаговая проверка системы на живом Kafka. Для **каждого шага** указано, **в каком окне** его выполнять.
Подробное объяснение, что и зачем проверяется, — в `TESTING.md` (там версия для bash).

**Идея проверки.** Офлайн строим **эталон** — ожидаемые `queue.v1` и `decision.v1` для записанной телеметрии. Затем гоняем ту же
телеметрию через живой Kafka и сверяем выход с эталоном (`verify full`). Движок детерминирован, поэтому выход обязан совпасть —
в том числе после падений и перезапусков.

---

## Как читать этот документ

### Окна (терминалы)

«Окно» — это отдельное окно (или вкладка Windows Terminal) PowerShell. Их три, у каждого постоянная роль:

| Окно | Роль | Что в нём происходит |
|---|---|---|
| **A** | **сервис** | `python -m vqueue.service …` работает постоянно, пока вы не остановите его `Ctrl+C`. Пока он работает, в этом окне больше ничего вводить нельзя. |
| **B** | **генератор** | `python -m vqueue.generator …` отрабатывает и **сам завершается** (возвращается приглашение `PS C:\Projects\virtual-queues>`). |
| **C** | **всё остальное** | проверки, `docker compose exec`, `create_topics.py`, `verify`, «убить процесс». Здесь выполняются разовые команды. |

На этапе 6 сервисов два, поэтому окно A превращается в **A1** и **A2**: два отдельных окна, в каждом свой сервис.

### Что делать при открытии любого нового окна

В **каждом новом окне** первым делом выполните три строки (иначе будут ошибки `FileNotFoundError: config\site.json`, «не найден модуль»
или «кракозябры»):

```powershell
cd C:\Projects\virtual-queues
.\.venv\Scripts\Activate.ps1
$env:PYTHONUTF8 = "1"
```

В приглашении должно быть `(.venv) PS C:\Projects\virtual-queues>` — слева `(.venv)`, и путь именно корня проекта, а **не** `…\src`.

### Обозначения в шагах

- Заголовок шага: **«Шаг 5.3 · окно A · запустить сервис»** — номер, окно, действие.
- **Ожидаем** — что должно появиться. Если у вас иначе, остановитесь и пришлите вывод (см. конец документа).
- Числа получены на эталонном прогоне. Если ваш эталон строится у вас из той же телеметрии и сверка проходит — небольшие отличия не страшны.

### Чего не делать

- **Не запускайте генератор повторно**, пока не сказано: он дописывает данные в топик, и входной поток удваивается.
- **Не пересоздавайте топики** (`create_topics.py --recreate`), пока в каком-либо окне работает сервис.

---

## Этап 0. Подготовка (один раз)

**Шаг 0.1 · окно C · установка**

```powershell
cd C:\Projects\virtual-queues
python -m venv .venv                 # только если папки .venv ещё нет
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt      # ставит confluent-kafka (Python 3.14 поддержан с версии 2.12)
pip install -e .                     # ТОЧКА В КОНЦЕ обязательна
New-Item -ItemType Directory -Force work | Out-Null
$env:PYTHONUTF8 = "1"
```

Если `Activate.ps1` запрещён политикой: `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, затем повторите.

**Шаг 0.2 · окно C · проверка**

```powershell
python -c "import confluent_kafka, vqueue; print('kafka', confluent_kafka.version(), '| vqueue ok')"
docker --version; docker compose version
```

**Ожидаем:** строка `kafka (…) | vqueue ok` и версии Docker. Docker Desktop должен быть **запущен**.

---

## Этап 1. Офлайн-проверки (без Kafka) — всё в окне C

**Шаг 1.1 · окно C · юнит-тесты** (≈1 мин)

```powershell
python -m unittest discover -s tests -t .
```

**Ожидаем:** `Ran 90 tests … OK`.

**Шаг 1.2 · окно C · пример из задания**

```powershell
python scripts/check_spec_example.py
```

**Ожидаем:** очередь S1 с ожиданиями 0 / 50 / 100 с; решение `recommended`, `from_station S1`, `to_station S2`, `gain_seconds 80`;
очередь S2 с T4; итог `РЕЗУЛЬТАТ: СОВПАДАЕТ С ПРИМЕРОМ`.

**Шаг 1.3 · окно C · эталон для основных этапов (3, 4, 6, 7)**

```powershell
python -m vqueue.generator --out work/telemetry.jsonl --duration 1800 --speedup 0 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
python -m vqueue.simulate --replay work/telemetry.jsonl --queue-out work/golden_queue.jsonl --decision-out work/golden_decision.jsonl --print 0
python -m vqueue.verify check --queue work/golden_queue.jsonl --decision work/golden_decision.jsonl
```

**Ожидаем:**

```
повторно проиграно из work/telemetry.jsonl; нечитаемых строк: 0
конец симуляции: 2025-09-15T10:29:59Z; сообщений принято {'accepted': 70577, 'ignored_old': 2880, 'unknown_unit': 0}
queue.v1: 3424 событий; decision.v1: 41
OK    формат и инварианты (3424 queue, 41 decision)
```

Если «нечитаемых строк» не 0 — файл записан не в UTF-8: убедитесь, что использовали `--out`, а не `>`.

---

## Этап 2. Брокер и топики — всё в окне C

**Шаг 2.1 · окно C · поднять Kafka**

```powershell
docker compose up -d
Start-Sleep -Seconds 20
docker compose logs kafka | Select-String "started" | Select-Object -Last 2
```

**Ожидаем:** в логе строка `Kafka Server started`.

**Шаг 2.2 · окно C · создать топики**

```powershell
python scripts/create_topics.py --recreate --reset-group vqueue-planner
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vqueue.state.v1
```

**Ожидаем:** `создан telemetry.v1 / queue.v1 / decision.v1 / vqueue.state.v1` (строки с `UNKNOWN_TOPIC_OR_PART` и про группу на чистом
брокере — нормальны); в списке четыре топика; у `vqueue.state.v1` — `PartitionCount: 1` и `cleanup.policy=compact`.

> «Coordinator not available» / «Broker transport failure» — брокер ещё стартует, подождите 15–20 с и повторите шаг.

---

## Этап 3. Основной прогон

Окна: **A** (сервис), **B** (генератор), **C** (проверки). Откройте три окна и в каждом выполните три строки из раздела «Что делать при
открытии любого нового окна».

**Шаг 3.1 · окно A · запустить сервис**

```powershell
python -m vqueue.service --config config/site.json
```

**Ожидаем** за 5–15 с строку `… vqueue.runner: снимка состояния нет — старт с чистого состояния`. Дальше раз в 15 с — строка `stats=…`
(пока данных нет: `now=None`). Окно A оставьте работать.

**Шаг 3.2 · окно B · запустить генератор** (≈30 с)

```powershell
python -m vqueue.generator --config config/site.json --speedup 60 --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
```

Параметры те же, что у эталона — это важно. Окно B молча работает ~30 с и возвращает приглашение.

**Шаг 3.3 · окно C · вход дошёл целиком**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic telemetry.v1
```

**Ожидаем:** `telemetry.v1:0:73457`.

**Шаг 3.4 · окно C · дождаться, пока сервис догонит вход**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
```

**Ожидаем:** `LAG` = 0 (если нет — повторите через 10–20 с). Параллельно **посмотрите в окно A**: в последней строке `stats=` должно быть
`'messages': 73457`, `'invalid': 0`, `'failed_commits': 0`, `engine={'accepted': 70577, 'ignored_old': 2880, 'unknown_unit': 0}`.

**Шаг 3.5 · окно C · главная проверка: выход совпадает с эталоном**

```powershell
python -m vqueue.verify full --golden-queue work/golden_queue.jsonl --golden-decision work/golden_decision.jsonl --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
```

**Ожидаем:**

```
прочитано: queue.v1 — 3424, decision.v1 — 41 (…)
OK    формат и инварианты
OK    queue.v1 совпадает с эталоном (3424 сообщений)
OK    decision.v1 совпадает с эталоном (41 сообщений)
ИТОГ: ВСЁ ПРОШЛО
```

**Шаг 3.6 · окно C · снимок состояния лежит в Kafka**

```powershell
python -m vqueue.verify state
```

**Ожидаем:** `оффсеты входа: {'0': 73457}`, `«сейчас» движка: 2025-09-15T10:29:59Z`, `машин в состоянии: 40`.

**Шаг 3.7 · окно C · посмотреть решения глазами (по желанию)**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic decision.v1 --from-beginning --isolation-level read_committed --max-messages 3 --property print.key=true
```

**Ожидаем:** три строки вида `T-11  {"unit_uuid":"T-11","at":"…","result":"recommended",…}` — ключ равен `unit_uuid`.

**Шаг 3.8 · окно C · мусор и неизвестная машина**

```powershell
python scripts/produce_raw.py garbage
python scripts/produce_raw.py unknown-unit
```

Каждая команда печатает `отправлено в telemetry.v1: …`. Затем **в окне A** через ≤15 с в строке `stats=` ожидаем `'invalid': 1` и
`'unknown_unit': 1`; сервис жив, `failed_commits: 0`.

**Шаг 3.9 · окно A · корректная остановка**

В окне A нажмите `Ctrl+C`, затем сразу там же:

```powershell
$LASTEXITCODE
```

**Ожидаем:** сервис завершился сам, без трейсбека, код `0`.

---

## Этап 4. Падение сервиса под нагрузкой (жёсткое убийство)

**Что проверяем:** при внезапном `kill` ни одно решение не теряется и не дублируется.
Окна: **A** (сервис), **B** (генератор), **C** (чистый старт и «убить процесс»).

**Шаг 4.1 · окно C · чистый старт**

Убедитесь, что сервис в окне A остановлен (`Ctrl+C`, если ещё работает). Затем:

```powershell
python scripts/create_topics.py --recreate --reset-group vqueue-planner
```

**Шаг 4.2 · окно A · запустить сервис**

```powershell
python -m vqueue.service --config config/site.json
```

Дождитесь строки `снимка состояния нет — старт с чистого состояния`.

**Шаг 4.3 · окно B · запустить медленный генератор** (≈90 с)

```powershell
python -m vqueue.generator --config config/site.json --speedup 20 --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
```

**Шаг 4.4 · окно C · убить сервис** (через ~25 с после старта генератора)

Вставьте команду целиком (она сама ничего не требует заранее):

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'vqueue\.service' -and $_.ProcessId -ne $PID } | ForEach-Object { Write-Host "kill $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force }
```

**Ожидаем:** строки `kill <номер>` (обычно две: лаунчер venv и сам Python). Окно A вернётся к приглашению или закроется.

**Шаг 4.5 · окно A · запустить сервис снова** (через ~10 с после убийства; если окно закрылось — откройте новое и выполните три строки)

```powershell
python -m vqueue.service --config config/site.json
```

**Ожидаем:** `состояние восстановлено из снимка, оффсеты {0: N}` — `N` из середины потока, не 0.

**Шаг 4.6 · окна C и A · второе убийство**

Через ~25 с (желательно сразу после того, как в окне A появились новые `stats`): в окне **C** снова выполните команду из шага 4.4, через ~10 с
в окне **A** снова `python -m vqueue.service --config config/site.json`.

**Шаг 4.7 · окно C · дождаться конца и сверить**

Дождитесь, пока окно B вернёт приглашение (генератор закончил). Затем `LAG` (команда из шага 3.4) должен стать 0. После этого:

```powershell
python -m vqueue.verify full --golden-queue work/golden_queue.jsonl --golden-decision work/golden_decision.jsonl --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
```

**Ожидаем:** снова `queue.v1 — 3424, decision.v1 — 41`, три `OK`, `ИТОГ: ВСЁ ПРОШЛО`. В последней строке `stats=` окна A:
`engine={'accepted': 70577, 'ignored_old': 2880, …}`.

> Если `verify` долго ничего не читает: убитый процесс оставил открытую транзакцию, читатель ждёт её закрытия (до минуты). Повторите.

---

## Этап 5. Сильный шум: много дублей и опозданий

**Что проверяем:** дубли, нарушение порядка и опоздания до 60 с не портят очереди. Окна: **A**, **B**, **C**.

**Шаг 5.1 · окно A · остановить сервис**

Если в окне A работает сервис — нажмите `Ctrl+C` и дождитесь приглашения. После шага 5.2 в окне A сервиса быть не должно.

**Шаг 5.2 · окно C · чистый старт**

```powershell
python scripts/create_topics.py --recreate --reset-group vqueue-planner
```

**Ожидаем:** `создан …` для четырёх топиков, **без** строки `NON_EMPTY_GROUP`. Если она есть — где-то ещё работает сервис; остановите и повторите.

**Шаг 5.3 · окно C · эталон для шума**

```powershell
python -m vqueue.generator --out work/telemetry_noisy.jsonl --duration 1800 --speedup 0 --start-ts 1757930400 --seed 1 --dup-rate 0.2 --late-rate 0.2
python -m vqueue.simulate --replay work/telemetry_noisy.jsonl --queue-out work/golden_queue_noisy.jsonl --decision-out work/golden_decision_noisy.jsonl --print 0
```

**Ожидаем:** `сообщений принято {'accepted': 57820, 'ignored_old': 28378, …}`, `queue.v1: 2940 событий; decision.v1: 41`.

**Шаг 5.4 · окно A · запустить сервис** ← без этого шага выход будет пустым

```powershell
python -m vqueue.service --config config/site.json
```

**Ожидаем** за 5–15 с: `снимка состояния нет — старт с чистого состояния`. Окно A оставьте работать.

**Шаг 5.5 · окно B · запустить генератор с шумом** (≈30 с)

```powershell
python -m vqueue.generator --config config/site.json --speedup 60 --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.2 --late-rate 0.2
```

**Шаг 5.6 · окно C · проверить вход и догон**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic telemetry.v1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
```

**Ожидаем:** `telemetry.v1:0:86198`; во второй команде группа `vqueue-planner` существует и `LAG` = 0 (если нет — подождите 10–20 с и повторите).
Если пишет `Consumer group 'vqueue-planner' does not exist` — сервис в окне A не запущен: вернитесь к шагу 5.4. Генератор повторно **не** запускайте.
В окне A в `stats=`: `'messages': 86198`, `'ignored_old': 28378`.

**Шаг 5.7 · окно C · сверка с эталоном шума**

```powershell
python -m vqueue.verify full --golden-queue work/golden_queue_noisy.jsonl --golden-decision work/golden_decision_noisy.jsonl --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
```

**Ожидаем:** `queue.v1 — 2940, decision.v1 — 41`, три `OK`, `ИТОГ: ВСЁ ПРОШЛО`.

---

## Этап 6. Резервный экземпляр и переключение

**Что проверяем:** два сервиса в одной группе. Партицию держит один (**активный**), второй — **резерв**. Когда активный внезапно умирает,
резерв сам подхватывает работу из снимка, и результат совпадает с эталоном.

Окна: **A1** (сервис №1), **A2** (сервис №2), **B** (генератор), **C** (вспомогательное). Откройте четыре окна и в каждом выполните три строки.

**Шаг 6.1 · окно C · подготовка**

Остановите **все** сервисы (`Ctrl+C` в каждом окне, где они работают) и убедитесь, что не осталось ни одного:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'vqueue\.service' -and $_.ProcessId -ne $PID } | Select-Object ProcessId, CommandLine
```

Список должен быть **пустым**. Затем чистый старт:

```powershell
python scripts/create_topics.py --recreate --reset-group vqueue-planner
```

(`NON_EMPTY_GROUP` в выводе = ещё работает сервис; остановите и повторите.)

**Шаг 6.2 · окно A1 · запустить сервис №1**

```powershell
python -m vqueue.service --config config/site.json --max-batch 500
```

**Ожидаем:** `снимка состояния нет — старт с чистого состояния`.

**Шаг 6.3 · окно A2 · запустить сервис №2**

```powershell
python -m vqueue.service --config config/site.json --max-batch 501
```

(`500` и `501` — «метки», чтобы потом отличить процессы; на работу не влияют.) Теперь в группе два участника, Kafka перераспределяет партицию.
**Ожидаем:** в одном окне — `партиции не получены — резервный режим` (это резерв); в другом — `снимка состояния нет …` (возможно, повторно, и
перед ним `партиции отозваны: [0]` — это обычный ребаланс). Кто станет активным — A1 или A2 — решает Kafka; запомните на шаге 6.5. Подождите ~10 с.

**Шаг 6.4 · окно B · запустить генератор** (≈90 с)

```powershell
python -m vqueue.generator --config config/site.json --speedup 20 --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
```

**Шаг 6.5 · окна A1/A2 и C · определить активный сервис и убить его**

Через ~25 с после старта генератора посмотрите **в окна A1 и A2** (строки `stats=` раз в 15 с): у **активного** `'messages'` растёт и `now=` — дата;
у **резерва** `'messages': 0` и `now=None`. Затем в окне **C**:

Если активным был **A1** (запущен с `--max-batch 500`) — вставьте целиком:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'max-batch 500' -and $_.ProcessId -ne $PID } | ForEach-Object { Write-Host "kill $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force }
```

Если активным был **A2** (`--max-batch 501`) — вставьте целиком:

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'max-batch 501' -and $_.ProcessId -ne $PID } | ForEach-Object { Write-Host "kill $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force }
```

Резерв не трогайте. Окно убитого сервиса вернётся к приглашению или закроется — так и должно быть.

**Шаг 6.6 · окно бывшего резерва · ждать переключения** (ничего не вводить, до ~45 с)

**Ожидаем** в его окне:

```
… vqueue.kafka: партиции отозваны: []
… vqueue.runner: состояние восстановлено из снимка, оффсеты {0: N}
```

`N` — число из середины потока, не 0; дальше `'messages'` и `now=` в его `stats=` начинают расти.

**Шаг 6.7 · окно C · дождаться конца и сверить**

Дождитесь, пока окно B вернёт приглашение, и `LAG` = 0:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
python -m vqueue.verify full --golden-queue work/golden_queue.jsonl --golden-decision work/golden_decision.jsonl --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
```

**Ожидаем:** `queue.v1 — 3424, decision.v1 — 41`, `ИТОГ: ВСЁ ПРОШЛО`.

**Шаг 6.8 · окно оставшегося сервиса · остановить** (`Ctrl+C`).

---

## Этап 7. Потеря состояния: пересчёт с нуля

**Что проверяем:** если снимок состояния потерян, вход перечитывается, и **решения воспроизводятся идентично**.
Этап самодостаточен: сначала мы сами создаём чистые данные. Окна: **A**, **B**, **C**. Нужен эталон из шага 1.3 (`work\golden_*.jsonl`).

**Шаг 7.1 · окно A · остановить сервис** (`Ctrl+C`, если работает). **Окно C · чистый старт:**

```powershell
python scripts/create_topics.py --recreate --reset-group vqueue-planner
```

**Шаг 7.2 · окно A · запустить сервис**

```powershell
python -m vqueue.service --config config/site.json
```

Дождитесь `снимка состояния нет …`.

**Шаг 7.3 · окно B · запустить генератор** (≈30 с)

```powershell
python -m vqueue.generator --config config/site.json --speedup 60 --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
```

**Шаг 7.4 · окно C · дождаться обработки** (`LAG` = 0; вход `telemetry.v1:0:73457`):

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
```

**Шаг 7.5 · окно A · остановить сервис** (`Ctrl+C`).

**Шаг 7.6 · окно C · «потерять» выходы и состояние, оставив вход**

```powershell
python scripts/create_topics.py --recreate --only queue.v1,decision.v1,vqueue.state.v1 --reset-group vqueue-planner
```

**Ожидаем:** `создан queue.v1`, `создан decision.v1`, `создан vqueue.state.v1`; без `NON_EMPTY_GROUP`. Топик `telemetry.v1` не тронут.

**Шаг 7.7 · окно A · запустить сервис**

```powershell
python -m vqueue.service --config config/site.json
```

**Ожидаем:** `снимка состояния нет — старт с чистого состояния`; затем за 10–30 с сервис проигрывает весь вход (в `stats=`: `'messages': 73457`).

**Шаг 7.8 · окно C · дождаться `LAG` = 0 и сверить**

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
python -m vqueue.verify full --golden-queue work/golden_queue.jsonl --golden-decision work/golden_decision.jsonl --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
```

**Ожидаем:** `ИТОГ: ВСЁ ПРОШЛО`, 3424 / 41 — пересчёт с нуля дал **те же** результаты.

### Этап 7б (опционально): повреждённый снимок

Выполняется сразу после 7.8 (сервис в окне A работает, выходные топики заполнены).

**Шаг 7б.1 · окно A · остановить сервис** (`Ctrl+C`).

**Шаг 7б.2 · окно C · записать мусор в топик состояния**

```powershell
python scripts/produce_raw.py broken-state
```

**Ожидаем:** `отправлено в vqueue.state.v1: ключ='state', значение='{broken'`.

**Шаг 7б.3 · окно A · запустить сервис**

```powershell
python -m vqueue.service --config config/site.json
```

**Ожидаем** `снимок повреждён — перечитываем вход с начала`. Сервис заново проиграет весь вход и допишет те же результаты в **непустые** выходные топики.

**Шаг 7б.4 · окно C · дождаться `LAG` = 0 и сравнить**

```powershell
python -m vqueue.verify full --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
python -m vqueue.verify compare --golden work/golden_decision.jsonl --actual work/actual_decision.jsonl --key unit_uuid --dedup-actual
```

**Ожидаем:** первая команда **ожидаемо покажет FAIL** (дубли решений — это задокументированное поведение); вторая напечатает `повторов убрано: 41`
и `OK … совпадает с эталоном`: повторы побайтно идентичны (потребитель решений должен дедуплицировать по `(unit_uuid, at)`).

**Шаг 7б.5 · окно A · остановить сервис** (`Ctrl+C`).

---

## Этап 8. Замкнутый контур (опционально, визуально)

Здесь водители выполняют рекомендации, поэтому поток зависит от решений и с эталоном **не сравнивается**: проверяем инварианты и смысл.
Окна: **A**, **B**, **C**.

**Шаг 8.1 · окно A · остановить сервис**, **окно C · чистый старт:** `python scripts/create_topics.py --recreate --reset-group vqueue-planner`.

**Шаг 8.2 · окно A · запустить сервис:** `python -m vqueue.service --config config/site.json`.

**Шаг 8.3 · окно B · запустить генератор в замкнутом контуре** (≈6 мин)

```powershell
python -m vqueue.generator --config config/site.json --speedup 10 --duration 3600 --seed 1 --dup-rate 0.01 --late-rate 0.02 --follow-decisions
```

**Шаг 8.4 · окно C · смотреть решения вживую** (остановить — `Ctrl+C`)

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic decision.v1 --isolation-level read_committed --property print.key=true
```

**Ожидаем:** по мере движения машин появляются `recommended` (чаще на менее загруженные S3/S4) и `rejected/no_gain`; новое решение для машины
появляется только после того, как она побывала на станции и снова приблизилась к своей на 1500 м.

**Шаг 8.5 · окно C · после окончания генератора и `LAG` = 0**

```powershell
python -m vqueue.verify full --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
(Select-String -Path work/actual_decision.jsonl -Pattern recommended).Count
```

**Ожидаем:** `ИТОГ: ВСЁ ПРОШЛО` (проверяются формат и инварианты) и число `recommended` больше нуля.

---

## Завершение

**Окно A:** `Ctrl+C` (остановить сервис). **Окно C:**

```powershell
docker compose down -v
deactivate
```

---

## Приложение: как жёстко убить процесс (окно C)

На этапах 4 и 6 нужно «внезапно умертвить» сервис (имитация падения). Для этого в окне C вставляется **одна команда целиком**; ничего
определять заранее не нужно. Она убивает все процессы, в командной строке которых встречается шаблон (подставляется между кавычками
после `-match`):

| Что убить | Шаблон |
|---|---|
| любой сервис (этап 4) | `vqueue\.service` |
| сервис A1 (этап 6) | `max-batch 500` |
| сервис A2 (этап 6) | `max-batch 501` |

```powershell
Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'ШАБЛОН' -and $_.ProcessId -ne $PID } | ForEach-Object { Write-Host "kill $($_.ProcessId)"; Stop-Process -Id $_.ProcessId -Force }
```

Команда печатает `kill <номер>` — обычно два номера: процесс-лаунчер venv и сам Python. `Stop-Process -Force` завершает процесс немедленно, без
корректной остановки: именно это и нужно для проверки.

---

## Чек-лист

| № | Окно | Шаг | Ожидание | ✔/✘ |
|---|---|---|---|---|
| 0 | C | Установка и импорт | `kafka (…) \| vqueue ok` | |
| 1.1 | C | Тесты | 90 OK | |
| 1.2 | C | Пример из задания | СОВПАДАЕТ С ПРИМЕРОМ | |
| 1.3 | C | Эталон | 3424 / 41, инварианты OK | |
| 2 | C | Топики | 4 топика, state — compact | |
| 3.3–3.4 | C | Вход и LAG | `…:0:73457`, LAG 0 | |
| 3.5 | C | Сверка с эталоном | ВСЁ ПРОШЛО (3424/41) | |
| 3.6 | C | Снимок состояния | оффсет 73457, 40 машин | |
| 3.8 | C → A | Мусор, неизвестная машина | `invalid` 1, `unknown_unit` 1 | |
| 3.9 | A | Ctrl+C | `$LASTEXITCODE` = 0 | |
| 4 | A, B, C | Жёсткое убийство ×2 | ВСЁ ПРОШЛО, те же числа | |
| 5 | A, B, C | Сильный шум | 2940 / 41, ВСЁ ПРОШЛО | |
| 6 | A1, A2, B, C | Резерв и переключение | резерв восстановился, ВСЁ ПРОШЛО | |
| 7 | A, B, C | Пересчёт с нуля | ВСЁ ПРОШЛО | |
| 7б | A, C | Повреждённый снимок (опц.) | `повторов убрано: 41`, OK | |
| 8 | A, B, C | Замкнутый контур (опц.) | инварианты OK, есть `recommended` | |

## Типичные проблемы

| Симптом | Что делать |
|---|---|
| `FileNotFoundError: config\site.json` | Окно не в корне проекта. `cd C:\Projects\virtual-queues` и повторить. |
| `-e option requires 1 argument` | Нужна точка: `pip install -e .` |
| `No module named 'vqueue'` | Не выполнено `pip install -e .` или не активирован venv (нет `(.venv)` в приглашении). |
| `No module named 'confluent_kafka'` | `pip install -r requirements.txt` в активированном venv. |
| Кракозябры вместо кириллицы | В этом окне не выполнено `$env:PYTHONUTF8="1"`; лучше использовать Windows Terminal. |
| `Activate.ps1 … running scripts is disabled` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`. |
| Ошибка `docker daemon` / `pipe/dockerDesktop` | Docker Desktop не запущен. |
| `Connect to ipv6#[::1]:9092 failed` | Шум: клиент пробует IPv6 и переходит на IPv4. В актуальной версии по умолчанию `127.0.0.1:9092`; если брокер запущен по старому compose — `docker compose up -d --force-recreate`, затем заново создать топики. |
| `Operation not valid in state Ready` при `init_transactions` | Ошибка старой версии (повторная инициализация продюсера при ребалансе). Исправлена в актуальном `kafka_io.py` — обновите файл. |
| `NON_EMPTY_GROUP` при `create_topics.py` | Где-то работает сервис. Остановите все сервисы и повторите. Топики под работающим сервисом удалять нельзя. |
| `прочитано: queue.v1 — 0, decision.v1 — 0` / `выходные топики пусты` | Сервис не запущен/упал или не успел. Проверьте: `kafka-consumer-groups.sh --describe --group vqueue-planner` должен показать участника и `LAG` = 0 (ответ `group does not exist` = сервис не запускали — шаг «запустить сервис в окне A»); `kafka-get-offsets.sh --topic telemetry.v1` — число сообщений на входе. Генератор повторно **не** запускайте. |
| `Kill-Hard : … не распознано` | Устаревшая инструкция: функция не нужна. Используйте полную команду из шага 4.4 / 6.5 (см. «Приложение»). |
| `exec - kafka` / `service "-" is not running` | Потеряна буква `T` во флаге `-T`. Используйте `python scripts/produce_raw.py …` вместо конвейеров в `docker exec`. |
| Сверка: «расхождение; ожидалось N, получено M» | Главный сигнал. Пришлите вывод `verify` (там первое отличие эталона и факта) и параметры генератора. Проверьте, что параметры генератора **точно** совпадают с эталоном и вход в `telemetry.v1` ровно такой, как в шаге. |
| Сверка: «лишний/нет в фактическом выходе» | Скорее всего, топики не пересоздавались перед этапом. Сделайте чистый старт (шаг «чистый старт» этапа). |
| `LAG` не падает до 0 | Сервис упал или завис — смотрите окно A; `--describe` покажет, есть ли участник. |
| Порт 9092 занят | `netstat -ano \| findstr :9092`, остановите занявший процесс или прежний `docker compose down`. |
| `ImportError: DLL load failed` при `import confluent_kafka` | Нужен 64-битный Python и актуальный Visual C++ Redistributable; пришлите полный текст ошибки. |

## Что прислать мне после прогона

1. Заполненный чек-лист.
2. Для каждого шага с ✘ — полный вывод **окна A** (лог сервиса) и вывод команды, которая показала расхождение.
3. Вывод `python -m vqueue.verify state` и `kafka-consumer-groups.sh --describe` на момент проблемы.
4. `python --version`, `docker --version`, `pip show confluent-kafka`.
