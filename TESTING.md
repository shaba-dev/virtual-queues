# Сценарий проверки по шагам

> **Windows / PowerShell:** используйте [`TESTING_WINDOWS.md`](TESTING_WINDOWS.md) — там каждый шаг подписан окном (A/B/C), этап 7 самодостаточен, команды переписаны под PowerShell. Этот документ — для bash (`export`, `make`, `pkill`, `sort`, `>`).

Цель — вместе убедиться, что система работает: расчёт верен, а Kafka-часть (которую в среде
разработки нельзя было запустить) ведёт себя правильно, в том числе при падениях.

**Как устроена проверка.** Движок детерминирован: один и тот же поток телеметрии всегда даёт один и тот
же выход. Поэтому сначала мы офлайн строим **эталон** (ожидаемые `queue.v1` и `decision.v1`) для записанной
телеметрии, потом гоняем ту же телеметрию через живой Kafka и сверяем выход побайтно. Если совпало — значит,
транзакции, рестарт и снимок состояния ничего не потеряли и не продублировали.

Время на всё: этапы 0–3 — около 20 минут, весь сценарий — около часа.

## Условные обозначения

- **Терминал** — отдельное окно (вкладка) оболочки; в каждом новом нужно активировать `.venv` и перейти в корень проекта.
- **A** — окно, где работает сервис; **B** — окно, где работает генератор; **C** — вспомогательное (проверки, kill, консольные
  консьюмеры). На этапе 6 сервисов два, и A превращается в **A1** и **A2** (два отдельных окна). Все команды — **из корня репозитория**, если не сказано иное.
- Блок «Ожидаем» — что должно появиться. Числа получены на эталонном прогоне; если ваши числа
  отличаются, но *сверка с эталоном проходит* (эталон вы строите у себя), это не ошибка.
- Отметьте каждый шаг в чек-листе в конце. На любом «не так» остановитесь и пришлите мне вывод.

---

## Этап 0. Подготовка

```bash
unzip virtual-queues.zip && cd virtual-queues
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$PWD/src          # нужен в КАЖДОМ новом терминале (и source .venv/bin/activate тоже)
mkdir -p work                       # сюда пойдут эталон и выгрузки из Kafka
docker --version && docker compose version
```

Нужно: Python 3.10+, Docker с `docker compose`, свободный порт 9092.

**Ожидаем:** `pip` ставит `confluent-kafka` без ошибок; `python -c "import confluent_kafka; print(confluent_kafka.version())"` печатает версию.

> Если `pip install` не находит колёсико `confluent-kafka` под вашу версию Python — пришлите версию Python и ОС.

---

## Этап 1. Офлайн-проверки (без Kafka)

### 1.1 Юнит-тесты

```bash
python -m unittest discover -s tests -t .
```

**Ожидаем:** `Ran 90 tests in ~75s` и `OK`. (Тесты используют заглушку Kafka, настоящий брокер не нужен.)

### 1.2 Пример из задания

```bash
python scripts/check_spec_example.py
```

**Ожидаем** три JSON и итог `РЕЗУЛЬТАТ: СОВПАДАЕТ С ПРИМЕРОМ`:

```
Очередь S1 в 12:00:00 (ожидаем T1: 0 с, T2: 50 с, T3: 100 с):
  {"station_uuid": "S1", "at": "2026-09-15T12:00:00Z", "queue": [
     T1: eta null,            service_start 11:59:00, free_at 12:02:50, wait_seconds 0
     T2: eta 12:02:00,        service_start 12:02:50, free_at 12:06:40, wait_seconds 50
     T3: eta 12:05:00,        service_start 12:06:40, free_at 12:10:30, wait_seconds 100 ]}
Решение для T4:
  {"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "recommended",
   "from_station": "S1", "to_station": "S2", "gain_seconds": 80}
Очередь S2 после решения: T4, eta 12:04:00, wait_seconds 0
```

Это те же цифры, что в условии задания.

### 1.3 Эталон для сценария на Kafka

```bash
make golden
```

(без `make`: две команды из `Makefile`, цель `golden`.) Создаёт в `work/`:
`telemetry.jsonl` (вход), `golden_queue.jsonl` и `golden_decision.jsonl` (ожидаемый выход).

**Ожидаем:**

```
повторно проиграно из work/telemetry.jsonl; нечитаемых строк: 0
конец симуляции: 2025-09-15T10:29:59Z; сообщений принято {'accepted': 70577, 'ignored_old': 2880, 'unknown_unit': 0}
queue.v1: 3424 событий; decision.v1: 41
решения: {('rejected','no_gain'): 9, ('recommended','S2'): 3, ('recommended','S4'): 23, ('recommended','S3'): 6}
```

```bash
wc -l work/*.jsonl
python -m vqueue.verify check --queue work/golden_queue.jsonl --decision work/golden_decision.jsonl
```

**Ожидаем:** `73457 work/telemetry.jsonl`, `41 … golden_decision`, `3424 … golden_queue`, и
`OK    формат и инварианты (3424 queue, 41 decision)`.

**Что проверили:** расчёт верен на примере из задания; поток из 40 машин с дублями и опозданиями даёт очереди,
которые проходят все инварианты (цепочка `free_at`, ожидание ≥ 0, горизонт, порог выигрыша, отсутствие
повторов и дублей решений).

---

## Этап 2. Брокер и топики

```bash
docker compose up -d
sleep 20                                            # дать брокеру подняться
docker compose logs kafka 2>&1 | grep -i "started" | tail -2
python scripts/create_topics.py --recreate --reset-group vqueue-planner
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Ожидаем:** в логе брокера строка `Kafka Server started`; скрипт печатает `создан telemetry.v1`, `создан queue.v1`,
`создан decision.v1`, `создан vqueue.state.v1` (строки про «удалён/группа» с ошибкой «неизвестный топик/группа» на
чистом брокере — нормальны); `--list` показывает эти четыре топика (и служебный `__consumer_offsets`).

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic vqueue.state.v1
```

**Ожидаем:** `PartitionCount: 1`, в `Configs` есть `cleanup.policy=compact`.

> Ошибка «Coordinator not available» / «Broker transport failure» — брокер ещё не готов, подождите 15–20 с и повторите.

---

## Этап 3. Основной прогон на живом Kafka

**Терминал A — сервис:**

```bash
python -m vqueue.service --config config/site.json
```

**Ожидаем** в течение нескольких секунд (вступление в группу):
`… vqueue.runner: снимка состояния нет — старт с чистого состояния`. Это значит: партиция получена, транзакционный
продюсер инициализирован, снимка ещё нет. Дальше раз в 15 с — строка `stats=… engine=… now=None`.

**Терминал B — генератор** (≈30 секунд реального времени, 30 минут симуляции):

```bash
python -m vqueue.generator --config config/site.json --speedup 60 \
       --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
```

Параметры те же, что у эталона — это важно. Генератор молча завершится.

**3.1 Вход дошёл целиком (терминал B или C):**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic telemetry.v1
```

**Ожидаем:** `telemetry.v1:0:73457` — столько же, сколько строк в `work/telemetry.jsonl`.

**3.2 Сервис догнал вход:**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
```

**Ожидаем:** `LAG` = 0 (повторите через 10–20 с, если нет). В терминале A в строке `stats=` —
`'messages': 73457`, `'invalid': 0`, `'failed_commits': 0`, и `engine={'accepted': 70577, 'ignored_old': 2880, 'unknown_unit': 0}`.

**3.3 Главная проверка — выход совпадает с эталоном:**

```bash
make verify
```

(или: `python -m vqueue.verify full --golden-queue work/golden_queue.jsonl --golden-decision work/golden_decision.jsonl --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl`)

**Ожидаем:**

```
прочитано: queue.v1 — 3424, decision.v1 — 41 (записано в …)
OK    формат и инварианты
OK    queue.v1 совпадает с эталоном (3424 сообщений)
OK    decision.v1 совпадает с эталоном (41 сообщений)
ИТОГ: ВСЁ ПРОШЛО
```

Читатель работает в режиме `read_committed` и ждёт 10 секунд тишины, чтобы понять, что поток закончился (при первом
запуске до 25 с).

**3.4 Снимок состояния лежит в Kafka:**

```bash
python -m vqueue.verify state
```

**Ожидаем:** `оффсеты входа: {'0': 73457}`, `«сейчас» движка: 2025-09-15T10:29:59Z`, `машин в состоянии: 40`, и те же счётчики движка.

**3.5 Посмотреть «глазами» (по желанию, терминал C):**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic decision.v1 --from-beginning --isolation-level read_committed --max-messages 3 \
   --property print.key=true
```

**Ожидаем:** строки вида `T-11  {"unit_uuid":"T-11","at":"2025-09-15T10:00:00Z","result":"recommended","from_station":"S1","to_station":"S2","gain_seconds":221}` — ключ равен `unit_uuid`.

**3.6 Плохие входные данные не ломают сервис (терминал C).** Можно проще, без `docker exec`: `python scripts/produce_raw.py garbage` и `python scripts/produce_raw.py unknown-unit`. Или вручную:

```bash
echo "not json" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic telemetry.v1
echo '{"unit_uuid":"X-1","ts":1757930400,"lat":49.1,"lon":142.6,"speed_kmh":10}' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic telemetry.v1
```

**Ожидаем** через ≤15 с в терминале A: `'invalid': 1` и `'unknown_unit': 1`; сервис продолжает работать, `failed_commits: 0`.
(Выход не меняется: проверка `make verify` по-прежнему проходит.)

**3.7 Корректная остановка:** в терминале A нажмите `Ctrl+C`, затем `echo $?`.
**Ожидаем:** процесс завершился сам, код `0`, без трейсбеков.

**Что проверили:** весь конвейер Kafka → сервис → Kafka работает; формат, ключи и содержимое выходных сообщений;
транзакционные коммиты; снимок состояния; устойчивость к мусору.

---

## Этап 4. Падение сервиса под нагрузкой (`kill -9`)

Самый важный этап: проверяет, что ни одно решение не потерялось и не продублировалось при жёстком падении.

**4.0 Чистый старт** (сервис остановлен, терминал B):

```bash
python scripts/create_topics.py --recreate --reset-group vqueue-planner
```

**4.1** Терминал A: `python -m vqueue.service --config config/site.json` — ждём строку про старт с чистого состояния.

**4.2** Терминал B — медленный генератор (≈90 с):

```bash
python -m vqueue.generator --config config/site.json --speedup 20 \
       --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
```

**4.3** Терминал C — через ~25 с после старта генератора убить сервис «жёстко»:

```bash
pkill -9 -f vqueue.service
```

Подождите ~10 с (генератор продолжает писать, у сервиса копится отставание), затем в терминале A запустите сервис снова.

**Ожидаем** в A после перезапуска: `состояние восстановлено из снимка, оффсеты {0: N}` (N — где-то в середине, не 0) — это и есть
восстановление без повторного чтения всего входа.

**4.4** Повторите убийство ещё раз через ~25 с (`pkill -9 -f vqueue.service`, подождать ~10 с, запустить снова). Желательно
хотя бы один раз попасть «в момент пакета» — делайте kill сразу после того, как в терминале A появились новые `stats`.

**4.5** Когда генератор закончил, а `LAG` (шаг 3.2) стал 0 — сверка:

```bash
make verify
```

**Ожидаем те же числа, что в 3.3:** `queue.v1 — 3424, decision.v1 — 41`, все три `OK`, `ИТОГ: ВСЁ ПРОШЛО`. В строке `stats=` последнего
запуска — `engine={'accepted': 70577, 'ignored_old': 2880, …}` (счётчики движка хранятся в снимке и переживают рестарты).

> Если `make verify` сначала долго ничего не читает — упавший процесс оставил открытую транзакцию, читатель `read_committed` ждёт её
> закрытия (до 60 с, обычно закрывается новым запуском сервиса сразу). Подождите минуту и повторите.

**Что проверили:** exactly-once между топиками (выход и оффсет фиксируются атомарно), восстановление из снимка, отсутствие дублей
решений и очередей, детерминизм (выход при рестартах = эталону).

---

## Этап 5. Сильный шум: много дублей и опозданий

**Что проверяем:** дубли, нарушение порядка и опоздания до 60 с не портят очереди. Окна: **A** (сервис), **B** (генератор), **C** (проверки).

**Шаг 5.1 · окно A · остановить сервис** (`Ctrl+C`, если работает).

**Шаг 5.2 · окно C · чистый старт:**

```bash
python scripts/create_topics.py --recreate --reset-group vqueue-planner
```

**Ожидаем:** `создан …` для четырёх топиков, без `NON_EMPTY_GROUP` (если он есть — где-то ещё работает сервис: остановите и повторите).

**Шаг 5.3 · окно C · эталон для шума:**

```bash
python -m vqueue.generator --out work/telemetry_noisy.jsonl --duration 1800 --speedup 0 --start-ts 1757930400 --seed 1 \
       --dup-rate 0.2 --late-rate 0.2
python -m vqueue.simulate --replay work/telemetry_noisy.jsonl \
       --queue-out work/golden_queue_noisy.jsonl --decision-out work/golden_decision_noisy.jsonl --print 0
```

**Ожидаем:** `{'accepted': 57820, 'ignored_old': 28378, …}`, `queue.v1: 2940 событий; decision.v1: 41`.

**Шаг 5.4 · окно A · запустить сервис** (без этого шага выход будет пустым):

```bash
python -m vqueue.service --config config/site.json
```

**Ожидаем** за 5–15 с: `снимка состояния нет — старт с чистого состояния`. Окно A оставьте работать.

**Шаг 5.5 · окно B · запустить генератор с шумом** (≈30 с):

```bash
python -m vqueue.generator --config config/site.json --speedup 60 --duration 1800 \
       --start-ts 1757930400 --seed 1 --dup-rate 0.2 --late-rate 0.2
```

**Шаг 5.6 · окно C · проверить вход и догон:**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic telemetry.v1
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
```

**Ожидаем:** `telemetry.v1:0:86198`; группа `vqueue-planner` существует, `LAG` = 0 (если нет — подождите 10–20 с). Ответ `group does not exist`
означает, что сервис не запущен — вернитесь к шагу 5.4. Генератор повторно **не** запускайте. В окне A: `'messages': 86198`, `'ignored_old': 28378`.

**Шаг 5.7 · окно C · сверка с эталоном шума:**

```bash
python -m vqueue.verify full --golden-queue work/golden_queue_noisy.jsonl \
       --golden-decision work/golden_decision_noisy.jsonl \
       --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
```

**Ожидаем:** `queue.v1 — 2940, decision.v1 — 41`, три `OK`, `ИТОГ: ВСЁ ПРОШЛО`.

---

## Этап 6. Резервный экземпляр и переключение

**Что проверяем.** Два сервиса работают в одной группе. Партицию входного топика в каждый момент держит только один (**активный**),
второй — **резерв**. Когда активный внезапно умирает, резерв должен сам подхватить работу из сохранённого снимка, и результат
обязан совпасть с эталоном: ничего не потеряно и не задвоено.

**Нужны четыре окна:** A1 (сервис №1), A2 (сервис №2), B (генератор), C (вспомогательное). Во всех четырёх активируйте venv и перейдите в корень проекта (этап 0).

### Шаг 6.1. Подготовка — окно C

Убедитесь, что никакие сервисы не запущены, и сделайте чистый старт:

```bash
pgrep -af vqueue.service
```

Вывод должен быть **пустым**. Если там что-то есть — остановите (`Ctrl+C` в соответствующем окне либо `pkill -9 -f vqueue.service`). Затем:

```bash
python scripts/create_topics.py --recreate --reset-group vqueue-planner
```

Если скрипт печатает `NON_EMPTY_GROUP` — где-то ещё работает сервис; топики под ним удалять нельзя. Остановите и повторите.

### Шаг 6.2. Запуск первого сервиса — окно A1

```bash
python -m vqueue.service --config config/site.json --max-batch 500
```

**Ожидаем** за 5–15 с: `… vqueue.runner: снимка состояния нет — старт с чистого состояния`. Первый сервис получил партицию.
Окно оставьте открытым.

### Шаг 6.3. Запуск второго сервиса — окно A2

```bash
python -m vqueue.service --config config/site.json --max-batch 501
```

(`500` и `501` — не настройка работы, а «метка», чтобы потом отличить процессы друг от друга по командной строке.)

Теперь в группе два участника, и Kafka перераспределяет партиции. **Ожидаем:**

- В одном из окон: `партиции не получены — резервный режим` — это резерв.
- В другом: `снимка состояния нет …` (возможно, ему предшествует `партиции отозваны: [0]` и сама строка встречается второй раз — это обычный
  ребаланс, ошибки нет). Это активный сервис.

Какое именно окно стало активным (A1 или A2) — **не определено заранее**: Kafka решает сама. Это нормально, запомнить нужно будет на шаге 6.5.
Подождите ~10 с, чтобы всё успокоилось.

### Шаг 6.4. Запуск генератора — окно B

Медленный генератор, около 90 секунд:

```bash
python -m vqueue.generator --config config/site.json --speedup 20 --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02
```

Окно B молча работает ~90 с и завершается.

### Шаг 6.5. Определить активный сервис и убить его — окна A1/A2 и C

Подождите ~25 с после старта генератора. В окнах A1 и A2 раз в 15 с появляются строки `stats=…`. **Смотрим на них:**

- у активного сервиса `'messages'` в строке `stats=` растёт, `now=` — дата вида `2025-09-15T10:…`;
- у резерва `'messages': 0` и `now=None`.

Допустим, активным оказался сервис из окна **A1** (запущен с `--max-batch 500`). Тогда в окне **C**:

```bash
pkill -9 -f 'max-batch 500'
```

Если активным был **A2** (`--max-batch 501`):

```bash
pkill -9 -f 'max-batch 501'
```

Команда завершает процесс . Окно убитого сервиса вернётся к приглашению оболочки — так и должно быть. **Резерв не трогайте.**

### Шаг 6.6. Переключение — окно резерва (A1 или A2)

Ничего не нажимая, ждите **до ~45 секунд** (так Kafka понимает, что участник умер: истекает таймаут его сессии). В окне бывшего резерва
**ожидаем**:

```
… vqueue.kafka: партиции отозваны: []
… vqueue.runner: состояние восстановлено из снимка, оффсеты {0: N}
```

где `N` — число из середины потока (не 0). Дальше в его строках `stats=` `'messages'` и `now=` начинают расти: он продолжил с того места, где
остановился убитый.

### Шаг 6.7. Дождаться конца и проверить — окно C

Дождитесь, пока генератор в окне B завершится, и сервис догонит вход:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group vqueue-planner
```

**Ожидаем:** `LAG` = 0 (если нет — повторите через 10–20 с). Затем главная сверка:

```bash
python -m vqueue.verify full --golden-queue work/golden_queue.jsonl --golden-decision work/golden_decision.jsonl --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
```

**Ожидаем:** `queue.v1 — 3424, decision.v1 — 41`, три `OK`, `ИТОГ: ВСЁ ПРОШЛО`.

> Эталон `work/golden_*.jsonl` получен на шаге 1.3 для именно этих параметров генератора. Если вы удаляли папку `work` — повторите 1.3.

**Этап пройден**, если: резерв сам подхватил работу (строка «состояние восстановлено из снимка, оффсеты {0: N}»), а `verify` показывает `ВСЁ ПРОШЛО`.

**Завершение этапа:** остановите оставшийся сервис (`Ctrl+C` в его окне).

---

## Этап 7. Потеря состояния: пересчёт с нуля

Проверяет сценарий из `ARCHITECTURE.md`, п. 6: если снимка нет, вход перечитывается, и **решения воспроизводятся идентично**.

**7.0** Нужен завершённый прогон этапа 3 (в топике `telemetry.v1` — 73457 сообщений, или 73459 после шага 3.6; параметры генератора как в 3.x).
Если его нет — повторите этап 3. Сервис остановите (`Ctrl+C`).

**7.1** Удалите выходы и состояние, оставив вход:

```bash
python scripts/create_topics.py --recreate --only queue.v1,decision.v1,vqueue.state.v1 --reset-group vqueue-planner
```

**7.2** Запустите сервис (терминал A).

**Ожидаем:** `снимка состояния нет — старт с чистого состояния`, затем сервис за 10–30 с проигрывает весь вход
(`'messages': 73457` в `stats=`; если делали шаг 3.6 — `73459`: к входу добавились два служебных сообщения, на выход они не влияют).

**7.3** `make verify`.

**Ожидаем:** `ИТОГ: ВСЁ ПРОШЛО` с теми же 3424/41 — то есть пересчёт с нуля дал **те же** очереди и решения, что и онлайн-обработка.

**7б (опционально) — повреждённый снимок.** Остановите сервис и запишите мусор в топик состояния:

```bash
echo 'state:{broken' | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
   --bootstrap-server localhost:9092 --topic vqueue.state.v1 --property parse.key=true --property key.separator=:
```

Запустите сервис и дождитесь `LAG` = 0. **Ожидаем** в логе `снимок повреждён — перечитываем вход с начала`. Выходные топики на этот раз
*не* пустые, поэтому в них окажутся повторы. Выполните `make verify` — он **ожидаемо покажет FAIL** (дубли решений, `at` идёт назад) и
запишет выгрузку в `work/actual_decision.jsonl`. Убедитесь, что повторы *побайтно одинаковые*:

```bash
sort -u work/actual_decision.jsonl | diff - <(sort -u work/golden_decision.jsonl) && echo "решения идентичны эталону"
```

**Ожидаем:** `решения идентичны эталону`. Это задокументированное поведение: потребитель решений должен дедуплицировать по `(unit_uuid, at)`.

---

## Этап 8. Замкнутый контур в реальном темпе (визуальная проверка)

Здесь телеметрия зависит от решений (водители выполняют рекомендации), поэтому с эталоном не сравниваем — проверяем инварианты и смысл.

**8.0** Остановите сервис, чистый старт (`create_topics.py --recreate --reset-group vqueue-planner`), запустите сервис (A).

**8.1** Терминал B (≈6 минут):

```bash
python -m vqueue.generator --config config/site.json --speedup 10 --duration 3600 --seed 1 \
       --dup-rate 0.01 --late-rate 0.02 --follow-decisions
```

**8.2** Терминал C — смотреть решения вживую:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
   --topic decision.v1 --isolation-level read_committed --property print.key=true
```

**Ожидаем:** по мере движения машин появляются `recommended` (чаще на менее загруженные S3/S4) и `rejected/no_gain`; для одной
машины новое решение появляется только после того, как она побывала на станции и снова приблизилась к своей на 1500 м.

**8.3** Когда генератор закончил и `LAG` = 0:

```bash
python -m vqueue.verify full --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
grep -c recommended work/actual_decision.jsonl
```

**Ожидаем:** `ИТОГ: ВСЁ ПРОШЛО` (эталона нет, проверяются формат и инварианты), число `recommended` больше нуля.

---

## Завершение

```bash
docker compose down -v        # остановить брокер и удалить данные
deactivate
```

---
## Если что-то пошло не так

| Симптом | Вероятная причина и что делать |
|---|---|
| `Local: Unknown topic`, `UNKNOWN_TOPIC_OR_PART` при старте сервиса | Топики не созданы — выполните `create_topics.py`. |
| `Coordinator not available`, долго нет `снимка состояния нет …` | Брокер ещё поднимается или группа вступает — подождите до 30 с. Проверьте `docker compose logs kafka`. |
| `Local: Fatal error`, `FatalTransportError` в логе сервиса | Нас вытеснил другой экземпляр с тем же `transactional.id`, либо брокер отверг транзакцию. Пришлите лог целиком. |
| `make verify` долго ничего не читает | Открытая транзакция убитого процесса; подождите до минуты или запустите сервис. |
| `прочитано: queue.v1 — 0, decision.v1 — 0`, `выходные топики пусты` | Сервис не запущен/упал или не успел обработать вход. В окне A сервис должен работать; `kafka-consumer-groups.sh --describe --group vqueue-planner` должен показать участника и `LAG` = 0; `kafka-get-offsets.sh --topic telemetry.v1` — число сообщений на входе. Генератор повторно **не** запускайте: данные уже в топике. |
| Сверка: «расхождение; ожидалось N, получено M» | Это главный сигнал. Пришлите вывод `verify` (там первое отличие эталона и факта) и параметры, с которыми запускали генератор. Сначала проверьте, что параметры генератора **точно** совпадают с эталоном и `telemetry.v1` содержит ровно 73457 сообщений. |
| Сверка: «лишний в фактическом выходе» / «нет в фактическом выходе» | Скорее всего, топики не были пересозданы перед прогоном (остались данные предыдущего этапа). Повторите чистый старт. |
| `LAG` не падает до 0 | Сервис упал или завис — смотрите терминал A; `kafka-consumer-groups.sh --describe` покажет, назначена ли партиция. |
| Числа в эталоне отличаются от «ожидаемых» | Допустимо, если сверка с вашим эталоном проходит (эталон строится у вас из той же телеметрии). Но если расходятся входные 73457 строк — пришлите версию Python и ОС. |
