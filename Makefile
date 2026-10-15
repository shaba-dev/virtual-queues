PY ?= python3
export PYTHONPATH := $(CURDIR)/src
GEN = --duration 1800 --start-ts 1757930400 --seed 1 --dup-rate 0.02 --late-rate 0.02

test:                ## все тесты, брокер не нужен
	$(PY) -m unittest discover -s tests -t . -v

simulate:            ## прогон симулятор -> движок без брокера
	$(PY) -m vqueue.simulate --config config/site.json --minutes 60

example:             ## пример из задания без брокера
	$(PY) scripts/check_spec_example.py

golden:              ## эталон для сценария из TESTING.md (телеметрия + ожидаемые queue/decision)
	mkdir -p work
	$(PY) -m vqueue.generator --stdout --speedup 0 $(GEN) > work/telemetry.jsonl
	$(PY) -m vqueue.simulate --replay work/telemetry.jsonl --queue-out work/golden_queue.jsonl --decision-out work/golden_decision.jsonl --print 0

up:                  ## брокер + топики с нуля
	docker compose up -d
	sleep 15
	$(PY) scripts/create_topics.py --recreate --reset-group vqueue-planner

service:
	$(PY) -m vqueue.service --config config/site.json

generator:           ## 30 секунд реального времени (ускорение x60), те же параметры, что у эталона
	$(PY) -m vqueue.generator --config config/site.json --speedup 60 $(GEN)

verify:              ## прочитать выходные топики, проверить и сверить с эталоном
	$(PY) -m vqueue.verify full --golden-queue work/golden_queue.jsonl --golden-decision work/golden_decision.jsonl \
	  --queue-out work/actual_queue.jsonl --decision-out work/actual_decision.jsonl
