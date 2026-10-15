"""In-memory брокер и транспорт: проверяют раннер без Kafka."""
from vqueue.runner import REPLAY_FROM_START, InMsg


class FakeBroker:
    """Одна партиция входа + журнал выходных сообщений + топик состояния."""

    def __init__(self):
        self.log: list[bytes] = []              # telemetry.v1
        self.out = []                           # зафиксированные OutMsg (то, что видят read_committed-читатели)
        self.snapshot = None                    # последнее значение топика состояния
        self.committed = 0                      # оффсет группы
        self.fail_commits = 0                   # сколько ближайших коммитов уронить


class FakeTransport:
    def __init__(self, broker: FakeBroker):
        self.b, self.pos, self.rollbacks = broker, 0, 0

    def start(self, restore):
        offsets = restore(self.b.snapshot)
        if offsets == REPLAY_FROM_START:
            self.pos = 0
        elif offsets:
            self.pos = offsets.get(0, 0)
        else:
            self.pos = self.b.committed

    def poll(self, max_messages, timeout):
        chunk = self.b.log[self.pos:self.pos + max_messages]
        msgs = [InMsg(0, self.pos + i, None, v) for i, v in enumerate(chunk)]
        self.pos += len(chunk)
        return msgs

    def commit(self, outputs, snapshot, offsets):
        if self.b.fail_commits > 0:
            self.b.fail_commits -= 1
            raise RuntimeError("брокер недоступен")
        self.b.out.extend(outputs)
        self.b.snapshot = snapshot
        self.b.committed = offsets[0]

    def rollback(self, start_offsets):
        self.rollbacks += 1
        self.pos = start_offsets[0]

    def close(self):
        pass
