<?php

namespace App\Services;

class RedisQueue
{

    const COUNTER_FAIL = 'counter-fail';

    const STREAM1 = 'stream1';
    const STREAM2 = 'stream2';
    const STREAM3 = 'stream3';

    const STREAM1_GROUP = 'group1';
    const STREAM2_GROUP = 'group2';
    const STREAM3_GROUP = 'group3';

    const TASK_TIMEOUT = 60;

    private \Redis $redis;

    private function __construct()
    {
    }

    public static function make(): self
    {
        $redis = new \Redis();
        $redis->connect('redis', 6379);
        $self = new self();
        $self->redis = $redis;
        return $self;
    }

    public function connector(): \Redis
    {
        return $this->redis;
    }

    public function counterFailInc()
    {
        $this->redis->incr(self::COUNTER_FAIL);
    }
    public function counterFail()
    {
        return $this->redis->get(self::COUNTER_FAIL);
    }


    public function init(): void
    {
        $this->redis->flushAll();
        $this->redis->xGroup('CREATE', self::STREAM1, self::STREAM1_GROUP, '0', true);
        $this->redis->xGroup('CREATE', self::STREAM2, self::STREAM2_GROUP, '0', true);
        $this->redis->xGroup('CREATE', self::STREAM3, self::STREAM3_GROUP, '0', true);
    }

    public function write($stream, array $message)
    {
        $this->redis->xAdd($stream, '*', $message);
    }


    public function read($stream, $group, $consumer): ?array
    {
        // сначала проверяем подвисшие сообщения
        $streamsPendingMsgs = $this->redis->xAutoClaim($stream, $group, $consumer, self::TASK_TIMEOUT * 1000, 0, 1);
        [$nextMsgId, $msgs, $deleted] = $streamsPendingMsgs;
        // если подвисших нет, то читаем из не подвисших
        if (empty($msgs)) {
            $streamsMsgs = $this->redis->xReadGroup($group, $consumer, [$stream => '>'], 1, 5000);
            $msgs = $streamsMsgs[$stream] ?? [];
        }
        foreach ($msgs as $msgId => $msg) {
            return [$msgId, $msg];
        }
        return null;
    }

    public function ack($stream, $group, $msgId): void
    {
        // просто удалением не обойтись, т.к. сообщение остается в pel-списке. Подтверждение его оттуда удаляет.
        // вместо xDel можно ограничивать длину стрима MAXLEN
        $this->redis->xAck($stream, $group, [$msgId]);
        $this->redis->xDel($stream, [$msgId]);
    }


}
