package taskflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrDuplicateTask is returned when a unique task is already active in the queue.
var ErrDuplicateTask = errors.New("taskflow: task with unique key already exists")

// RedisBroker manages queue storage, atomic state transitions, and Lua scripts.
type RedisBroker struct {
	client redis.UniversalClient
	prefix string
}

// NewRedisBroker creates a new RedisBroker instance.
func NewRedisBroker(client redis.UniversalClient, prefix string) *RedisBroker {
	if prefix == "" {
		prefix = "taskflow"
	}
	return &RedisBroker{
		client: client,
		prefix: prefix,
	}
}

func (b *RedisBroker) keyQueue(q string) string {
	return fmt.Sprintf("%s:queue:%s", b.prefix, q)
}

func (b *RedisBroker) keyProcessing(q string) string {
	return fmt.Sprintf("%s:processing:%s", b.prefix, q)
}

func (b *RedisBroker) keyDelayed(q string) string {
	return fmt.Sprintf("%s:delayed:%s", b.prefix, q)
}

func (b *RedisBroker) keyDLQ(q string) string {
	return fmt.Sprintf("%s:dlq:%s", b.prefix, q)
}

func (b *RedisBroker) keyUnique(key string) string {
	return fmt.Sprintf("%s:unique:%s", b.prefix, key)
}

// Lua script for atomic enqueue with optional unique key deduplication (using millisecond precision).
var enqueueScript = redis.NewScript(`
local queueKey = KEYS[1]
local delayedKey = KEYS[2]
local uniqueKey = KEYS[3]

local taskData = ARGV[1]
local processAtMilli = tonumber(ARGV[2])
local uniqueTTLMilli = tonumber(ARGV[3])
local nowMilli = tonumber(ARGV[4])

if uniqueKey ~= "" and uniqueTTLMilli > 0 then
    local set = redis.call("SET", uniqueKey, "1", "NX", "PX", uniqueTTLMilli)
    if not set then
        return 0 -- duplicate task
    end
end

if processAtMilli > nowMilli then
    redis.call("ZADD", delayedKey, processAtMilli, taskData)
else
    redis.call("LPUSH", queueKey, taskData)
end

return 1
`)

// Enqueue adds a task into the pending or delayed queue.
func (b *RedisBroker) Enqueue(ctx context.Context, task *Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	uniqueKey := ""
	var uniqueTTLMilli int64
	if task.UniqueKey != "" && task.UniqueTTL > 0 {
		uniqueKey = b.keyUnique(task.UniqueKey)
		uniqueTTLMilli = task.UniqueTTL.Milliseconds()
	}

	queueKey := b.keyQueue(task.Queue)
	delayedKey := b.keyDelayed(task.Queue)

	nowMilli := time.Now().UTC().UnixMilli()
	processAtMilli := task.ProcessAt.UnixMilli()

	res, err := enqueueScript.Run(ctx, b.client,
		[]string{queueKey, delayedKey, uniqueKey},
		string(data), processAtMilli, uniqueTTLMilli, nowMilli,
	).Int()

	if err != nil {
		return err
	}

	if res == 0 {
		return ErrDuplicateTask
	}

	return nil
}

// Fetch fetches a pending task from the queue, atomically moving it to processing list.
func (b *RedisBroker) Fetch(ctx context.Context, queue string, timeout time.Duration) (*Task, error) {
	queueKey := b.keyQueue(queue)
	processingKey := b.keyProcessing(queue)

	res, err := b.client.BRPopLPush(ctx, queueKey, processingKey, timeout).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil // timeout, no task available
		}
		return nil, err
	}

	var t Task
	if err := json.Unmarshal([]byte(res), &t); err != nil {
		return nil, err
	}
	t.raw = res

	return &t, nil
}

// Ack removes the task from the processing list upon successful completion.
func (b *RedisBroker) Ack(ctx context.Context, task *Task) error {
	raw := task.raw
	if raw == "" {
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		raw = string(data)
	}

	processingKey := b.keyProcessing(task.Queue)
	if err := b.client.LRem(ctx, processingKey, 1, raw).Err(); err != nil {
		return err
	}

	if task.UniqueKey != "" {
		_ = b.client.Del(ctx, b.keyUnique(task.UniqueKey)).Err()
	}

	return nil
}

// Retry queues a task for retry, moving it from processing to delayed queue.
func (b *RedisBroker) Retry(ctx context.Context, task *Task, delay time.Duration, lastErr error) error {
	raw := task.raw
	if raw == "" {
		d, _ := json.Marshal(task)
		raw = string(d)
	}

	task.Retried++
	if lastErr != nil {
		task.LastError = lastErr.Error()
	}
	task.ProcessAt = time.Now().UTC().Add(delay)

	newData, err := json.Marshal(task)
	if err != nil {
		return err
	}

	processingKey := b.keyProcessing(task.Queue)
	delayedKey := b.keyDelayed(task.Queue)

	pipe := b.client.TxPipeline()
	pipe.LRem(ctx, processingKey, 1, raw)
	pipe.ZAdd(ctx, delayedKey, redis.Z{
		Score:  float64(task.ProcessAt.UnixMilli()),
		Member: string(newData),
	})

	_, err = pipe.Exec(ctx)
	return err
}

// RouteToDLQ moves an exhausted task to the Dead Letter Queue.
func (b *RedisBroker) RouteToDLQ(ctx context.Context, task *Task, finalErr error) error {
	raw := task.raw
	if raw == "" {
		d, _ := json.Marshal(task)
		raw = string(d)
	}

	if finalErr != nil {
		task.LastError = finalErr.Error()
	}

	data, err := json.Marshal(task)
	if err != nil {
		return err
	}

	processingKey := b.keyProcessing(task.Queue)
	dlqKey := b.keyDLQ(task.Queue)

	pipe := b.client.TxPipeline()
	pipe.LRem(ctx, processingKey, 1, raw)
	pipe.LPush(ctx, dlqKey, string(data))

	if task.UniqueKey != "" {
		pipe.Del(ctx, b.keyUnique(task.UniqueKey))
	}

	_, err = pipe.Exec(ctx)
	return err
}

// Lua script to atomically migrate due delayed tasks to the active pending queue.
var migrateDelayedScript = redis.NewScript(`
local delayedKey = KEYS[1]
local queueKey = KEYS[2]
local maxScoreMilli = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])

local tasks = redis.call("ZRANGEBYSCORE", delayedKey, "-inf", maxScoreMilli, "LIMIT", 0, limit)
if #tasks > 0 then
    for _, task in ipairs(tasks) do
        redis.call("LPUSH", queueKey, task)
    end
    redis.call("ZREMRANGEBYSCORE", delayedKey, "-inf", maxScoreMilli)
end

return #tasks
`)

// MigrateDelayed moves due tasks from delayed ZSet to pending queue.
func (b *RedisBroker) MigrateDelayed(ctx context.Context, queue string, limit int) (int, error) {
	return b.MigrateDelayedUntil(ctx, queue, time.Now().UTC().UnixMilli(), limit)
}

// MigrateDelayedUntil moves tasks scheduled up to maxTimestampMilli from delayed ZSet to pending queue.
func (b *RedisBroker) MigrateDelayedUntil(ctx context.Context, queue string, maxTimestampMilli int64, limit int) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	delayedKey := b.keyDelayed(queue)
	queueKey := b.keyQueue(queue)

	res, err := migrateDelayedScript.Run(ctx, b.client,
		[]string{delayedKey, queueKey},
		maxTimestampMilli, limit,
	).Int()

	return res, err
}

// QueueDepth returns the number of tasks in pending, processing, delayed, and DLQ.
func (b *RedisBroker) QueueDepth(ctx context.Context, queue string) (pending, processing, delayed, dlq int64, err error) {
	pipe := b.client.Pipeline()
	pCmd := pipe.LLen(ctx, b.keyQueue(queue))
	prCmd := pipe.LLen(ctx, b.keyProcessing(queue))
	dCmd := pipe.ZCard(ctx, b.keyDelayed(queue))
	dlqCmd := pipe.LLen(ctx, b.keyDLQ(queue))

	_, err = pipe.Exec(ctx)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	return pCmd.Val(), prCmd.Val(), dCmd.Val(), dlqCmd.Val(), nil
}
