package sql_test

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/message/subscriber"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/davideimola/watermill-pgx/pkg/sql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	logger = watermill.NewStdLogger(true, false)
)

func newPubSub(t *testing.T, db *pgxpool.Pool, consumerGroup string, schemaAdapter sql.SchemaAdapter, offsetsAdapter sql.OffsetsAdapter) (message.Publisher, message.Subscriber) {
	publisher, err := sql.NewPublisher(
		db,
		sql.PublisherConfig{
			SchemaAdapter: schemaAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	subscriber, err := sql.NewSubscriber(
		db,
		sql.SubscriberConfig{
			ConsumerGroup: consumerGroup,

			PollInterval:   1 * time.Millisecond,
			ResendInterval: 5 * time.Millisecond,
			SchemaAdapter:  schemaAdapter,
			OffsetsAdapter: offsetsAdapter,
		},
		logger,
	)
	require.NoError(t, err)

	return publisher, subscriber
}

func newPgxPostgreSQL(t *testing.T) *pgxpool.Pool {
	addr := os.Getenv("WATERMILL_TEST_POSTGRES_HOST")
	if addr == "" {
		addr = "localhost"
	}

	connStr := fmt.Sprintf("postgres://watermill:password@%s/watermill?sslmode=disable", addr)
	conf, err := pgxpool.ParseConfig(connStr)
	require.NoError(t, err)

	db, err := pgxpool.NewWithConfig(context.Background(), conf)

	err = db.Ping(context.Background())
	require.NoError(t, err)

	return db
}

func createPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	return newPubSub(
		t,
		newPgxPostgreSQL(t),
		consumerGroup,
		newPostgresSchemaAdapter(0),
		newPostgresOffsetsAdapter(),
	)
}

func newPostgresOffsetsAdapter() sql.DefaultPostgreSQLOffsetsAdapter {
	return sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_offsets_%s"`, topic)
		},
	}
}

func newPostgresSchemaAdapter(batchSize int) *sql.DefaultPostgreSQLSchema {
	return &sql.DefaultPostgreSQLSchema{
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
		SubscribeBatchSize: batchSize,
	}
}

func createPgxPostgreSQLPubSubWithConsumerGroup(t *testing.T, consumerGroup string) (message.Publisher, message.Subscriber) {
	schemaAdapter := &sql.DefaultPostgreSQLSchema{
		GenerateMessagesTableName: func(topic string) string {
			return fmt.Sprintf(`"test_%s"`, topic)
		},
		GeneratePayloadType: func(topic string) string {
			return "BYTEA"
		},
	}

	offsetsAdapter := sql.DefaultPostgreSQLOffsetsAdapter{
		GenerateMessagesOffsetsTableName: func(topic string) string {
			return fmt.Sprintf(`"test_offsets_%s"`, topic)
		},
	}

	return newPubSub(t, newPgxPostgreSQL(t), consumerGroup, schemaAdapter, offsetsAdapter)
}

func createPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func createPgxPostgreSQLPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	return createPgxPostgreSQLPubSubWithConsumerGroup(t, "test")
}

func TestPostgreSQLPublishSubscribe(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: true,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPostgreSQLPubSub,
		createPostgreSQLPubSubWithConsumerGroup,
	)
}

func TestPgxPostgreSQLPublishSubscribe(t *testing.T) {
	t.Parallel()

	features := tests.Features{
		ConsumerGroups:      true,
		ExactlyOnceDelivery: false,
		GuaranteedOrder:     true,
		Persistent:          true,
	}

	tests.TestPubSub(
		t,
		features,
		createPgxPostgreSQLPubSub,
		createPgxPostgreSQLPubSubWithConsumerGroup,
	)
}

func TestCtxValues(t *testing.T) {
	pubSubConstructors := []struct {
		Name        string
		Constructor func(t *testing.T) (message.Publisher, message.Subscriber)
	}{
		{
			Name:        "postgresql",
			Constructor: createPostgreSQLPubSub,
		},
	}

	for _, constructor := range pubSubConstructors {
		pub, sub := constructor.Constructor(t)

		t.Run(constructor.Name, func(t *testing.T) {
			t.Parallel()
			topicName := "topic_" + watermill.NewUUID()

			err := sub.(message.SubscribeInitializer).SubscribeInitialize(topicName)
			require.NoError(t, err)

			var messagesToPublish []*message.Message

			id := watermill.NewUUID()
			messagesToPublish = append(messagesToPublish, message.NewMessage(id, nil))

			err = pub.Publish(topicName, messagesToPublish...)
			require.NoError(t, err, "cannot publish message")

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			messages, err := sub.Subscribe(ctx, topicName)
			require.NoError(t, err)

			select {
			case msg := <-messages:
				tx, ok := sql.TxFromContext(msg.Context())
				assert.True(t, ok)
				assert.NotNil(t, t, tx)
				msg.Ack()
			case <-time.After(time.Second * 10):
				t.Fatal("no message received")
			}
		})
	}
}

// TestNotMissingMessages checks if messages are not missing when messages are published in concurrent transactions.
// See more: https://github.com/ThreeDotsLabs/watermill/issues/311
func TestNotMissingMessages(t *testing.T) {
	t.Parallel()

	pubSubs := []struct {
		Name           string
		DbConstructor  func(t *testing.T) *pgxpool.Pool
		SchemaAdapter  sql.SchemaAdapter
		OffsetsAdapter sql.OffsetsAdapter
	}{
		{
			Name:           "postgresql",
			DbConstructor:  newPgxPostgreSQL,
			SchemaAdapter:  newPostgresSchemaAdapter(0),
			OffsetsAdapter: newPostgresOffsetsAdapter(),
		},
	}

	for _, pubSub := range pubSubs {
		pubSub := pubSub

		t.Run(pubSub.Name, func(t *testing.T) {
			t.Parallel()

			db := pubSub.DbConstructor(t)

			topicName := "topic_" + watermill.NewUUID()

			messagesToPublish := []*message.Message{
				message.NewMessage("0", nil),
				message.NewMessage("1", nil),
				message.NewMessage("2", nil),
			}

			sub, err := sql.NewSubscriber(
				db,
				sql.SubscriberConfig{
					ConsumerGroup: "consumerGroup",

					PollInterval:   1 * time.Millisecond,
					ResendInterval: 5 * time.Millisecond,
					SchemaAdapter:  pubSub.SchemaAdapter,
					OffsetsAdapter: pubSub.OffsetsAdapter,
				},
				logger,
			)
			require.NoError(t, err)

			err = sub.SubscribeInitialize(topicName)
			require.NoError(t, err)

			messagesAsserted := make(chan struct{})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				defer close(messagesAsserted)

				messages, err := sub.Subscribe(ctx, topicName)
				require.NoError(t, err)

				received, all := subscriber.BulkRead(messages, len(messagesToPublish), time.Second*10)
				assert.True(t, all)

				tests.AssertAllMessagesReceived(t, messagesToPublish, received)
			}()

			tx0, err := db.BeginTx(ctx, pgx.TxOptions{
				IsoLevel: pgx.ReadCommitted,
			})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			tx1, err := db.BeginTx(ctx, pgx.TxOptions{
				IsoLevel: pgx.ReadCommitted,
			})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			txRollback, err := db.BeginTx(ctx, pgx.TxOptions{
				IsoLevel: pgx.ReadCommitted,
			})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			tx2, err := db.BeginTx(ctx, pgx.TxOptions{
				IsoLevel: pgx.ReadCommitted,
			})
			assert.NoError(t, err)
			time.Sleep(time.Millisecond * 10)

			pub0, err := sql.NewPublisher(
				tx0,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub0.Publish(topicName, messagesToPublish[0])
			require.NoError(t, err, "cannot publish message")

			pub1, err := sql.NewPublisher(
				tx1,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub1.Publish(topicName, messagesToPublish[1])
			require.NoError(t, err, "cannot publish message")

			pubRollback, err := sql.NewPublisher(
				txRollback,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pubRollback.Publish(topicName, message.NewMessage("rollback", nil))
			require.NoError(t, err, "cannot publish message")

			pub2, err := sql.NewPublisher(
				tx2,
				sql.PublisherConfig{
					SchemaAdapter: pubSub.SchemaAdapter,
				},
				logger,
			)
			require.NoError(t, err)
			err = pub2.Publish(topicName, messagesToPublish[2])
			require.NoError(t, err, "cannot publish message")

			require.NoError(t, tx2.Commit(ctx))
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, txRollback.Rollback(ctx))
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, tx1.Commit(ctx))
			time.Sleep(time.Millisecond * 10)

			require.NoError(t, tx0.Commit(ctx))
			time.Sleep(time.Millisecond * 10)

			<-messagesAsserted
		})
	}
}

func TestConcurrentSubscribe_different_bulk_sizes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Name        string
		Constructor func(t *testing.T) (message.Publisher, message.Subscriber)
		Test        func(t *testing.T, tCtx tests.TestContext, pubSubConstructor tests.PubSubConstructor)
	}{
		{
			Name: "TestConcurrentSubscribe_postgresql_1",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPgxPostgreSQL(t),
					"test",
					newPostgresSchemaAdapter(1),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestPublishSubscribe,
		},
		{
			Name: "TestConcurrentSubscribe_postgresql_5",
			Constructor: func(t *testing.T) (message.Publisher, message.Subscriber) {
				return newPubSub(
					t,
					newPgxPostgreSQL(t),
					"test",
					newPostgresSchemaAdapter(5),
					newPostgresOffsetsAdapter(),
				)
			},
			Test: tests.TestConcurrentSubscribe,
		},
	}
	for i := range testCases {
		tc := testCases[i]

		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			tc.Test(
				t,
				tests.TestContext{
					TestID: tests.NewTestID(),
					Features: tests.Features{
						ConsumerGroups:                      true,
						ExactlyOnceDelivery:                 true,
						GuaranteedOrder:                     true,
						GuaranteedOrderWithSingleSubscriber: true,
						Persistent:                          true,
					},
				},
				tc.Constructor,
			)
		})
	}
}

func TestDefaultPostgreSQLSchema_planner_mis_estimate_regression(t *testing.T) {
	// this test should be not executed in Parallel to not disturb performance measurements

	db := newPgxPostgreSQL(t)

	offsetsAdapter := newPostgresOffsetsAdapter()

	pub, sub := newPubSub(
		t,
		db,
		"test",
		newPostgresSchemaAdapter(1000),
		offsetsAdapter,
	)

	topicName := "topic_" + watermill.NewUUID()

	err := sub.(message.SubscribeInitializer).SubscribeInitialize(topicName)
	require.NoError(t, err)

	messagesCount := 100_000
	if testing.Short() {
		messagesCount = 1_000
	}
	tests.AddSimpleMessagesParallel(t, messagesCount, pub, topicName, 50)

	subscribeCtx, cancelSubscribe := context.WithCancel(context.Background())

	messages, err := sub.Subscribe(subscribeCtx, topicName)
	require.NoError(t, err)

	// we want to consume most of the messages,
	// but not all to catch performance issues with more unacked messages
	messagesToConsume := int(float64(messagesCount) * 0.8)
	_, all := subscriber.BulkRead(messages, messagesToConsume, time.Minute)
	assert.True(t, all)

	cancelSubscribe()
	<-messages // wait for the subscriber to finish

	schemAdapterBatch1 := newPostgresSchemaAdapter(1)
	q := schemAdapterBatch1.SelectQuery(topicName, "", offsetsAdapter)

	var analyseResult string

	res, err := db.Query(context.Background(), "EXPLAIN ANALYZE\n"+q.Query, q.Args...)
	require.NoError(t, err)

	for res.Next() {
		var line string
		err := res.Scan(&line)
		require.NoError(t, err)
		analyseResult += line + "\n"
	}
	res.Close()

	t.Log(analyseResult)

	rowsRemovedByFilter := findRowsRemovedByFilterInAnalyze(analyseResult)

	for _, i := range rowsRemovedByFilter {
		assert.LessOrEqual(
			t,
			i,
			1,
			"too many rows removed by filter - it's likely a performance regression",
		)
	}

	duration := extractDurationFromAnalyze(t, analyseResult)

	// TBD if it will be stable in CI
	assert.LessOrEqual(t, duration, time.Millisecond, "query duration is too long")
}

func findRowsRemovedByFilterInAnalyze(input string) []int {
	pattern := `Rows Removed by Filter: (\d+)`
	re := regexp.MustCompile(pattern)

	matches := re.FindAllStringSubmatch(input, -1)

	result := make([]int, 0, len(matches))

	for _, match := range matches {
		if len(match) > 1 {
			value, err := strconv.Atoi(match[1])
			if err == nil {
				result = append(result, value)
			}
		}
	}

	return result
}

func extractDurationFromAnalyze(t *testing.T, s string) time.Duration {
	t.Helper()

	// Regular expression to match "Execution Time: X.XXX ms" pattern
	re := regexp.MustCompile(`Execution Time:\s*(\d+(?:\.\d+)?)\s*ms`)

	match := re.FindStringSubmatch(s)
	if len(match) != 2 {
		t.Fatalf("cannot find duration in the string: %s", s)
	}

	durationStr := match[1]

	parsed, err := time.ParseDuration(durationStr + "ms")
	require.NoError(t, err)

	return parsed
}

// TestDefaultPostgreSQLSchema_orders_by_transaction_id_across_a_decimal_digit
// publishes one message right below a power of ten of the transaction id and a
// batch right above it. Ordered as text, "1000" sorts before "999": the
// subscriber acks the newer rows first and the older message is never
// delivered.
func TestDefaultPostgreSQLSchema_orders_by_transaction_id_across_a_decimal_digit(t *testing.T) {
	db := newPgxPostgreSQL(t)
	schemaAdapter := newPostgresSchemaAdapter(100)

	pub, sub := newPubSub(t, db, "test", schemaAdapter, newPostgresOffsetsAdapter())

	topicName := "topic_" + watermill.NewUUID()
	require.NoError(t, sub.(message.SubscribeInitializer).SubscribeInitialize(topicName))

	current := currentTransactionID(t, db)
	boundary := uint64(10)
	for boundary <= current+10 {
		boundary *= 10
	}
	if boundary-current > maxTransactionIDsToBurn {
		t.Skipf("transaction id %d is too far from %d to burn the difference", current, boundary)
	}

	burnTransactionIDs(t, db, boundary-5)
	older := message.NewMessage(watermill.NewUUID(), []byte("older"))
	require.NoError(t, pub.Publish(topicName, older))

	burnTransactionIDs(t, db, boundary)
	newer := make([]*message.Message, 0, 250)
	for i := 0; i < 250; i++ {
		newer = append(newer, message.NewMessage(watermill.NewUUID(), []byte(strconv.Itoa(i))))
	}
	require.NoError(t, pub.Publish(topicName, newer...))

	var olderDigits, newerDigits int
	require.NoError(t, db.QueryRow(context.Background(), fmt.Sprintf(
		`SELECT min(length(transaction_id::text)), max(length(transaction_id::text)) FROM %s`,
		schemaAdapter.MessagesTable(topicName),
	)).Scan(&olderDigits, &newerDigits))
	require.Less(t, olderDigits, newerDigits, "the messages must straddle a decimal digit of the transaction id")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := sub.Subscribe(ctx, topicName)
	require.NoError(t, err)

	received, all := subscriber.BulkReadWithDeduplication(messages, len(newer)+1, 5*time.Second)
	assert.True(t, all, "received %d of %d messages", len(received), len(newer)+1)
	require.NotEmpty(t, received)
	assert.Equal(t, older.UUID, received[0].UUID, "the message of the older transaction must be delivered first")
}

// maxTransactionIDsToBurn bounds how long the test may take on a long-lived
// database, where the next power of ten can be far away.
const maxTransactionIDsToBurn = 20_000_000

func currentTransactionID(t *testing.T, db *pgxpool.Pool) uint64 {
	var xid uint64
	require.NoError(t, db.QueryRow(context.Background(), `SELECT pg_current_xact_id()::text::bigint`).Scan(&xid))

	return xid
}

// burnTransactionIDs consumes transaction ids until the next transaction is
// assigned at least target. Every subtransaction that writes takes an id of its
// own, so one statement can burn many of them.
func burnTransactionIDs(t *testing.T, db *pgxpool.Pool, target uint64) {
	for {
		current := currentTransactionID(t, db)
		if current+1 >= target {
			return
		}

		burn := target - current - 2
		if burn == 0 {
			continue
		}

		_, err := db.Exec(context.Background(), fmt.Sprintf(`DO $$
			BEGIN
				CREATE TEMP TABLE IF NOT EXISTS watermill_xid_burn (x int);
				FOR i IN 1..%d LOOP
					BEGIN
						INSERT INTO watermill_xid_burn VALUES (1);
					EXCEPTION WHEN others THEN NULL;
					END;
				END LOOP;
			END $$`, burn))
		require.NoError(t, err)
	}
}
