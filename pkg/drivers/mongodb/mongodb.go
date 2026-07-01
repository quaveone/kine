package mongodb

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/metrics"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"k8s.io/client-go/util/workqueue"
)

const (
	retryInterval       = 250 * time.Millisecond
	minCompactBatchSize = 100
	revisionDocumentID  = "global"
)

func init() {
	drivers.Register("mongodb", New)
}

const (
	defaultConnectionURI = "mongodb://localhost:27017"
	defaultDatabase      = "kine"
)

// KineReg is the document stored in the kine collection.
// Each document represents one revision event (create, update, or delete) for a key.
type KineReg struct {
	ID             bson.ObjectID `bson:"_id,omitempty"`
	Revision       int64         `bson:"revision"`
	Key            string        `bson:"key"`
	Created        int64         `bson:"created"`
	Deleted        int64         `bson:"deleted"`
	CreateRevision int64         `bson:"createRevision"`
	PrevRevision   int64         `bson:"prevRevision"`
	Lease          int64         `bson:"lease"`
	Value          []byte        `bson:"value,omitempty"`
	PrevValue      []byte        `bson:"prevValue,omitempty"`
	Version        int64         `bson:"version"`
}

// RevisionReg is the single document in the revision collection used as an atomic counter.
// It also stores the last compacted revision so that Get/Watch can return ErrCompacted
// for historical reads below the compact point.
type RevisionReg struct {
	Revision        int64 `bson:"revision"`
	CompactRevision int64 `bson:"compactRevision,omitempty"`
}

// CollStats holds size information for a collection.
type CollStats struct {
	Size int64 `bson:"size"`
}

func (kr *KineReg) toKeyValue(keysOnly bool) *server.KeyValue {
	kv := &server.KeyValue{
		Key:            kr.Key,
		CreateRevision: kr.CreateRevision,
		ModRevision:    kr.Revision,
		Version:        kr.Version,
		Lease:          kr.Lease,
	}
	if !keysOnly {
		kv.Value = kr.Value
	}
	return kv
}

// ttlEventKV tracks when a key with a TTL lease should be deleted.
type ttlEventKV struct {
	key         string
	modRevision int64
	expiredAt   time.Time
}

// MongoBackend implements server.Backend using MongoDB.
type MongoBackend struct {
	client                *mongo.Client
	db                    *mongo.Database
	coll                  *mongo.Collection
	collRevision          *mongo.Collection
	compactInterval       time.Duration
	compactIntervalJitter int
	compactTimeout        time.Duration
	compactMinRetain      int64
	compactBatchSize      int64
	// watchMu and watchCh implement a broadcast-on-write mechanism for Watch subscribers.
	// After every append, the current watchCh is closed and replaced with a new one,
	// waking all goroutines waiting on the old channel.
	watchMu sync.RWMutex
	watchCh chan struct{}
}

// New creates a MongoBackend and registers it under the "mongodb" scheme.
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	uri := cfg.DataSourceName
	if uri == "" {
		uri = defaultConnectionURI
	}
	if !strings.HasPrefix(uri, "mongodb://") && !strings.HasPrefix(uri, "mongodb+srv://") {
		uri = "mongodb://" + uri
	}

	// Extract database name from URI path (e.g. mongodb://host/mydb → mydb).
	dbName := defaultDatabase
	if u, err := url.Parse(uri); err == nil {
		if db := strings.TrimPrefix(u.Path, "/"); db != "" {
			dbName = db
		}
	}

	clientOpts := options.Client().ApplyURI(uri)

	if tlsCfg, err := cfg.BackendTLSConfig.ClientConfig(); err != nil {
		return false, nil, fmt.Errorf("building TLS config for MongoDB: %w", err)
	} else if tlsCfg != nil {
		clientOpts.SetTLSConfig(tlsCfg)
	}

	compactTimeout := cfg.CompactTimeout
	if compactTimeout == 0 {
		compactTimeout = 5 * time.Second
	}
	compactBatchSize := cfg.CompactBatchSize
	if compactBatchSize == 0 {
		compactBatchSize = 1000
	}

	client, err := mongo.Connect(clientOpts)
	if err != nil {
		return false, nil, fmt.Errorf("connecting to MongoDB: %w", err)
	}

	b := &MongoBackend{
		client:                client,
		compactInterval:       cfg.CompactInterval,
		compactIntervalJitter: cfg.CompactIntervalJitter,
		compactTimeout:        compactTimeout,
		compactMinRetain:      cfg.CompactMinRetain,
		compactBatchSize:      compactBatchSize,
		watchCh:               make(chan struct{}),
	}
	b.db = client.Database(dbName)
	b.coll = b.db.Collection("kine")
	b.collRevision = b.db.Collection("revision")

	// leaderElect = true: multiple kine instances can safely share the same MongoDB.
	return true, b, nil
}

// Start pings MongoDB, creates indexes, ensures a health-check key exists,
// and launches the TTL goroutine that expires keys with a lease.
func (b *MongoBackend) Start(ctx context.Context) error {
	if b.compactBatchSize < minCompactBatchSize {
		return fmt.Errorf("compact-batch-size %d too small: must be at least %d", b.compactBatchSize, minCompactBatchSize)
	}
	if err := b.client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("pinging MongoDB: %w", err)
	}
	if err := b.validateTransactionSupport(ctx); err != nil {
		return err
	}
	if err := b.ensureRevisionDocument(ctx); err != nil {
		return fmt.Errorf("ensuring MongoDB revision document: %w", err)
	}
	if err := setupIndexes(ctx, b.coll); err != nil {
		return fmt.Errorf("setting up MongoDB indexes: %w", err)
	}
	// Create the health key and immediately update it to advance to revision 2.
	// All kine backends start at revision 2 (the compiled apiserver conformance
	// tests rely on this). On subsequent restarts the key already exists, so only
	// the Create is skipped and the revision stays at its current (higher) value.
	if rev, err := b.Create(ctx, "/registry/health", nil, 0); err != nil {
		if !errors.Is(err, server.ErrKeyExists) {
			logrus.Warnf("mongodb: failed to create health check key: %v", err)
		}
	} else if _, _, _, err := b.Update(ctx, "/registry/health", []byte(`{"health":"true"}`), rev, 0); err != nil {
		logrus.Warnf("mongodb: failed to update health check key: %v", err)
	}
	if err := b.startCompactor(ctx); err != nil {
		return err
	}
	b.observeRevisionMetrics(ctx)
	b.observeCollectionMetrics(ctx)
	go b.ttl(ctx)
	// Disconnect the client when the context is cancelled so that connection pool
	// file descriptors are released promptly (important in test environments where
	// many backends are created and destroyed in the same process).
	go func() {
		<-ctx.Done()
		disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.client.Disconnect(disconnectCtx)
	}()
	logrus.Infof("MongoDB backend started (db=%s)", b.db.Name())
	return nil
}

func (b *MongoBackend) validateTransactionSupport(ctx context.Context) error {
	var hello struct {
		SetName                      string `bson:"setName"`
		Msg                          string `bson:"msg"`
		LogicalSessionTimeoutMinutes *int64 `bson:"logicalSessionTimeoutMinutes"`
	}
	if err := b.db.RunCommand(ctx, bson.M{"hello": 1}).Decode(&hello); err != nil {
		return fmt.Errorf("checking MongoDB transaction support: %w", err)
	}
	if hello.LogicalSessionTimeoutMinutes == nil {
		return errors.New("MongoDB backend requires logical sessions for transactions")
	}
	if hello.SetName == "" && hello.Msg != "isdbgrid" {
		return errors.New("MongoDB backend requires a replica set or sharded cluster because Kine revisions and events must commit transactionally")
	}
	return nil
}

// ensureRevisionDocument creates the singleton global revision counter document.
// The counter starts at 0 so the health Create and Update performed by Start
// leave a fresh backend at revision 2, matching the SQL backends.
func (b *MongoBackend) ensureRevisionDocument(ctx context.Context) error {
	err := b.collRevision.FindOne(ctx, bson.M{"_id": revisionDocumentID}).Err()
	if err == nil {
		return nil
	}
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return err
	}

	var legacy RevisionReg
	legacyErr := b.collRevision.FindOne(ctx, bson.M{}).Decode(&legacy)
	if legacyErr != nil && !errors.Is(legacyErr, mongo.ErrNoDocuments) {
		return legacyErr
	}
	revision := int64(0)
	compactRevision := int64(0)
	if legacyErr == nil {
		revision = legacy.Revision
		compactRevision = legacy.CompactRevision
	}

	_, err = b.collRevision.UpdateOne(
		ctx,
		bson.M{"_id": revisionDocumentID},
		bson.M{"$setOnInsert": bson.M{
			"revision":        revision,
			"compactRevision": compactRevision,
		}},
		options.UpdateOne().SetUpsert(true),
	)
	if mongo.IsDuplicateKeyError(err) {
		return nil
	}
	return err
}

func (b *MongoBackend) startCompactor(ctx context.Context) error {
	if b.compactIntervalJitter < 0 || b.compactIntervalJitter > 100 {
		return errors.New("compact-interval-jitter must be between 0 and 100")
	}
	if b.compactInterval <= 0 {
		logrus.Debugf("COMPACT disabled; automatic MongoDB compaction will not occur")
		return nil
	}

	maxJitter := float64(b.compactIntervalJitter) / 100.0 * float64(b.compactInterval)
	jitter := time.Duration(rand.Float64()*2*maxJitter - maxJitter)
	go b.compactor(ctx, b.compactInterval+jitter)
	return nil
}

// CurrentRevision returns the current global revision.
func (b *MongoBackend) CurrentRevision(ctx context.Context) (int64, error) {
	var rev RevisionReg
	err := b.collRevision.FindOne(ctx, bson.M{"_id": revisionDocumentID}).Decode(&rev)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 1, nil
	}
	if err != nil {
		return 0, err
	}
	return rev.Revision, nil
}

// nextRevision atomically increments the global revision counter and returns the new value.
func (b *MongoBackend) nextRevision(ctx context.Context) (int64, error) {
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)
	var rev RevisionReg
	err := b.collRevision.FindOneAndUpdate(
		ctx,
		bson.M{"_id": revisionDocumentID},
		bson.M{"$inc": bson.M{"revision": int64(1)}},
		opts,
	).Decode(&rev)
	if err != nil {
		return 0, err
	}
	return rev.Revision, nil
}

// getCompactRevision returns the last revision that has been compacted, or 0 if none.
func (b *MongoBackend) getCompactRevision(ctx context.Context) (int64, error) {
	var rev RevisionReg
	err := b.collRevision.FindOne(ctx, bson.M{"_id": revisionDocumentID}).Decode(&rev)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return rev.CompactRevision, nil
}

// setCompactRevision persists the compact revision so future Get/Watch calls can
// return ErrCompacted for historical reads at or below this point.
func (b *MongoBackend) setCompactRevision(ctx context.Context, revision int64) error {
	_, err := b.collRevision.UpdateOne(
		ctx,
		bson.M{"_id": revisionDocumentID},
		bson.M{"$set": bson.M{"compactRevision": revision}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

type updateResult struct {
	rev int64
	kv  *server.KeyValue
	ok  bool
}

func (b *MongoBackend) withTransaction(ctx context.Context, fn func(context.Context) (any, error)) (any, error) {
	session, err := b.client.StartSession()
	if err != nil {
		return nil, err
	}
	defer session.EndSession(ctx)

	return session.WithTransaction(
		ctx,
		fn,
		options.Transaction().
			SetReadConcern(readconcern.Snapshot()).
			SetWriteConcern(writeconcern.Majority()).
			SetReadPreference(readpref.Primary()),
	)
}

func (b *MongoBackend) currentKeyConflict(ctx context.Context, key string) (int64, *server.KeyValue, bool, error) {
	existing, err := b.findLatestForKey(ctx, key, 0, true)
	if err != nil {
		return 0, nil, false, err
	}
	if existing == nil || existing.Deleted != 0 {
		curRev, curErr := b.CurrentRevision(ctx)
		return curRev, nil, false, curErr
	}
	return existing.Revision, existing.toKeyValue(false), false, nil
}

func (b *MongoBackend) observeRevisionMetrics(ctx context.Context) {
	currentRev, err := b.CurrentRevision(ctx)
	if err != nil {
		logrus.Debugf("mongodb metrics: failed to read current revision: %v", err)
		return
	}
	compactRev, err := b.getCompactRevision(ctx)
	if err != nil {
		logrus.Debugf("mongodb metrics: failed to read compact revision: %v", err)
		return
	}
	metrics.MongoDBCurrentRevision.Set(float64(currentRev))
	metrics.MongoDBCompactRevision.Set(float64(compactRev))
	metrics.MongoDBCompactionGap.Set(float64(currentRev - compactRev))
}

func (b *MongoBackend) observeCollectionMetrics(ctx context.Context) {
	count, err := b.coll.EstimatedDocumentCount(ctx)
	if err != nil {
		logrus.Debugf("mongodb metrics: failed to estimate document count: %v", err)
		return
	}
	metrics.MongoDBDocuments.Set(float64(count))
}

// Get returns the most recent value for key at or before revision.
// It returns nil KV when the key does not exist or its latest event is a deletion.
// If revision is 0, the latest is returned. rangeEnd and limit are unused for single-key gets.
func (b *MongoBackend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	// Return ErrCompacted for historical reads before the compact point. Reads at
	// the compact revision itself remain valid, matching etcd storage semantics.
	if revision > 0 {
		compactRev, err := b.getCompactRevision(ctx)
		if err != nil {
			return 0, nil, err
		}
		if revision < compactRev {
			return revision, nil, server.ErrCompacted
		}
	}

	// Fetch the LATEST document for the key (including tombstones) so we can correctly
	// determine whether the key is live. Filtering on deleted=0 first would return
	// a stale live document even when a newer tombstone exists.
	filter := bson.M{"key": key}
	if revision > 0 {
		filter["revision"] = bson.M{"$lte": revision}
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "revision", Value: -1}})
	if keysOnly {
		opts.SetProjection(bson.M{"value": 0, "prevValue": 0})
	}

	var doc KineReg
	err := b.coll.FindOne(ctx, filter, opts).Decode(&doc)
	curRev, curRevErr := b.CurrentRevision(ctx)
	if errors.Is(err, mongo.ErrNoDocuments) || doc.Deleted != 0 {
		if curRevErr != nil {
			return 0, nil, curRevErr
		}
		return curRev, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}

	if revision > 0 {
		return revision, doc.toKeyValue(keysOnly), nil
	}
	if curRevErr != nil {
		return 0, nil, curRevErr
	}
	return curRev, doc.toKeyValue(keysOnly), nil
}

// Create inserts a new key. Returns ErrKeyExists if the key already exists and is not deleted.
// The read, revision allocation, and event insert happen in one MongoDB transaction.
// The unique (key, prevRevision) index provides the cross-process CAS guard for
// the three-Kine-replica production target.
func (b *MongoBackend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	result, err := b.withTransaction(ctx, func(txCtx context.Context) (any, error) {
		existing, err := b.findLatestForKey(txCtx, key, 0, true)
		if err != nil {
			return nil, err
		}
		if existing != nil && existing.Deleted == 0 {
			return nil, server.ErrKeyExists
		}

		prevKV := &server.KeyValue{}
		if existing != nil {
			prevKV = existing.toKeyValue(false)
		}

		rev, err := b.insertEvent(txCtx, &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:     key,
				Value:   value,
				Lease:   lease,
				Version: 1,
			},
			PrevKV: prevKV,
		})
		if err != nil {
			return nil, err
		}
		return rev, nil
	})
	if err != nil {
		if errors.Is(err, server.ErrKeyExists) || mongo.IsDuplicateKeyError(err) {
			curRev, _ := b.CurrentRevision(ctx)
			return curRev, server.ErrKeyExists
		}
		return 0, err
	}
	b.notifyWatchers()
	b.observeRevisionMetrics(ctx)
	rev, ok := result.(int64)
	if !ok {
		return 0, fmt.Errorf("unexpected MongoDB create transaction result type %T", result)
	}
	return rev, nil
}

// Update modifies the value of key at the exact given revision.
// Returns (rev, kv, false, nil) when the key has already been modified past revision.
func (b *MongoBackend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	result, err := b.withTransaction(ctx, func(txCtx context.Context) (any, error) {
		// Always fetch the LATEST version of the key with no revision upper bound.
		// Using revision as an upper bound would incorrectly find an old document that
		// still matches the requested revision even after subsequent updates.
		existing, err := b.findLatestForKey(txCtx, key, 0, true)
		if err != nil {
			return nil, err
		}
		if existing == nil || existing.Deleted != 0 {
			return updateResult{}, nil
		}
		// The revision parameter is an upper bound: the caller says "I expect the latest
		// revision of this key to be at or before revision". If the actual latest is
		// already past that point (concurrent write), the update is a conflict.
		if existing.Revision > revision {
			return updateResult{
				rev: existing.Revision,
				kv:  existing.toKeyValue(false),
				ok:  false,
			}, nil
		}

		newKV := &server.KeyValue{
			Key:            key,
			Value:          value,
			Lease:          lease,
			Version:        existing.Version + 1,
			CreateRevision: existing.CreateRevision,
		}
		rev, err := b.insertEvent(txCtx, &server.Event{
			KV:     newKV,
			PrevKV: existing.toKeyValue(false),
		})
		if err != nil {
			return nil, err
		}
		newKV.ModRevision = rev
		return updateResult{
			rev: rev,
			kv:  newKV,
			ok:  true,
		}, nil
	})
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return b.currentKeyConflict(ctx, key)
		}
		return 0, nil, false, err
	}
	out, ok := result.(updateResult)
	if !ok {
		return 0, nil, false, fmt.Errorf("unexpected MongoDB update transaction result type %T", result)
	}
	if out.ok {
		b.notifyWatchers()
		b.observeRevisionMetrics(ctx)
	}
	return out.rev, out.kv, out.ok, nil
}

// Delete marks key as deleted at the given revision.
// Returns (rev, kv, false, nil) when the key has been modified past revision.
func (b *MongoBackend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	result, err := b.withTransaction(ctx, func(txCtx context.Context) (any, error) {
		existing, err := b.findLatestForKey(txCtx, key, 0, true)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			// Key never existed — no-op (mirrors logstructured behaviour).
			curRev, _ := b.CurrentRevision(txCtx)
			return updateResult{rev: curRev, ok: true}, nil
		}
		if existing.Deleted == 1 {
			// Key was already deleted — return not-found so the server sends a failed
			// Txn response, which the Kubernetes storage layer maps to IsNotFound.
			curRev, _ := b.CurrentRevision(txCtx)
			return updateResult{rev: curRev, ok: false}, nil
		}
		if revision != 0 && existing.Revision != revision {
			return updateResult{
				rev: existing.Revision,
				kv:  existing.toKeyValue(false),
				ok:  false,
			}, nil
		}

		kv := existing.toKeyValue(false)
		rev, err := b.insertEvent(txCtx, &server.Event{
			Delete: true,
			KV:     kv,
			PrevKV: kv,
		})
		if err != nil {
			return nil, err
		}
		return updateResult{
			rev: rev,
			kv:  kv,
			ok:  true,
		}, nil
	})
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return b.currentKeyConflict(ctx, key)
		}
		return 0, nil, false, err
	}
	out, ok := result.(updateResult)
	if !ok {
		return 0, nil, false, fmt.Errorf("unexpected MongoDB delete transaction result type %T", result)
	}
	if out.ok && out.kv != nil {
		b.notifyWatchers()
		b.observeRevisionMetrics(ctx)
	}
	return out.rev, out.kv, out.ok, nil
}

// List returns the latest non-deleted version of all keys with the given prefix,
// at or before revision. startKey acts as a lower bound for key range queries.
func (b *MongoBackend) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	curRev, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}

	if revision > 0 {
		// Reject requests for future revisions.
		if revision > curRev {
			return curRev, nil, server.ErrFutureRev
		}
		// Reject requests for compacted revisions.
		compactRev, err := b.getCompactRevision(ctx)
		if err != nil {
			return 0, nil, err
		}
		if revision < compactRev {
			return revision, nil, server.ErrCompacted
		}
	}

	returnRev := curRev
	matchFilter := buildPrefixFilter(prefix, startKey)
	if revision > 0 {
		matchFilter["revision"] = bson.M{"$lte": revision}
		returnRev = revision
	}

	// Aggregate to get the latest revision per key (equivalent to DISTINCT ON in SQL).
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: matchFilter}},
		{{Key: "$sort", Value: bson.D{{Key: "key", Value: 1}, {Key: "revision", Value: -1}}}},
		{{Key: "$group", Value: bson.M{
			"_id": "$key",
			"doc": bson.M{"$first": "$$ROOT"},
		}}},
		{{Key: "$replaceRoot", Value: bson.M{"newRoot": "$doc"}}},
		{{Key: "$match", Value: bson.M{"deleted": int64(0)}}},
		{{Key: "$sort", Value: bson.D{{Key: "key", Value: 1}}}},
	}
	if limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: limit}})
	}
	if keysOnly {
		pipeline = append(pipeline, bson.D{{Key: "$project", Value: bson.M{"value": 0, "prevValue": 0}}})
	}

	cursor, err := b.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, nil, err
	}
	defer cursor.Close(ctx)

	var result []*server.KeyValue
	for cursor.Next(ctx) {
		var doc KineReg
		if err := cursor.Decode(&doc); err != nil {
			return 0, nil, err
		}
		result = append(result, doc.toKeyValue(keysOnly))
	}
	return returnRev, result, cursor.Err()
}

// Count returns the current revision and the number of live keys matching the given prefix,
// at or before revision. startKey acts as a lower bound for the key range.
func (b *MongoBackend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	if revision > 0 {
		curRev, err := b.CurrentRevision(ctx)
		if err != nil {
			return 0, 0, err
		}
		if revision > curRev {
			return curRev, 0, server.ErrFutureRev
		}
		compactRev, err := b.getCompactRevision(ctx)
		if err != nil {
			return 0, 0, err
		}
		if revision < compactRev {
			return revision, 0, server.ErrCompacted
		}
	}

	matchFilter := buildPrefixFilter(prefix, startKey)
	if revision > 0 {
		matchFilter["revision"] = bson.M{"$lte": revision}
	}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: matchFilter}},
		{{Key: "$sort", Value: bson.D{{Key: "revision", Value: -1}}}},
		{{Key: "$group", Value: bson.M{
			"_id":     "$key",
			"deleted": bson.M{"$first": "$deleted"},
		}}},
		{{Key: "$match", Value: bson.M{"deleted": int64(0)}}},
		{{Key: "$count", Value: "total"}},
	}

	cursor, err := b.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close(ctx)

	var countResult struct {
		Total int64 `bson:"total"`
	}
	if cursor.Next(ctx) {
		if err := cursor.Decode(&countResult); err != nil {
			return 0, 0, err
		}
	}

	curRev, err := b.CurrentRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	return curRev, countResult.Total, nil
}

// Watch returns a WatchResult that emits Events for keys matching the key prefix
// with revision greater than the given revision.
// Uses a polling approach: a goroutine waits for a broadcast signal from appendEvent,
// then queries for new documents since the last known revision.
func (b *MongoBackend) Watch(ctx context.Context, key string, revision int64) server.WatchResult {
	curRev, _ := b.CurrentRevision(ctx)
	compactRev, _ := b.getCompactRevision(ctx)

	// If the requested start revision has been compacted, signal the caller immediately.
	// Watch uses <= because the event log AT compactRev is gone; List/Get use < because
	// the data document AT compactRev is still physically present.
	if revision > 0 && revision <= compactRev {
		eventC := make(chan []*server.Event)
		close(eventC)
		return server.WatchResult{
			CurrentRevision: curRev,
			CompactRevision: compactRev,
			Events:          eventC,
		}
	}

	eventC := make(chan []*server.Event, 100)
	errC := make(chan error, 1)

	go func() {
		defer close(eventC)
		// When revision > 0 the caller wants events starting AT that revision (inclusive).
		// fetchEventsSince uses $gt (exclusive), so subtract 1 to convert to an exclusive
		// lower bound that yields the same inclusive result.
		lastRev := curRev
		if revision > 0 {
			lastRev = revision - 1
		}

		for {
			// Step 1: snapshot the notification channel BEFORE fetching.
			// Any write that occurs after this snapshot closes this channel,
			// ensuring we never miss a notification between the fetch and the wait.
			b.watchMu.RLock()
			notifyCh := b.watchCh
			b.watchMu.RUnlock()

			// Step 2: fetch events already in DB since lastRev (handles the case
			// where Watch is created after events have already been written).
			events, err := b.fetchEventsSince(ctx, key, lastRev)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					errC <- err
				}
				return
			}
			if len(events) > 0 {
				lastRev = events[len(events)-1].KV.ModRevision
				select {
				case eventC <- events:
				case <-ctx.Done():
					return
				}
				// More events may exist; loop immediately without waiting.
				continue
			}

			// Step 3: no new events — wait for the next write or cancellation.
			select {
			case <-ctx.Done():
				return
			case <-notifyCh:
			}
		}
	}()

	return server.WatchResult{
		CurrentRevision: curRev,
		Events:          eventC,
		Errorc:          errC,
	}
}

// DbSize returns the storage size of the kine collection in bytes.
func (b *MongoBackend) DbSize(ctx context.Context) (int64, error) {
	result := b.db.RunCommand(ctx, bson.M{"collStats": b.coll.Name()})
	var stats CollStats
	if err := result.Decode(&stats); err != nil {
		return 0, err
	}
	return stats.Size, nil
}

// Compact deletes superseded and tombstone documents up to the given revision.
// Work is split into bounded revision windows so a large backlog cannot become
// one unbounded aggregation/delete, which was the failure mode in the Postgres incident.
func (b *MongoBackend) Compact(ctx context.Context, revision int64) (int64, error) {
	resultLabel := metrics.ResultSuccess
	compactRev, err := b.getCompactRevision(ctx)
	if err != nil {
		metrics.CompactTotal.WithLabelValues(metrics.ResultError).Inc()
		return 0, err
	}

	for compactRev < revision {
		target := compactRev + b.compactBatchSize
		if target > revision {
			target = revision
		}

		compacted, _, err := b.compactBatch(ctx, compactRev, target)
		if err != nil {
			if errors.Is(err, server.ErrCompacted) {
				if compacted <= compactRev {
					break
				}
				compactRev = compacted
				continue
			}
			resultLabel = metrics.ResultError
			metrics.CompactTotal.WithLabelValues(resultLabel).Inc()
			b.observeRevisionMetrics(ctx)
			return 0, err
		}
		compactRev = compacted
	}

	metrics.CompactTotal.WithLabelValues(resultLabel).Inc()
	b.observeRevisionMetrics(ctx)
	b.observeCollectionMetrics(ctx)
	return b.CurrentRevision(ctx)
}

func (b *MongoBackend) compactor(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	compactRev, _ := b.getCompactRevision(ctx)
	targetCompactRev, _ := b.CurrentRevision(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		compactRev, targetCompactRev = b.compactIter(ctx, compactRev, targetCompactRev)
	}
}

func (b *MongoBackend) compactIter(ctx context.Context, compactRev, targetCompactRev int64) (int64, int64) {
	resultLabel := metrics.ResultSuccess
	iterCompactRev := compactRev
	compactedRev := compactRev
	currentRev := int64(0)
	iterStart := time.Now()
	iterCount := int64(0)
	var err error

	for iterCompactRev < targetCompactRev {
		iterCompactRev += b.compactBatchSize
		if iterCompactRev > targetCompactRev {
			iterCompactRev = targetCompactRev
		}

		compacted, current, cerr := b.compactBatch(ctx, compactedRev, iterCompactRev)
		if compacted != 0 && current != 0 {
			compactedRev = compacted
			currentRev = current
		}
		if cerr != nil {
			err = cerr
			break
		}
		iterCount++
	}

	if iterCount > 0 {
		logrus.Infof("COMPACT mongodb compacted from %d to %d in %d transactions over %s", compactRev, compactedRev, iterCount, time.Since(iterStart).Round(time.Millisecond))
	}

	if currentRev > 0 {
		compactRev = compactedRev
		targetCompactRev = currentRev
	}

	if err != nil && !errors.Is(err, server.ErrCompacted) {
		logrus.Errorf("MongoDB compact failed: %v", err)
		resultLabel = metrics.ResultError
	}
	metrics.CompactTotal.WithLabelValues(resultLabel).Inc()
	b.observeRevisionMetrics(ctx)
	b.observeCollectionMetrics(ctx)

	return compactRev, targetCompactRev
}

func (b *MongoBackend) compactBatch(ctx context.Context, compactRev, targetCompactRev int64) (int64, int64, error) {
	batchCtx := ctx
	cancel := func() {}
	if b.compactTimeout > 0 {
		batchCtx, cancel = context.WithTimeout(ctx, b.compactTimeout)
	}
	defer cancel()

	start := time.Now()
	result, err := b.withTransaction(batchCtx, func(txCtx context.Context) (any, error) {
		currentRev, err := b.CurrentRevision(txCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to get current revision: %w", err)
		}
		dbCompactRev, err := b.getCompactRevision(txCtx)
		if err != nil {
			return nil, fmt.Errorf("failed to get compact revision: %w", err)
		}
		if compactRev != dbCompactRev {
			logrus.Infof("COMPACT mongodb compact revision changed since last iteration: %d => %d", compactRev, dbCompactRev)
			return compactBatchResult{compacted: dbCompactRev, current: currentRev}, server.ErrCompacted
		}

		targetCompactRev = safeCompactRev(targetCompactRev, currentRev, b.compactMinRetain)
		if targetCompactRev <= compactRev {
			return compactBatchResult{compacted: dbCompactRev, current: currentRev}, server.ErrCompacted
		}

		logrus.Infof("COMPACT mongodb compactRev=%d targetCompactRev=%d currentRev=%d", compactRev, targetCompactRev, currentRev)

		deleted, err := b.compactDocuments(txCtx, compactRev, targetCompactRev)
		if err != nil {
			return nil, fmt.Errorf("failed to compact to revision %d: %w", targetCompactRev, err)
		}
		if err := b.setCompactRevision(txCtx, targetCompactRev); err != nil {
			return nil, fmt.Errorf("failed to record compact revision: %w", err)
		}
		return compactBatchResult{
			compacted: targetCompactRev,
			current:   currentRev,
			deleted:   deleted,
		}, nil
	})

	metrics.MongoDBCompactionBatchSeconds.Observe(time.Since(start).Seconds())
	if err != nil {
		if result != nil && errors.Is(err, server.ErrCompacted) {
			out, ok := result.(compactBatchResult)
			if !ok {
				return 0, 0, fmt.Errorf("unexpected MongoDB compact transaction result type %T", result)
			}
			return out.compacted, out.current, err
		}
		return 0, 0, err
	}
	out, ok := result.(compactBatchResult)
	if !ok {
		return 0, 0, fmt.Errorf("unexpected MongoDB compact transaction result type %T", result)
	}
	if out.deleted > 0 {
		metrics.MongoDBCompactionDeletedDocumentsTotal.Add(float64(out.deleted))
	}
	logrus.Infof("COMPACT mongodb deleted %d documents from %d revisions in %s - compacted to %d/%d", out.deleted, (out.compacted - compactRev), time.Since(start).Round(time.Millisecond), out.compacted, out.current)
	return out.compacted, out.current, nil
}

type compactBatchResult struct {
	compacted int64
	current   int64
	deleted   int64
}

func (b *MongoBackend) compactDocuments(ctx context.Context, compactRev, targetCompactRev int64) (int64, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{
			"revision": bson.M{
				"$gt":  compactRev,
				"$lte": targetCompactRev,
			},
			"prevRevision": bson.M{"$gt": int64(0)},
		}}},
		{{Key: "$group", Value: bson.M{
			"_id":           nil,
			"prevRevisions": bson.M{"$addToSet": "$prevRevision"},
		}}},
	}
	cursor, err := b.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	var agg struct {
		PrevRevisions []int64 `bson:"prevRevisions"`
	}
	if cursor.Next(ctx) {
		if err := cursor.Decode(&agg); err != nil {
			return 0, err
		}
	}
	if err := cursor.Err(); err != nil {
		return 0, err
	}

	deleted := int64(0)
	if len(agg.PrevRevisions) > 0 {
		result, err := b.coll.DeleteMany(ctx, bson.M{"revision": bson.M{"$in": agg.PrevRevisions}})
		if err != nil {
			return 0, err
		}
		deleted += result.DeletedCount
	}

	result, err := b.coll.DeleteMany(ctx, bson.M{
		"revision": bson.M{
			"$gt":  compactRev,
			"$lte": targetCompactRev,
		},
		"deleted": bson.M{"$gt": int64(0)},
	})
	if err != nil {
		return 0, err
	}
	deleted += result.DeletedCount
	return deleted, nil
}

func safeCompactRev(targetCompactRev int64, currentRev int64, compactMinRetain int64) int64 {
	safeRev := currentRev - compactMinRetain
	if targetCompactRev < safeRev {
		safeRev = targetCompactRev
	}
	if safeRev < 0 {
		safeRev = 0
	}
	return safeRev
}

// WaitForSyncTo blocks until the current global revision reaches the target.
func (b *MongoBackend) WaitForSyncTo(revision int64) {
	for {
		curRev, err := b.CurrentRevision(context.Background())
		if err == nil && curRev >= revision {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// appendEvent inserts a new event document and broadcasts to all active Watch goroutines.
func (b *MongoBackend) appendEvent(ctx context.Context, event *server.Event) (int64, error) {
	rev, err := b.insertEvent(ctx, event)
	if err != nil {
		return 0, err
	}
	b.notifyWatchers()
	b.observeRevisionMetrics(ctx)
	return rev, nil
}

func (b *MongoBackend) insertEvent(ctx context.Context, event *server.Event) (int64, error) {
	if event.KV == nil {
		event.KV = &server.KeyValue{}
	}
	if event.PrevKV == nil {
		event.PrevKV = &server.KeyValue{}
	}

	nextRev, err := b.nextRevision(ctx)
	if err != nil {
		return 0, err
	}

	doc := KineReg{
		Revision:       nextRev,
		Key:            event.KV.Key,
		Lease:          event.KV.Lease,
		Value:          event.KV.Value,
		PrevValue:      event.PrevKV.Value,
		PrevRevision:   event.PrevKV.ModRevision,
		Version:        event.KV.Version,
		CreateRevision: event.KV.CreateRevision,
	}
	if event.Create {
		doc.Created = 1
		doc.CreateRevision = nextRev
	}
	if event.Delete {
		doc.Deleted = 1
	}

	if _, err := b.coll.InsertOne(ctx, doc); err != nil {
		return 0, err
	}

	event.KV.ModRevision = nextRev
	return nextRev, nil
}

// notifyWatchers closes the current watchCh and replaces it with a new one,
// waking all goroutines that are select-waiting on the old channel.
func (b *MongoBackend) notifyWatchers() {
	b.watchMu.Lock()
	old := b.watchCh
	b.watchCh = make(chan struct{})
	b.watchMu.Unlock()
	close(old)
}

// fetchEventsSince returns all events for keys matching key (as prefix) with
// revision greater than lastRev, ordered by ascending revision.
func (b *MongoBackend) fetchEventsSince(ctx context.Context, key string, lastRev int64) ([]*server.Event, error) {
	filter := bson.M{"revision": bson.M{"$gt": lastRev}}
	if key != "" {
		lower, upper := prefixRange(key)
		keyFilter := bson.M{"$gte": lower}
		if upper != "" {
			keyFilter["$lt"] = upper
		}
		filter["key"] = keyFilter
	}

	opts := options.Find().SetSort(bson.D{{Key: "revision", Value: 1}})
	cursor, err := b.coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var events []*server.Event
	for cursor.Next(ctx) {
		var doc KineReg
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		event := &server.Event{
			Create: doc.Created == 1,
			Delete: doc.Deleted == 1,
			KV:     doc.toKeyValue(false),
		}
		if doc.PrevRevision > 0 {
			event.PrevKV = &server.KeyValue{
				Key:         doc.Key,
				Value:       doc.PrevValue,
				ModRevision: doc.PrevRevision,
			}
		}
		events = append(events, event)
	}
	return events, cursor.Err()
}

// findLatestForKey returns the most recent document for key with revision <= maxRevision.
// If maxRevision is 0, no upper bound is applied.
// If includeDeleted is false, deleted documents are excluded.
func (b *MongoBackend) findLatestForKey(ctx context.Context, key string, maxRevision int64, includeDeleted bool) (*KineReg, error) {
	filter := bson.M{"key": key}
	if maxRevision > 0 {
		filter["revision"] = bson.M{"$lte": maxRevision}
	}
	if !includeDeleted {
		filter["deleted"] = int64(0)
	}

	opts := options.FindOne().SetSort(bson.D{{Key: "revision", Value: -1}})
	var doc KineReg
	err := b.coll.FindOne(ctx, filter, opts).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

// ttl is a long-running goroutine that watches for keys with a Lease (TTL) and
// deletes them once the TTL has elapsed. It mirrors the logstructured TTL behaviour.
func (b *MongoBackend) ttl(ctx context.Context) {
	queue := workqueue.NewTypedDelayingQueue[string]()
	mu := &sync.RWMutex{}
	store := make(map[string]*ttlEventKV)
	eventCh := b.ttlEvents(ctx)

	go func() {
		for b.handleTTLEvent(ctx, mu, queue, store) {
		}
	}()

	for {
		select {
		case <-ctx.Done():
			queue.ShutDown()
			return
		case event, ok := <-eventCh:
			if !ok {
				queue.ShutDown()
				return
			}
			if event.Delete {
				continue
			}
			b.upsertTTLEntry(mu, store, queue, event.KV)
		}
	}
}

// ttlEvents returns a channel that first yields all currently live keys that
// have a Lease (from an initial list), then streams all subsequent watch events
// that carry a Lease.
func (b *MongoBackend) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)
	go func() {
		defer close(result)

		// Initial list: page through all keys starting with "/" that have a lease.
		curRev, err := b.CurrentRevision(ctx)
		if err != nil {
			logrus.Errorf("mongodb TTL: failed to get current revision: %v", err)
			return
		}
		startKey := ""
		for {
			_, kvs, err := b.List(ctx, "/", startKey, 1000, curRev, false)
			if err != nil {
				logrus.Errorf("mongodb TTL: initial list failed: %v", err)
				return
			}
			for _, kv := range kvs {
				if kv.Lease > 0 {
					select {
					case result <- &server.Event{KV: kv}:
					case <-ctx.Done():
						return
					}
				}
			}
			if len(kvs) < 1000 {
				break
			}
			startKey = kvs[len(kvs)-1].Key
		}

		// Continuous watch from curRev onwards.
		wr := b.Watch(ctx, "/", curRev)
		if wr.CompactRevision != 0 {
			logrus.Errorf("mongodb TTL: watch failed: compacted at %d", wr.CompactRevision)
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case events, ok := <-wr.Events:
				if !ok {
					return
				}
				for _, event := range events {
					if event.KV.Lease > 0 {
						select {
						case result <- event:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}
	}()
	return result
}

// handleTTLEvent processes one item from the TTL work queue.
func (b *MongoBackend) handleTTLEvent(ctx context.Context, mu *sync.RWMutex, queue workqueue.TypedDelayingInterface[string], store map[string]*ttlEventKV) bool {
	key, shutdown := queue.Get()
	if shutdown {
		return false
	}
	defer queue.Done(key)

	mu.RLock()
	entry := store[key]
	mu.RUnlock()
	if entry == nil {
		return true
	}

	if remaining := time.Until(entry.expiredAt); remaining > 0 {
		queue.AddAfter(key, remaining)
		return true
	}

	logrus.Tracef("mongodb TTL: deleting key=%s modRev=%d", entry.key, entry.modRevision)
	if _, _, _, err := b.Delete(ctx, entry.key, entry.modRevision); err != nil && !errors.Is(err, context.Canceled) {
		logrus.Errorf("mongodb TTL: delete failed for key=%s: %v, retrying", entry.key, err)
		queue.AddAfter(key, retryInterval)
		return true
	}

	mu.Lock()
	delete(store, key)
	mu.Unlock()
	return true
}

// upsertTTLEntry adds or updates the TTL entry for a key and enqueues the deletion.
func (b *MongoBackend) upsertTTLEntry(mu *sync.RWMutex, store map[string]*ttlEventKV, queue workqueue.TypedDelayingInterface[string], kv *server.KeyValue) {
	expires := time.Duration(kv.Lease) * time.Second
	expiredAt := time.Now().Add(expires)

	mu.Lock()
	existing := store[kv.Key]
	if existing != nil && existing.modRevision >= kv.ModRevision {
		mu.Unlock()
		return // already tracking a newer or same revision
	}
	store[kv.Key] = &ttlEventKV{
		key:         kv.Key,
		modRevision: kv.ModRevision,
		expiredAt:   expiredAt,
	}
	mu.Unlock()

	logrus.Tracef("mongodb TTL: scheduled key=%s modRev=%d in %v", kv.Key, kv.ModRevision, expires)
	queue.AddAfter(kv.Key, expires)
}

// setupIndexes creates the required indexes on the kine collection.
func setupIndexes(ctx context.Context, coll *mongo.Collection) error {
	models := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "key", Value: 1}},
			Options: options.Index().SetName("idx_key"),
		},
		{
			Keys:    bson.D{{Key: "key", Value: 1}, {Key: "revision", Value: -1}},
			Options: options.Index().SetName("idx_key_revision_desc"),
		},
		{
			Keys:    bson.D{{Key: "key", Value: 1}, {Key: "prevRevision", Value: 1}},
			Options: options.Index().SetName("idx_key_prev_revision_unique").SetUnique(true),
		},
		{
			Keys:    bson.D{{Key: "key", Value: 1}, {Key: "_id", Value: 1}},
			Options: options.Index().SetName("idx_key_id"),
		},
		{
			Keys:    bson.D{{Key: "_id", Value: 1}, {Key: "deleted", Value: 1}},
			Options: options.Index().SetName("idx_id_deleted"),
		},
		{
			Keys:    bson.D{{Key: "prevRevision", Value: 1}},
			Options: options.Index().SetName("idx_prev_revision"),
		},
		{
			Keys:    bson.D{{Key: "revision", Value: 1}, {Key: "prevRevision", Value: 1}},
			Options: options.Index().SetName("idx_revision_prev_revision"),
		},
		{
			Keys:    bson.D{{Key: "revision", Value: 1}, {Key: "deleted", Value: 1}},
			Options: options.Index().SetName("idx_revision_deleted"),
		},
		{
			Keys:    bson.D{{Key: "revision", Value: 1}},
			Options: options.Index().SetName("idx_revision_unique").SetUnique(true),
		},
	}
	_, err := coll.Indexes().CreateMany(ctx, models)
	return err
}

// buildPrefixFilter constructs a bson.M that matches all keys with the given prefix
// and with key >= startKey (for range queries / pagination).
// It uses a range query ($gte / $lt) instead of $regex so that MongoDB can perform
// an index range scan on idx_key rather than evaluating a regex against every entry.
func buildPrefixFilter(prefix, startKey string) bson.M {
	lower, upper := prefixRange(prefix)
	if startKey != "" && startKey > lower {
		lower = startKey
	}
	keyFilter := bson.M{"$gte": lower}
	if upper != "" {
		keyFilter["$lt"] = upper
	}
	return bson.M{"key": keyFilter}
}

// prefixRange returns the [lower, upper) bounds for a lexicographic prefix range.
// upper is an empty string when no upper bound is needed (prefix is empty or "/").
func prefixRange(prefix string) (lower, upper string) {
	if prefix == "" || prefix == "/" {
		return prefix, ""
	}
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return prefix, string(b[:i+1])
		}
	}
	// All bytes were 0xff — no finite upper bound exists.
	return prefix, ""
}
