// postgres-to-mongodb migrates an offline Kine PostgreSQL datastore into the
// MongoDB document layout used by Kine's MongoDB backend.
//
// This is intentionally a datastore-level migration, not an etcd API copy. The
// API copy in examples/migrate only preserves current key/value state and assigns
// new revisions. This tool preserves Kine revision ids, historical rows, delete
// tombstones, previous revisions, leases, values, old values, and the compact
// revision marker.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5"
	kinemongo "github.com/k3s-io/kine/pkg/drivers/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const revisionDocumentID = "global"

func main() {
	postgresURL := flag.String("postgres", "", "Source PostgreSQL connection URL for the database that contains the kine table")
	mongoURL := flag.String("mongodb", "", "Target MongoDB connection URL, including database name, for example mongodb://host:27017/kine?replicaSet=rs0")
	batchSize := flag.Int("batch", 1000, "MongoDB InsertMany batch size")
	dropTarget := flag.Bool("drop-target", false, "Drop target MongoDB kine/revision collections before migration")
	dryRun := flag.Bool("dry-run", false, "Validate source/target and print the migration plan without writing MongoDB")
	flag.Parse()

	if *postgresURL == "" || *mongoURL == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *batchSize <= 0 {
		log.Fatalf("batch must be positive")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, migrateConfig{
		postgresURL: *postgresURL,
		mongoURL:    *mongoURL,
		batchSize:   *batchSize,
		dropTarget:  *dropTarget,
		dryRun:      *dryRun,
	}); err != nil {
		log.Fatalf("migration failed: %v", err)
	}
}

type migrateConfig struct {
	postgresURL string
	mongoURL    string
	batchSize   int
	dropTarget  bool
	dryRun      bool
}

func run(ctx context.Context, cfg migrateConfig) error {
	pgConn, err := pgx.Connect(ctx, cfg.postgresURL)
	if err != nil {
		return fmt.Errorf("connecting to PostgreSQL: %w", err)
	}
	defer pgConn.Close(context.Background())

	summary, err := inspectPostgres(ctx, pgConn)
	if err != nil {
		return err
	}
	if summary.totalRows == 0 {
		return fmt.Errorf("source kine table is empty")
	}

	mongoClient, err := mongo.Connect(options.Client().ApplyURI(cfg.mongoURL))
	if err != nil {
		return fmt.Errorf("connecting to MongoDB: %w", err)
	}
	defer mongoClient.Disconnect(context.Background())
	if err := mongoClient.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("pinging MongoDB primary: %w", err)
	}

	dbName, err := databaseName(cfg.mongoURL)
	if err != nil {
		return err
	}
	db := mongoClient.Database(dbName)
	kineColl := db.Collection("kine")
	revColl := db.Collection("revision")

	if err := validateMongoTarget(ctx, db, kineColl, revColl, cfg.dropTarget); err != nil {
		return err
	}

	fmt.Printf("source rows: %d\n", summary.totalRows)
	fmt.Printf("source non-compact rows: %d\n", summary.migratableRows)
	fmt.Printf("source max revision: %d\n", summary.maxRevision)
	fmt.Printf("source compact revision: %d\n", summary.compactRevision)
	fmt.Printf("target MongoDB database: %s\n", dbName)
	if cfg.dryRun {
		fmt.Println("dry run complete; no MongoDB writes performed")
		return nil
	}

	inserted, err := copyRows(ctx, pgConn, kineColl, cfg.batchSize)
	if err != nil {
		return err
	}
	if inserted != summary.migratableRows {
		return fmt.Errorf("inserted %d rows, expected %d", inserted, summary.migratableRows)
	}

	if err := writeRevisionDocument(ctx, revColl, summary.maxRevision, summary.compactRevision); err != nil {
		return err
	}

	fmt.Printf("migration complete: inserted %d MongoDB Kine documents\n", inserted)
	fmt.Printf("revision document: revision=%d compactRevision=%d\n", summary.maxRevision, summary.compactRevision)
	return nil
}

type postgresSummary struct {
	totalRows       int64
	migratableRows  int64
	maxRevision     int64
	compactRevision int64
}

func inspectPostgres(ctx context.Context, conn *pgx.Conn) (postgresSummary, error) {
	var summary postgresSummary
	if err := conn.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE name <> 'compact_rev_key'),
		       COALESCE(max(id), 0),
		       COALESCE(max(prev_revision) FILTER (WHERE name = 'compact_rev_key'), 0)
		FROM kine
	`).Scan(&summary.totalRows, &summary.migratableRows, &summary.maxRevision, &summary.compactRevision); err != nil {
		return summary, fmt.Errorf("inspecting PostgreSQL kine table: %w", err)
	}
	return summary, nil
}

func validateMongoTarget(ctx context.Context, db *mongo.Database, kineColl, revColl *mongo.Collection, dropTarget bool) error {
	var hello struct {
		SetName                      string `bson:"setName"`
		Msg                          string `bson:"msg"`
		LogicalSessionTimeoutMinutes *int64 `bson:"logicalSessionTimeoutMinutes"`
	}
	if err := db.RunCommand(ctx, bson.M{"hello": 1}).Decode(&hello); err != nil {
		return fmt.Errorf("checking MongoDB target: %w", err)
	}
	if hello.LogicalSessionTimeoutMinutes == nil || (hello.SetName == "" && hello.Msg != "isdbgrid") {
		return fmt.Errorf("target MongoDB must be a replica set or sharded cluster with logical sessions")
	}

	if dropTarget {
		if err := kineColl.Drop(ctx); err != nil {
			return fmt.Errorf("dropping target kine collection: %w", err)
		}
		if err := revColl.Drop(ctx); err != nil {
			return fmt.Errorf("dropping target revision collection: %w", err)
		}
		return nil
	}

	kineCount, err := kineColl.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("checking target kine collection: %w", err)
	}
	revCount, err := revColl.EstimatedDocumentCount(ctx)
	if err != nil {
		return fmt.Errorf("checking target revision collection: %w", err)
	}
	if kineCount > 0 || revCount > 0 {
		return fmt.Errorf("target MongoDB is not empty (kine=%d revision=%d); use -drop-target only after taking backups", kineCount, revCount)
	}
	return nil
}

type postgresKineRow struct {
	ID             int64
	Name           string
	Created        int64
	Deleted        int64
	CreateRevision int64
	PrevRevision   int64
	Lease          int64
	Value          []byte
	OldValue       []byte
}

func copyRows(ctx context.Context, conn *pgx.Conn, coll *mongo.Collection, batchSize int) (int64, error) {
	rows, err := conn.Query(ctx, `
		SELECT id, name, created, deleted, create_revision, prev_revision, lease, value, old_value
		FROM kine
		WHERE name <> 'compact_rev_key'
		ORDER BY id ASC
	`)
	if err != nil {
		return 0, fmt.Errorf("querying PostgreSQL kine rows: %w", err)
	}
	defer rows.Close()

	state := newVersionState()
	batch := make([]any, 0, batchSize)
	inserted := int64(0)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if _, err := coll.InsertMany(ctx, batch, options.InsertMany().SetOrdered(true)); err != nil {
			return err
		}
		inserted += int64(len(batch))
		fmt.Printf("\r  %d rows migrated", inserted)
		batch = batch[:0]
		return nil
	}

	for rows.Next() {
		var row postgresKineRow
		if err := rows.Scan(&row.ID, &row.Name, &row.Created, &row.Deleted, &row.CreateRevision, &row.PrevRevision, &row.Lease, &row.Value, &row.OldValue); err != nil {
			return inserted, fmt.Errorf("scanning PostgreSQL kine row: %w", err)
		}
		doc := state.toMongoDocument(row)
		batch = append(batch, doc)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return inserted, fmt.Errorf("inserting MongoDB batch: %w", err)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return inserted, fmt.Errorf("reading PostgreSQL kine rows: %w", err)
	}
	if err := flush(); err != nil {
		return inserted, fmt.Errorf("inserting final MongoDB batch: %w", err)
	}
	fmt.Println()
	return inserted, nil
}

type keyVersion struct {
	version int64
}

type versionState struct {
	keys map[string]keyVersion
}

func newVersionState() *versionState {
	return &versionState{keys: map[string]keyVersion{}}
}

func (s *versionState) toMongoDocument(row postgresKineRow) kinemongo.KineReg {
	version := int64(1)
	if row.Created != 0 {
		version = 1
	} else if current, ok := s.keys[row.Name]; ok {
		version = current.version
		if row.Deleted == 0 {
			version++
		}
	}

	// PostgreSQL kine does not persist etcd's Version as a separate column. For
	// un-compacted history we reconstruct it by walking rows in revision order. If
	// older history was already compacted, this is the best available lower bound;
	// Kubernetes primarily relies on ModRevision for storage concurrency.
	s.keys[row.Name] = keyVersion{version: version}

	return kinemongo.KineReg{
		Revision:       row.ID,
		Key:            row.Name,
		Created:        row.Created,
		Deleted:        row.Deleted,
		CreateRevision: row.CreateRevision,
		PrevRevision:   row.PrevRevision,
		Lease:          row.Lease,
		Value:          row.Value,
		PrevValue:      row.OldValue,
		Version:        version,
	}
}

func writeRevisionDocument(ctx context.Context, coll *mongo.Collection, revision, compactRevision int64) error {
	_, err := coll.UpdateOne(
		ctx,
		bson.M{"_id": revisionDocumentID},
		bson.M{"$set": bson.M{"revision": revision, "compactRevision": compactRevision}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("writing MongoDB revision document: %w", err)
	}
	return nil
}

func databaseName(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parsing MongoDB URL: %w", err)
	}
	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return "", errors.New("MongoDB URL must include a target database name")
	}
	return dbName, nil
}
