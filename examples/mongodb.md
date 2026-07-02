# Using the MongoDB backend

[MongoDB](https://www.mongodb.com) is a document-oriented database that provides high availability, horizontal scaling,
and rich query capabilities.

The MongoDB backend stores each revision event (create, update, or delete) as a document in a `kine` collection and uses
a separate `revision` collection as an atomic counter for global ordering.

Key characteristics of this backend:

- Revision allocation and event insert are coupled in a multi-document transaction, so a failed write cannot leave a committed revision gap
- Atomic revision counter using MongoDB `$inc` operations
- Unique `(key, prevRevision)` index as the cross-instance compare-and-swap guard
- Polling-based watch mechanism (no Change Streams required)
- TTL support for leased keys
- Multiple kine instances can safely share the same MongoDB deployment (leader election enabled)
- **Requires a MongoDB replica set or sharded cluster.** The backend depends on
  multi-document transactions and refuses to start against a standalone `mongod`.
  A single-node replica set (`rs.initiate()`) is enough for local development.

## Configuring KINE

This is done by specifying the `--endpoint` option with the following format:

```
mongodb://[<user>:<password>@]<host>[:<port>][/<database>]
```

or using the DNS seed list format for replica sets and MongoDB Atlas:

```
mongodb+srv://[<user>:<password>@]<host>[/<database>]
```

The tokens are defined as follows:

- `user` / `password` - Optional credentials for authentication.
- `host` - Hostname or IP address of the MongoDB server. Default is `localhost`.
- `port` - Port of the MongoDB server. Default is `27017`.
- `database` - Name of the MongoDB database to use. Default is `kine`.

Any additional parameters supported by
the [MongoDB connection string](https://www.mongodb.com/docs/manual/reference/connection-string/) can be appended as
query parameters (e.g. `authSource`, `replicaSet`, `tls`).

### Examples

All examples assume the target is a replica set (a single-node replica set works for
local development; standalone servers are rejected at startup).

Connect to a local single-node replica set using the default database `kine`:

```
mongodb://localhost:27017/?replicaSet=rs0
```

Connect specifying a custom database name:

```
mongodb://localhost:27017/mydb?replicaSet=rs0
```

Connect with authentication:

```
mongodb://admin:secret@localhost:27017/kine?replicaSet=rs0
```

Connect to a MongoDB Atlas cluster using the DNS seed list format:

```
mongodb+srv://user:password@cluster0.example.mongodb.net/kine
```

Connect to a three-member replica set:

```
mongodb://mongo1:27017,mongo2:27017,mongo3:27017/kine?replicaSet=rs0
```

## Running kine standalone

Start kine pointing to a local single-node replica set:

```bash
kine --endpoint "mongodb://localhost:27017/kine?replicaSet=rs0"
```

With authentication:

```bash
kine --endpoint "mongodb://admin:secret@localhost:27017/kine?replicaSet=rs0"
```

With TLS:

```bash
kine --endpoint "mongodb://localhost:27017/kine?replicaSet=rs0" \
  --ca-file ca.crt \
  --cert-file client.crt \
  --key-file client.key
```

## Using with k3s

```bash
k3s server --datastore-endpoint "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/kine?replicaSet=rs0"
```

With TLS:

```bash
k3s server \
  --datastore-endpoint "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/kine?replicaSet=rs0" \
  --datastore-cafile ca.crt \
  --datastore-certfile client.crt \
  --datastore-keyfile client.key
```

## MongoDB schema

### Collection: `kine`

Each document represents one revision event for a key.

| Field            | Type     | Description                                 |
|------------------|----------|---------------------------------------------|
| `_id`            | ObjectID | MongoDB internal document ID                |
| `revision`       | int64    | Unique monotonic global revision            |
| `key`            | string   | Key path                                    |
| `created`        | int64    | `1` if this document is a create event      |
| `deleted`        | int64    | `1` if this document is a delete tombstone  |
| `createRevision` | int64    | Revision at which the key was first created |
| `prevRevision`   | int64    | Previous revision for this key              |
| `lease`          | int64    | Lease ID (used for TTL expiry)              |
| `value`          | bytes    | Value payload                               |
| `prevValue`      | bytes    | Previous value (populated on update events) |
| `version`        | int64    | Version counter for this key                |

### Collection: `revision`

Two singleton documents. They are separate documents on purpose: MongoDB write
conflicts are document-level, so keeping the compact marker off the write-hot
counter document prevents compaction transactions from conflicting with every
concurrent write.

| `_id`      | Field             | Description                                                 |
|------------|-------------------|-------------------------------------------------------------|
| `global`   | `revision`        | Current global revision (incremented atomically via `$inc`) |
| `compact`  | `compactRevision` | Last compacted revision                                     |

### Indexes on `kine`

| Index                          | Fields                    | Notes                                                       |
|--------------------------------|---------------------------|-------------------------------------------------------------|
| `idx_key_revision_desc`        | `key`, `revision` desc    | Latest revision per key (Get/List/Count)                    |
| `idx_key_prev_revision_unique` | `key`, `prevRevision`     | Unique — cross-instance compare-and-swap guard              |
| `idx_revision_unique`          | `revision`                | Unique — global ordering, watch catch-up, compaction delete |
| `idx_revision_prev_revision`   | `revision`, `prevRevision`| Compaction window aggregation                               |
| `idx_revision_deleted`         | `revision`, `deleted`     | Compaction tombstone deletes                                |

## Local development with Docker Compose and k3d

The file [docker-compose.mongodb.yml](docker-compose.mongodb.yml) provides a ready-to-use stack with MongoDB and kine
built from the local source. Combined with [k3d](https://k3d.io), you can run a full k3s cluster locally without
installing anything on the host.

### Prerequisites

- [Docker](https://docs.docker.com/engine/install/)
- [k3d](https://k3d.io/stable/#install-script)

```bash
curl -s https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
```

### Step 1 — Start MongoDB and kine

Run from the repository root:

```bash
docker compose -f examples/docker-compose.mongodb.yml up -d --build
```

Wait for kine to be ready:

```bash
docker compose -f examples/docker-compose.mongodb.yml logs -f kine
```

You should see kine listening on `0.0.0.0:2379`.

### Step 2 — Create the k3s cluster

k3d must join the same Docker network (`kine-net`) so that its containers can reach the `kine` service by name:

```bash
k3d cluster create dev \
  --network kine-net \
  --k3s-arg "--datastore-endpoint=http://kine:2379@server:*" \
  --k3s-arg "--disable=local-storage@server:*" \
  --no-lb
```

Verify the cluster is up:

```bash
kubectl get nodes
kubectl get pods -A
```

### Step 3 — Tear everything down

```bash
k3d cluster delete dev
docker compose -f examples/docker-compose.mongodb.yml down -v
```

The `-v` flag also removes the MongoDB data volume.

## Migrating from another backend

> **Warning:** the migration tool is experimental and has not been extensively tested across different cluster sizes,
> workloads, or failure scenarios. Use it at your own risk. Always take a full backup of your source database before
> proceeding.

kine does not support `etcdctl snapshot` (the etcd binary snapshot format is not implemented).

For PostgreSQL-to-MongoDB migrations, prefer the datastore-level tool at
[`postgres-to-mongodb/main.go`](postgres-to-mongodb/main.go). It preserves Kine revisions,
historical rows, tombstones, previous revisions, leases, values, old values, and the compact
revision marker.

The older API-level tool at [`migrate/main.go`](migrate/main.go) only copies the current
key/value state through the etcd API. It can be useful for experiments, but it reassigns
revisions and does not preserve history.

### Step 1 — Run both kine instances simultaneously

For the datastore-level migration, keep the target MongoDB online but do not start k3s/kine
against it yet. Stop API server writes before taking the final source backup and running the
copy:

```bash
# k3s
systemctl stop k3s

# or k3d
k3d cluster stop <cluster-name>
```

### Step 2 — Back up PostgreSQL

Always take a source backup first:

```bash
pg_dump "$POSTGRES_URL" -t kine > kine-before-mongodb-migration.sql
```

### Step 3 — Run the migration

```bash
go run ./examples/postgres-to-mongodb \
  -postgres "$POSTGRES_URL" \
  -mongodb "mongodb://localhost:27017/kine?replicaSet=rs0" \
  -dry-run

go run ./examples/postgres-to-mongodb \
  -postgres "$POSTGRES_URL" \
  -mongodb "mongodb://localhost:27017/kine?replicaSet=rs0"
```

Example output:

```
source rows: 155000
source non-compact rows: 154999
source max revision: 4188000
source compact revision: 4180000
target MongoDB database: kine
  154999 rows migrated
building MongoDB indexes...
migration complete: inserted 154999 MongoDB Kine documents
revision document: revision=4188000 compactRevision=4180000
```

The migration also builds all backend indexes (including the unique
`(key, prevRevision)` compare-and-swap index) before declaring success, so any
data problem surfaces before cutover instead of during the first k3s start.

### Step 4 — Switch k3s to the MongoDB kine

Update the k3s datastore endpoint to point to the new kine instance and restart:

```bash
# Edit /etc/systemd/system/k3s.service or pass the flag directly
k3s server --datastore-endpoint "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/kine?replicaSet=rs0"
```

### What is preserved

| Item                      | API-level `examples/migrate` | PostgreSQL-level `examples/postgres-to-mongodb` |
|---------------------------|------------------------------|-------------------------------------------------|
| Current value of each key | Yes                          | Yes                                             |
| Revision history          | No                           | Yes                                             |
| Delete tombstones         | No                           | Yes                                             |
| Previous revisions        | No                           | Yes                                             |
| Leases / TTLs             | No                           | Yes                                             |
| Compact revision marker   | No                           | Yes                                             |

PostgreSQL kine does not persist etcd's `Version` field as a dedicated column. The direct
migration reconstructs it by walking visible rows in revision order. If old rows were already
compacted before the migration, the reconstructed version is a lower bound; Kubernetes storage
concurrency uses `ModRevision`, which is preserved exactly.
