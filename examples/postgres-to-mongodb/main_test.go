package main

import "testing"

func TestVersionStateToMongoDocument(t *testing.T) {
	state := newVersionState()
	rows := []postgresKineRow{
		{ID: 10, Name: "/registry/a", Created: 1, CreateRevision: 10, Value: []byte("v1")},
		{ID: 11, Name: "/registry/a", CreateRevision: 10, PrevRevision: 10, Value: []byte("v2"), OldValue: []byte("v1")},
		{ID: 12, Name: "/registry/a", Deleted: 1, CreateRevision: 10, PrevRevision: 11, OldValue: []byte("v2")},
		{ID: 13, Name: "/registry/a", Created: 1, CreateRevision: 13, PrevRevision: 12, Value: []byte("reborn")},
	}
	wantVersions := []int64{1, 2, 2, 1}
	for i, row := range rows {
		doc := state.toMongoDocument(row)
		if doc.Revision != row.ID {
			t.Fatalf("row %d revision: want %d got %d", i, row.ID, doc.Revision)
		}
		if doc.Key != row.Name {
			t.Fatalf("row %d key: want %s got %s", i, row.Name, doc.Key)
		}
		if doc.Version != wantVersions[i] {
			t.Fatalf("row %d version: want %d got %d", i, wantVersions[i], doc.Version)
		}
		if doc.PrevRevision != row.PrevRevision {
			t.Fatalf("row %d prev revision: want %d got %d", i, row.PrevRevision, doc.PrevRevision)
		}
	}
}

func TestDatabaseName(t *testing.T) {
	got, err := databaseName("mongodb://localhost:27017/kine?replicaSet=rs0")
	if err != nil {
		t.Fatal(err)
	}
	if got != "kine" {
		t.Fatalf("want kine got %s", got)
	}
	if _, err := databaseName("mongodb://localhost:27017/?replicaSet=rs0"); err == nil {
		t.Fatal("expected missing database error")
	}
}
