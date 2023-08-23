package main

import (
	"testing"
)

func TestStoreGetStoreValue(t *testing.T) {
	store := NewStore()

	if err := store.Set("key1", "value1"); err != nil {
		t.Fatal("failed to set key1")
	}

	if err := store.Set("key2", "value2"); err != nil {
		t.Fatal("failed to set key2")
	}

	if value, ok := store.Get("key1"); ok != true || value != "value1" {
		t.Fatalf("Expected %q got %q or value is missing", "value1", value)
	}
}

func TestStoreOverwrite(t *testing.T) {
	store := NewStore()

	store.Set("key1", "value1")
	if value, ok := store.Get("key1"); ok != true || value != "value1" {
		t.Fatalf("Expected %q got %q or value is missing", "value1", value)
	}

	store.Set("key1", "value2")
	if value, ok := store.Get("key1"); ok != true || value != "value2" {
		t.Fatalf("Expected %q got %q or value is missing", "value2", value)
	}
}

func TestStoreGetMissingValue(t *testing.T) {
	store := NewStore()

	store.Set("key1", "value1")
	if value, ok := store.Get("key1"); ok != true || value != "value1" {
		t.Fatalf("Expected %q got %q or value is missing", "value1", value)
	}

	if value, ok := store.Get("key2"); ok == true || value != "" {
		t.Fatal("Expectted to be missing")
	}
}

func TestStoreRemoveKey(t *testing.T) {
	store := NewStore()

	store.Set("key1", "value1")

	if value, ok := store.Get("key1"); ok != true || value != "value1" {
		t.Fatalf("Expected %q got %q or value is missing", "value1", value)
	}

	if err := store.Remove("key1"); err != nil {
		t.Fatal("Failed to remove key1")
	}
}
