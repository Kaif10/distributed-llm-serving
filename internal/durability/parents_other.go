//go:build !linux

package durability

// Directory-entry power-loss durability is currently specified for Linux only.
// Other platforms retain file syncing and process-crash coverage, not that claim.
func SyncParents(string) error { return nil }
