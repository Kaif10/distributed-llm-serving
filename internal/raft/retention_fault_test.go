package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

// Application bytes encode the applied entry sequence for this core model. The
// real configuration codec/API is exercised by separate native-process tests.
func TestSeededRetentionSnapshotSafetyAcrossFaults(t *testing.T) {
	for seed := int64(1); seed <= 25; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ids := []string{"a", "b", "c"}
			nodes := map[string]*Node{}
			stores := map[string]*RetainedStorage{}
			dirs := map[string]string{}
			histories := map[string][]Entry{}
			rng := rand.New(rand.NewSource(seed))
			committed := map[uint64]Entry{}
			leaders := map[uint64]string{}
			retentions, installs := 0, 0
			for i, id := range ids {
				dirs[id] = t.TempDir()
				s, err := OpenRetainedStorage(dirs[id], RetainedIdentity{Group: "seeded", Member: id})
				if err != nil {
					t.Fatal(err)
				}
				stores[id] = s
				defer func() { stores[id].Close() }()
				nodes[id] = replica(t, id, ids, s, seed*10+int64(i)+1, 4)
			}
			check := func() {
				for _, id := range ids {
					n := nodes[id]
					if n.role == Leader {
						if other := leaders[n.hard.Term]; other != "" && other != id {
							t.Fatal("two leaders in one term")
						}
						leaders[n.hard.Term] = id
					}
					if n.BaseIndex() > uint64(len(histories[id])) {
						var image []Entry
						if err := json.Unmarshal(n.Snapshot().Data, &image); err != nil {
							t.Fatal(err)
						}
						if uint64(len(image)) != n.BaseIndex() {
							t.Fatal("incomplete application image")
						}
						for i, old := range histories[id] {
							if old.Term != image[i].Term || old.Noop != image[i].Noop || !bytes.Equal(old.Data, image[i].Data) {
								t.Fatal("snapshot overwrote applied history")
							}
						}
						histories[id] = image
					}
					entries, err := n.DrainCommitted()
					if err != nil {
						t.Fatal(err)
					}
					for _, e := range entries {
						if e.Index != uint64(len(histories[id])+1) {
							t.Fatal("noncontiguous delivery")
						}
						histories[id] = append(histories[id], e)
					}
					for _, e := range histories[id] {
						if old, ok := committed[e.Index]; ok && (old.Term != e.Term || old.Noop != e.Noop || !bytes.Equal(old.Data, e.Data)) {
							t.Fatal("conflicting applied index", e.Index)
						}
						committed[e.Index] = e
					}
					if n.commit-n.BaseIndex() >= 4 {
						data, err := json.Marshal(histories[id])
						if err != nil {
							t.Fatal(err)
						}
						term, _ := n.TermAt(n.commit)
						if err := n.RetainLocal(RetainedSnapshot{Index: n.commit, Term: term, Data: data}); err != nil {
							t.Fatal(err)
						}
						retentions++
					}
				}
			}
			var queue []Message
			pump := func(offline string, random bool) {
				for delivery := 0; delivery < 150 && len(queue) > 0; delivery++ {
					i := 0
					if random {
						i = rng.Intn(len(queue))
					}
					m := queue[i]
					queue = append(queue[:i], queue[i+1:]...)
					if m.To == offline || m.From == offline {
						continue
					}
					if random && rng.Intn(5) == 0 {
						continue
					}
					if random && rng.Intn(8) == 0 {
						queue = append(queue, m)
					}
					out, err := nodes[m.To].Step(m)
					if err != nil {
						t.Fatal(err)
					}
					queue = append(queue, out...)
					if m.Type == InstallSnapshot {
						installs++
					}
					check()
				}
			}
			queue = append(queue, campaign(t, nodes["a"])...)
			pump("c", false)
			for i := 0; i < 12; i++ {
				out, err := nodes["a"].Propose([]byte(fmt.Sprint("warm-", i)))
				if err != nil {
					t.Fatal(err)
				}
				queue = append(queue, out...)
				pump("c", false)
			}
			queue = append(queue, nodes["a"].heartbeats()...)
			pump("", false)
			for tick := 0; tick < 160; tick++ {
				if tick < 90 && rng.Intn(12) == 0 {
					id := ids[rng.Intn(len(ids))]
					if err := stores[id].Close(); err != nil {
						t.Fatal(err)
					}
					s, err := OpenRetainedStorage(dirs[id], RetainedIdentity{Group: "seeded", Member: id})
					if err != nil {
						t.Fatal(err)
					}
					stores[id] = s
					nodes[id] = replica(t, id, ids, s, seed*1000+int64(tick)+1, 4)
					histories[id] = nil
				}
				for _, id := range ids {
					out, err := nodes[id].Tick()
					if err != nil {
						t.Fatal(err)
					}
					queue = append(queue, out...)
					check()
					if nodes[id].role == Leader && tick%7 == 0 {
						out, err := nodes[id].Propose([]byte(fmt.Sprint(seed, "/", tick, "/", id)))
						if err != nil {
							t.Fatal(err)
						}
						queue = append(queue, out...)
						check()
					}
				}
				offline := ""
				if tick < 90 {
					offline = "c"
				}
				pump(offline, tick < 90)
			}
			count := 0
			for _, id := range ids {
				if nodes[id].role == Leader {
					count++
				}
			}
			if count != 1 || len(committed) < 15 || retentions == 0 || installs == 0 {
				t.Fatal("healed schedule lacked progress/retention/install", count, len(committed), retentions, installs)
			}
		})
	}
}
