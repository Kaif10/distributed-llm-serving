package control

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
)

func (s *Store) Handler() http.Handler {
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, state Snapshot, err error) {
		if err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, ErrConflict) {
				status = http.StatusConflict
			}
			if errors.Is(err, ErrUnavailable) {
				status = http.StatusServiceUnavailable
			}
			http.Error(w, err.Error(), status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(state)
	}
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) { state, err := s.Read(); write(w, state, err) })
	mux.HandleFunc("PUT /config", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRecord)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var update Update
		if err := decoder.Decode(&update); err != nil {
			http.Error(w, "invalid update", 400)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "expected one JSON object", 400)
			return
		}
		state, err := s.Apply(update)
		write(w, state, err)
	})
	return mux
}
