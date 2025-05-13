package slowio

import (
	"encoding/json"

	"github.com/rs/zerolog/log"
)

func mustJsonMarshal(v any) string {
	m, err := json.Marshal(v)
	if err != nil {
		log.Panic().Err(err).Msg("Marshal failed")
	}
	return string(m)
}
