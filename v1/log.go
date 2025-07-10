package fusestream

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func InitLogging(level zerolog.Level) {
	writer := zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.RFC3339Nano,
		PartsOrder: []string{
			zerolog.TimestampFieldName,
			zerolog.LevelFieldName,
			zerolog.CallerFieldName,
			zerolog.MessageFieldName,
		},
		FieldsExclude: []string{
			zerolog.ErrorStackFieldName,
		},
		FormatExtra: func(m map[string]interface{}, buffer *bytes.Buffer) error {
			s, ok := m["stack"]
			if ok {
				_, err := buffer.WriteString(s.(string))
				return err
			}
			return nil
		},
	}

	zerolog.ErrorStackMarshaler = func(err error) interface{} {
		return fmt.Sprintf("\n%+v", err)
	}

	log.Logger = zerolog.New(writer).
		Level(level).With().Timestamp().Caller().Stack().
		Logger()
}
