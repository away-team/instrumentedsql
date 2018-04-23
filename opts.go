package instrumentedsql

import (
	"github.com/healthimation/go-service/database"
)

// Opt is a functional option type for the wrapped driver
type Opt func(*wrappedDriver)

// WithLogger sets the logger of the wrapped driver to the provided logger
func WithLogger(l Logger) Opt {
	return func(w *wrappedDriver) {
		w.logger = l
	}
}

// WithTracer sets the tracer of the wrapped driver to the provided tracer
func WithInstrumenter(t database.DBInstrumentTimer) Opt {
	return func(w *wrappedDriver) {
		w.instrumenter = t
	}
}
