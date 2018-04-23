package instrumentedsql

type nullInstrumenter struct {
}

func (n nullInstrumenter) End() error {
	return nil
}
