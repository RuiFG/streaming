package stream

import (
	"github.com/RuiFG/streaming/streaming-core/element"
	"github.com/RuiFG/streaming/streaming-core/element/log"
	. "github.com/RuiFG/streaming/streaming-core/operator"
	"go.uber.org/zap/zapcore"
)

type core struct {
	fields    []zapcore.Field
	collector element.Collector[log.Entry]
}

func (l *core) Enabled(level zapcore.Level) bool {
	return true
}

func (l *core) With(fields []zapcore.Field) zapcore.Core {
	return &core{
		fields:    append(l.fields, fields...),
		collector: l.collector,
	}
}

func (l *core) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(entry, l)
}

func (l *core) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	l.collector.EmitEvent(&element.Event[log.Entry]{Value: l.ToLog(entry, fields)})
	return nil
}

func (l *core) ToLog(entry zapcore.Entry, fields []zapcore.Field) log.Entry {
	var logFields []log.Field
	for _, field := range fields {
		logFields = append(logFields, log.Field{
			Key:       field.Key,
			Type:      log.FieldType(field.Type),
			Integer:   field.Integer,
			String:    field.String,
			Interface: field.Interface,
		})
	}
	return log.Entry{
		Level:      log.Level(entry.Level),
		Time:       entry.Time,
		LoggerName: entry.LoggerName,
		Message:    entry.Message,
		Caller: log.EntryCaller{
			Defined:  entry.Caller.Defined,
			PC:       entry.Caller.PC,
			File:     entry.Caller.File,
			Line:     entry.Caller.Line,
			Function: entry.Caller.Function,
		},
		Stack:  entry.Stack,
		Fields: logFields,
	}
}

func (l *core) Sync() error {
	return nil
}

type logSource struct {
	BaseOperator[any, any, log.Entry]
	core
	done chan struct{}
}

func (s *logSource) Open(ctx Context, collector element.Collector[log.Entry]) error {
	if err := s.BaseOperator.Open(ctx, collector); err != nil {
		return err
	}
	s.done = make(chan struct{})
	s.collector = collector
	return nil
}

func (s *logSource) Close() error {
	close(s.done)
	return s.BaseOperator.Close()
}

func (s *logSource) Run() {
	<-s.done
}
