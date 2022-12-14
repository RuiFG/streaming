package log

import (
	"time"
)

// A Level is a logging priority. Higher levels are more important.
type Level int8

const (
	// DebugLevel logs are typically voluminous, and are usually disabled in
	// production.
	DebugLevel Level = iota - 1
	// InfoLevel is the default logging priority.
	InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual
	// human review.
	WarnLevel
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	ErrorLevel
	// DPanicLevel logs are particularly important errors. In development the
	// logger panics after writing the message.
	DPanicLevel
	// PanicLevel logs a message, then panics.
	PanicLevel
	// FatalLevel logs a message, then calls os.Exit(1).
	FatalLevel
)

// EntryCaller represents the caller of a logging function.
type EntryCaller struct {
	Defined  bool
	PC       uintptr
	File     string
	Line     int
	Function string
}

// A FieldType indicates which member of the Field union struct should be used
// and how it should be serialized.
type FieldType uint8

const (
	// UnknownType is the default field type. Attempting to add it to an encoder will panic.
	UnknownType FieldType = iota
	// ArrayMarshalerType indicates that the field carries an ArrayMarshaler.
	ArrayMarshalerType
	// ObjectMarshalerType indicates that the field carries an ObjectMarshaler.
	ObjectMarshalerType
	// BinaryType indicates that the field carries an opaque binary blob.
	BinaryType
	// BoolType indicates that the field carries a bool.
	BoolType
	// ByteStringType indicates that the field carries UTF-8 encoded bytes.
	ByteStringType
	// Complex128Type indicates that the field carries a complex128.
	Complex128Type
	// Complex64Type indicates that the field carries a complex128.
	Complex64Type
	// DurationType indicates that the field carries a time.Duration.
	DurationType
	// Float64Type indicates that the field carries a float64.
	Float64Type
	// Float32Type indicates that the field carries a float32.
	Float32Type
	// Int64Type indicates that the field carries an int64.
	Int64Type
	// Int32Type indicates that the field carries an int32.
	Int32Type
	// Int16Type indicates that the field carries an int16.
	Int16Type
	// Int8Type indicates that the field carries an int8.
	Int8Type
	// StringType indicates that the field carries a string.
	StringType
	// TimeType indicates that the field carries a time.Time that is
	// representable by a UnixNano() stored as an int64.
	TimeType
	// TimeFullType indicates that the field carries a time.Time stored as-is.
	TimeFullType
	// Uint64Type indicates that the field carries a uint64.
	Uint64Type
	// Uint32Type indicates that the field carries a uint32.
	Uint32Type
	// Uint16Type indicates that the field carries a uint16.
	Uint16Type
	// Uint8Type indicates that the field carries a uint8.
	Uint8Type
	// UintptrType indicates that the field carries a uintptr.
	UintptrType
	// ReflectType indicates that the field carries an interface{}, which should
	// be serialized using reflection.
	ReflectType
	// NamespaceType signals the beginning of an isolated namespace. All
	// subsequent fields should be added to the new namespace.
	NamespaceType
	// StringerType indicates that the field carries a fmt.Stringer.
	StringerType
	// ErrorType indicates that the field carries an error.
	ErrorType
	// SkipType indicates that the field is a no-op.
	SkipType

	// InlineMarshalerType indicates that the field carries an ObjectMarshaler
	// that should be inlined.
	InlineMarshalerType
)

type Field struct {
	Key       string
	Type      FieldType
	Integer   int64
	String    string
	Interface interface{}
}

type Entry struct {
	Level      Level
	Time       time.Time
	LoggerName string
	Message    string
	Caller     EntryCaller
	Stack      string
	Fields     []Field
}
