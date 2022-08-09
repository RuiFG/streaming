package log

type Options struct {
	//-------------------
	//Is it displayed in standard output and standard error
	stdOutput bool
	//AddOutput mode,the optional value is JsonOutputEncoder ConsoleOutputEncoder
	outPutEncoder OutputEncoder
	//Log level,the optional value is DebugLevel InfoLevel WarnLevel ErrorLevel FatalLevel PanicLevel
	level Level
	//Report callerEncoder
	callerEncoder CallerEncoder
	//Report levelEncoder
	levelEncoder LevelEncoder
	//Report Warn level stack trace
	stacktrace bool
	//time layout
	timeLayout string
	//init the named
	name string
}

func (o *Options) WithStacktrace(stacktrace bool) *Options {
	o.stacktrace = stacktrace
	return o
}

func (o *Options) WithTimeLayout(timeLayout string) *Options {
	o.timeLayout = timeLayout
	return o
}

func (o *Options) WithOutputEncoder(outputEncoder OutputEncoder) *Options {
	o.outPutEncoder = outputEncoder
	return o
}

func (o *Options) WithLevel(level Level) *Options {
	o.level = level
	return o
}

func (o *Options) WithCallerEncoder(callerEncoder CallerEncoder) *Options {
	o.callerEncoder = callerEncoder
	return o
}

func (o *Options) WithLevelEncoder(encoder LevelEncoder) *Options {
	o.levelEncoder = encoder
	return o
}

func (o *Options) WithNamed(name string) *Options {
	o.name = name
	return o
}

func DefaultOptions() *Options {
	return &Options{level: InfoLevel,
		timeLayout:    "02/Jan/2006:15:04:05 +0800",
		levelEncoder:  BracketLevelEncoder,
		outPutEncoder: JsonOutputEncoder, callerEncoder: nil, stdOutput: true}
}
