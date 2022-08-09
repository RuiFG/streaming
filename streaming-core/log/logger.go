package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"sync"
)

var (
	one   *logger = &logger{zap.S()}
	mutex sync.Mutex
)

type logger struct {
	*zap.SugaredLogger
}

func Setup(options *Options) {
	mutex.Lock()
	defer mutex.Unlock()
	var (
		infoWriteSyncers []zapcore.WriteSyncer
		errWriteSyncers  []zapcore.WriteSyncer
		cores            []zapcore.Core
		opts             []zap.Option
		//infoHook, errHook io.Writer
		encoderConfig = zap.NewProductionEncoderConfig()
	)

	infoWriteSyncers = append(infoWriteSyncers, zapcore.AddSync(os.Stdout))
	errWriteSyncers = append(errWriteSyncers, zapcore.AddSync(os.Stderr))

	if options.callerEncoder != nil {
		opts = append(opts, zap.AddCaller())
		encoderConfig.EncodeCaller = zapcore.CallerEncoder(options.callerEncoder)
	}

	encoderConfig.EncodeLevel = zapcore.LevelEncoder(options.levelEncoder)
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(options.timeLayout)
	//fix #15
	encoderConfig.ConsoleSeparator = " "
	cores = []zapcore.Core{zapcore.NewCore(
		options.outPutEncoder(encoderConfig),
		zapcore.NewMultiWriteSyncer(infoWriteSyncers...),
		zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl >= zapcore.Level(options.level) && lvl < zapcore.WarnLevel
		}),
	), zapcore.NewCore(
		options.outPutEncoder(encoderConfig),
		zapcore.NewMultiWriteSyncer(errWriteSyncers...),
		zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return lvl >= zapcore.Level(options.level) && lvl >= zapcore.WarnLevel
		}),
	)}

	if options.stacktrace {
		opts = append(opts, zap.AddStacktrace(zapcore.WarnLevel))
	}
	zapSugarLogger := zap.New(zapcore.NewTee(cores...), opts...).Sugar()
	if options.name != "" {
		zapSugarLogger = zapSugarLogger.Named(options.name)

	}

	one = &logger{zapSugarLogger}
}

func Named(name string) Logger {
	named := one.SugaredLogger.Named(name)
	return &logger{named}
}
