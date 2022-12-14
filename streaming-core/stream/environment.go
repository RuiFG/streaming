package stream

import (
	"github.com/RuiFG/streaming/streaming-core/common/safe"
	"github.com/RuiFG/streaming/streaming-core/common/status"
	"github.com/RuiFG/streaming/streaming-core/element/log"
	"github.com/RuiFG/streaming/streaming-core/element/metrics"
	"github.com/RuiFG/streaming/streaming-core/operator"
	"github.com/RuiFG/streaming/streaming-core/store"
	"github.com/RuiFG/streaming/streaming-core/task"
	"github.com/pkg/errors"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/multi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"sync"
	"time"
)

type environmentOptions struct {
	//periodic checkpoints, if set 0, will not be enabled
	EnablePeriodicCheckpoint time.Duration
	//checkpoint state storage directory
	CheckpointsDir string
	//number of checkpoint state saved in storage
	CheckpointsNumRetained int
	//the interval between two checkpoint
	MinPauseBetweenCheckpoints time.Duration
	//maximum number of checkpoint failures allowed
	TolerableCheckpointFailureNumber int
	//maximum number of concurrent checkpoints
	MaxConcurrentCheckpoints int
	//one checkpoint timeout
	CheckpointTimeout time.Duration
	//maximum number of event buffer by each operator
	BufferSize int

	MetricsOptions        tally.ScopeOptions
	MetricsReportInterval time.Duration

	LogCore    zapcore.Core
	LogOptions []zap.Option
}

type WithOptions func(options *environmentOptions) error

func WithPeriodicCheckpoint(interval time.Duration) WithOptions {
	return func(options *environmentOptions) error {
		options.EnablePeriodicCheckpoint = interval
		return nil
	}
}

func WithCheckpointDir(checkpointDir string) WithOptions {
	return func(options *environmentOptions) error {
		options.CheckpointsDir = checkpointDir
		return nil
	}

}

func WithCheckpointsNumRetained(checkpointsNumRetained int) WithOptions {
	return func(options *environmentOptions) error {
		options.CheckpointsNumRetained = checkpointsNumRetained
		return nil
	}
}

func WithMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints time.Duration) WithOptions {
	return func(options *environmentOptions) error {
		options.MinPauseBetweenCheckpoints = minPauseBetweenCheckpoints
		return nil
	}
}

func WithTolerableCheckpointFailureNumber(tolerableCheckpointFailureNumber int) WithOptions {
	return func(options *environmentOptions) error {
		options.TolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber
		return nil
	}
}

func WithMaxConcurrentCheckpoints(maxConcurrentCheckpoints int) WithOptions {
	return func(options *environmentOptions) error {
		options.MaxConcurrentCheckpoints = maxConcurrentCheckpoints
		return nil
	}
}

func WithCheckpointTimeout(checkpointTimeout time.Duration) WithOptions {
	return func(options *environmentOptions) error {
		options.CheckpointTimeout = checkpointTimeout
		return nil
	}
}

func WithBufferSize(bufferSize int) WithOptions {
	return func(options *environmentOptions) error {
		options.BufferSize = bufferSize
		return nil
	}
}

func WithMetrics(metricsOptions tally.ScopeOptions, reportInterval time.Duration) WithOptions {
	return func(options *environmentOptions) error {
		options.MetricsOptions = metricsOptions
		options.MetricsReportInterval = reportInterval
		return nil
	}
}

func WithLog(core zapcore.Core, logOptions ...zap.Option) WithOptions {
	return func(options *environmentOptions) error {
		options.LogCore = core
		options.LogOptions = logOptions
		return nil
	}
}

// Environment is stream environment, every stream application needs the support of the *Environment.
type Environment struct {
	name              string
	options           *environmentOptions
	status            *status.Status
	coordinator       *task.Coordinator
	barrierSignalChan chan task.Signal
	sourceInitFns     []sourceInitFn
	storeBackend      store.Backend
	allChainTasks     []*task.Task

	scope tally.Scope

	metricsCloser io.Closer
	metricsSource *metricsSource
	metricStream  *SourceStream[metrics.Metric]

	logger *zap.Logger

	logSource *logSource
	logStream *SourceStream[log.Entry]
}

func (e *Environment) addSourceInit(fn sourceInitFn) {
	e.sourceInitFns = append(e.sourceInitFns, fn)
}

func (e *Environment) MetricsStream() Stream[metrics.Metric] {
	if e.metricStream == nil {
		e.metricsSource = &metricsSource{}
		e.metricStream = &SourceStream[metrics.Metric]{
			OperatorStream: OperatorStream[metrics.Metric]{
				options: OperatorStreamOptions{
					Name: "metrics",
					Operator: operator.OneInputOperatorToNormal[any, metrics.Metric](&operator.SourceOperatorWrap[metrics.Metric]{
						Source: e.metricsSource,
					}),
				},
				env:                 e,
				downstreamInitFnMap: map[string]downstreamInitFn{},
				once:                &sync.Once{},
			},
		}
		e.addSourceInit(e.metricStream.Init)
	}
	return e.metricStream
}

func (e *Environment) LogStream() Stream[log.Entry] {
	if e.logStream == nil {
		e.logSource = &logSource{}
		e.logStream = &SourceStream[log.Entry]{
			OperatorStream: OperatorStream[log.Entry]{
				options: OperatorStreamOptions{
					Name: "log",
					Operator: operator.OneInputOperatorToNormal[any, log.Entry](&operator.SourceOperatorWrap[log.Entry]{
						Source: e.logSource,
					}),
				},
				env:                 e,
				downstreamInitFnMap: map[string]downstreamInitFn{},
				once:                &sync.Once{},
			},
		}
		e.addSourceInit(e.logStream.Init)
	}
	return e.logStream
}

func (e *Environment) initLog() {
	if e.logSource != nil {
		e.options.LogCore = zapcore.NewTee(e.options.LogCore, e.logSource)
	}
	e.logger = zap.New(e.options.LogCore, e.options.LogOptions...).Named(e.name)
}

func (e *Environment) initMetrics() {

	if e.options.MetricsOptions.Prefix != "" {
		e.options.MetricsOptions.Prefix = e.name + e.options.MetricsOptions.Separator + e.options.MetricsOptions.Prefix
	} else {
		e.options.MetricsOptions.Prefix = e.name
	}
	if e.metricsSource != nil {
		if e.options.MetricsOptions.Reporter != nil {
			e.options.MetricsOptions.Reporter = multi.NewMultiReporter(e.options.MetricsOptions.Reporter, e.metricsSource)
		}
		if e.options.MetricsOptions.CachedReporter != nil {
			e.options.MetricsOptions.CachedReporter = multi.NewMultiCachedReporter(e.options.MetricsOptions.CachedReporter, e.metricsSource)
		}
	}
	e.scope, e.metricsCloser = tally.NewRootScope(e.options.MetricsOptions, e.options.MetricsReportInterval)

}

func (e *Environment) Start() (err error) {
	var (
		rootTasks []*task.Task
	)
	if len(e.sourceInitFns) <= 0 {
		return errors.Errorf("metricsSource init fn should not be empty")
	}
	if e.status.CAS(status.Ready, status.Running) {
		e.initMetrics()
		e.initLog()
		//0. check stream graph and print

		//1. init all task
		if e.storeBackend, err = store.NewFSBackend(e.options.CheckpointsDir, e.options.CheckpointsNumRetained); err != nil {
			return errors.WithMessage(err, "failed to new fs store backend")
		}

		for _, initFn := range e.sourceInitFns {
			if rootTask, chainTasks, err := initFn(); err != nil {
				return errors.WithMessage(err, "failed to init task")
			} else {
				if rootTask != nil {
					rootTasks = append(rootTasks, rootTask)
				}
				if chainTasks != nil {
					e.allChainTasks = append(e.allChainTasks, chainTasks...)
				}
			}
		}
		//2. start coordinator
		e.coordinator = task.NewCoordinator(
			e.logger.Named("coordinator"),
			e.scope.SubScope("coordinator"),
			rootTasks, e.allChainTasks, e.storeBackend, e.barrierSignalChan,
			e.options.MaxConcurrentCheckpoints, e.options.MinPauseBetweenCheckpoints, e.options.CheckpointTimeout, e.options.TolerableCheckpointFailureNumber)
		e.coordinator.Activate()
		go func() {
			<-e.coordinator.Done()
			_ = e.Stop(false)
		}()
		//3. start all chain task
		for _, _task := range e.allChainTasks {
			go func(inn *task.Task) {
				if err := safe.Run(inn.Daemon); err != nil {
					e.logger.Error("failed to daemon.", zap.Error(err), zap.String("task", inn.Name()))
				}
				_ = e.Stop(false)
			}(_task)
		}
		//4. waiting task running
		for _, _task := range e.allChainTasks {
			for !_task.Running() {
				select {
				case <-e.coordinator.Done():
					e.logger.Error("the current application has been stopped, interrupting task")
					return errors.Errorf("unable to start the application")
				default:
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
		//5. start periodic checkpoint if enable
		if e.options.EnablePeriodicCheckpoint > 0 {
			e.startPeriodicCheckpoint()
		}
	}
	return nil
}

func (e *Environment) Stop(savepoint bool) error {
	if e.status.CAS(status.Running, status.Closed) {
		if e.coordinator != nil {
			e.coordinator.Deactivate(savepoint)
			<-e.coordinator.Done()
		}
		for _, _task := range e.allChainTasks {
			_task.Close()
		}
		_ = e.metricsCloser.Close()
		_ = e.logger.Sync()
		return e.storeBackend.Close()
	}
	return nil

}

func (e *Environment) Done() <-chan struct{} {
	return e.coordinator.Done()
}

func (e *Environment) startPeriodicCheckpoint() {
	go func() {
		ticker := time.NewTicker(e.options.EnablePeriodicCheckpoint)
		defer ticker.Stop()
		for true {
			select {
			case <-e.coordinator.Done():
				e.logger.Info("periodic checkpoint stopped")
				return
			case <-ticker.C:
				e.coordinator.TriggerCheckpoint()
			}
		}
	}()
}

func New(name string, withOptions ...WithOptions) (*Environment, error) {
	options := &environmentOptions{
		EnablePeriodicCheckpoint:         0,
		CheckpointsDir:                   ".",
		CheckpointsNumRetained:           2,
		MinPauseBetweenCheckpoints:       60 * time.Second,
		TolerableCheckpointFailureNumber: 5,
		MaxConcurrentCheckpoints:         2,
		CheckpointTimeout:                10 * time.Second,
		BufferSize:                       2048,
		MetricsOptions:                   tally.ScopeOptions{},
		MetricsReportInterval:            time.Minute,
		LogCore:                          zapcore.NewNopCore(),
		LogOptions:                       nil,
	}
	for _, fn := range withOptions {
		if err := fn(options); err != nil {
			return nil, err
		}
	}
	return &Environment{
		name:              name,
		options:           options,
		barrierSignalChan: make(chan task.Signal),
		status:            status.NewStatus(status.Ready),
	}, nil
}
