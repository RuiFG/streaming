package safe

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"runtime/debug"
)

//be safe, don't panic

func Run(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			_, _ = fmt.Fprintf(os.Stderr, "panic: %#v\n", r)
			debug.PrintStack()
			switch x := r.(type) {
			case string:
				err = fmt.Errorf(x)
			case error:
				err = x
			default:
				err = fmt.Errorf("%#v", x)
			}
		}
	}()
	err = fn()
	return err
}

func Go(fn func() error) chan error {
	c := make(chan error)
	go func() {
		err := Run(fn)
		c <- err
		close(c)
	}()
	return c
}

func GoChannel(fn func() error, errorChan chan<- error) {
	go func() {
		if err := Run(fn); err != nil {
			errorChan <- err
		}
	}()
}

func GoChannelWithMessage(fn func() error, message string, errorChan chan<- error) {
	go func() {
		if err := Run(fn); err != nil {
			errorChan <- errors.WithMessage(err, message)
		}
	}()
}
