package safe

import (
	"fmt"
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
