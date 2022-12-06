package geddon

import (
	"bufio"
	"fmt"
	"github.com/hpcloud/tail"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBufferRead(t *testing.T) {
	var (
		err  error
		line string
		seek int64
	)
	file, _ := os.Open("/Users/klein/Desktop/logs-from-flink-main-container-in-sdwan-cpe-ping-test-taskmanager-1-1.log")
	buf := bufio.NewReader(file)
	file.Seek(0, io.SeekStart)
	for i := 0; err != io.EOF; i++ {
		line, err = buf.ReadString('\n')
		if err != nil && err != io.EOF {
			panic(errors.WithMessagef(err, "read file failed"))
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		seek, err = file.Seek(0, io.SeekCurrent)
		//发送数据
		fmt.Println(line)
		lastOffset := seek - int64(buf.Buffered())
		fmt.Println(lastOffset)
	}
}

func TestTail(t *testing.T) {
	tailFile, _ := tail.TailFile("/Users/klein/Desktop/logs-from-flink-main-container-in-sdwan-cpe-ping-test-taskmanager-1-1.log", tail.Config{
		Location: &tail.SeekInfo{
			Offset: 0,
			Whence: io.SeekStart,
		},
		ReOpen:      false,
		MustExist:   true,
		Poll:        false,
		Pipe:        false,
		RateLimiter: nil,
		Follow:      false,
		MaxLineSize: 0,
		Logger:      nil,
	})
	time.Sleep(1 * time.Second)
	for i := 0; i < 1000; i++ {
		<-tailFile.Lines
		fmt.Println(tailFile.Tell())
	}

}
