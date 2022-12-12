package viewDB

import (
	"context"
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/log"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"bt.baishancloud.com/log/bsip"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var m = new(sync.Map)

func S2Client(endpoint string) *s3.S3 {
	if i, ok := m.Load(endpoint); ok {
		return i.(*s3.S3)
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		os.Setenv("AWS_ACCESS_KEY_ID", "s2nyqlr0xuag76pf83ze")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", "ZkUcXoFII4eE2Upqhs3frR7uSDnjTaOKXjXfahES")
	}
	if os.Getenv("AWS_DEFAULT_REGION") == "" {
		os.Setenv("AWS_DEFAULT_REGION", "us-east-1")
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Endpoint: aws.String(endpoint),
		Region:   aws.String(os.Getenv("AWS_DEFAULT_REGION")),
	}))
	s := s3.New(sess)
	m.Store(endpoint, s)
	return s
}

func listAllIPBDX(endpoint, bucket, prefix string) ([]*s3.Object, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute/2)
	defer cancel()
	cli := S2Client(endpoint)
	resp, err := cli.ListObjectsV2WithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}
	var res []*s3.Object
	for _, v := range resp.Contents {
		if strings.HasSuffix(strings.ToLower(*v.Key), ".ipdbx") {
			res = append(res, v)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].LastModified.After(*res[j].LastModified)
	})
	return res, nil
}

func getLatestIPDB(endpoint, bucket, prefix, localPath string) (string, bool, error) {
	info, err := os.Stat(localPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return "", false, err
		}
	}
	list, err := listAllIPBDX(endpoint, bucket, prefix)
	if err != nil {
		return "", false, err
	}
	if len(list) == 0 {
		return "", false, fmt.Errorf("no file in %v with prefix %v", bucket, prefix)
	}
	if info != nil && list[0].LastModified.Before(info.ModTime()) && *list[0].Size == info.Size() {
		return "", false, nil
	}
	return *list[0].Key, true, nil
}

func getAndSave(endpoint, bucket, key, localPath string) error {
	cli := S2Client(endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	log.Global().Infof("downloading view ipdbx file: %v", key)
	resp, err := cli.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	f, err := os.OpenFile(localPath+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	log.Global().Infof("saving viewDB %v to %v", key, localPath)
	return os.Rename(localPath+".tmp", localPath)
}

func CheckAndUpate(endpoint, bucket, prefix, localPath string) (bool, error) {
	key, changed, err := getLatestIPDB(endpoint, bucket, prefix, localPath)
	if err != nil {
		return false, err
	}
	if !changed {
		return false, nil
	}
	err = getAndSave(endpoint, bucket, key, localPath)
	if err != nil {
		return false, err
	}
	return true, nil
}

func InitViewDB(endpoint, bucket, prefix, localPath string, checkUpdateDelay time.Duration) (func() *bsip.IPLib, error) {
	lock, done, firstUpdate := new(sync.RWMutex), make(chan struct{}), true
	var l *bsip.IPLib
	l, err := bsip.NewIPLib(localPath)
	if err != nil {
		log.Global().Warnw(fmt.Sprintf("failed to load local viewDB %v", localPath), "err", err)
	} else {
		close(done)
		l.UseLanguage("EN")
	}
	delay := func() time.Duration {
		if l == nil {
			return time.Duration(rand.Int31n(30)) * time.Second
		}
		if firstUpdate {
			firstUpdate = false
			return time.Duration(rand.Int63n(int64(time.Minute * 5)))
		}
		return checkUpdateDelay
	}
	go func() {
		for time.Sleep(delay()); ; time.Sleep(delay()) {
			changed, err := CheckAndUpate(endpoint, bucket, prefix, localPath)
			if err != nil {
				log.Global().Errorw("failed to check&update view ipdb", "err", err)
				continue
			}
			if !changed {
				log.Global().Info("view ipdb unchanged")
				continue
			}
			log.Global().Info("view ipdb updated")
			lib, err := bsip.NewIPLib(localPath)
			if err != nil {
				log.Global().Errorw("failed to load view ipdb", "err", err)
				continue
			}
			lib.UseLanguage("EN")
			lock.Lock()
			if l == nil {
				l = lib
				close(done)
			} else {
				l = lib
			}
			lock.Unlock()
		}
	}()
	<-done
	return func() *bsip.IPLib {
		lock.RLock()
		defer lock.RUnlock()
		return l
	}, nil
}
