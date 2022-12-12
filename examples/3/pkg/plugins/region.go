package plugins

import (
	"3/pkg/format"
	"3/pkg/plugins/pb"
	"fmt"
	"github.com/RuiFG/streaming/streaming-operator/window"
	"github.com/golang/protobuf/proto"
	"strings"
)

var PVContentType = []string{
	"text/html", "text/asp", "text/plain",
}

type RegionPlugin struct{}

func (r *RegionPlugin) NeedCalculate(log *format.Log) bool {
	return true
}

func (r *RegionPlugin) ID(log *format.Log) string {
	return fmt.Sprintln(log.InternalExt.IsParent, log.InternalExt.MinutelyTime, log.Domain, log.InternalExt.Country, log.InternalExt.Isp,
		log.InternalExt.Province, log.InternalExt.IsIpv6,
		log.InternalExt.IsHttps, log.InternalExt.Asn, log.InternalExt.View)
}

func (r *RegionPlugin) NewStruct(log *format.Log) *pb.Region {
	return &pb.Region{
		Timestamp:    log.InternalExt.MinutelyTime,
		Domain:       log.Domain,
		Country:      log.InternalExt.Country,
		Isp:          log.InternalExt.Isp,
		Province:     log.InternalExt.Province,
		IsParent:     log.InternalExt.IsParent,
		IsHit:        log.InternalExt.IsHit,
		IsIpv6:       log.InternalExt.IsIpv6,
		IsHttps:      log.InternalExt.IsHttps,
		Asn:          log.InternalExt.Asn,
		View:         log.InternalExt.View,
		Requests:     map[int64]int64{},
		Traffic:      map[int64]int64{},
		TimeResponse: map[int64]int64{},
		PV:           map[int64]int64{},
	}
}

func (r *RegionPlugin) Calculate(log *format.Log, v *pb.Region) {
	v.Traffic[log.HttpCode] += log.BytesSent
	v.Requests[log.HttpCode]++
	v.TimeResponse[log.HttpCode] += log.ResponseTime
	if !log.InternalExt.IsParent && (log.HttpCode <= 299 && log.HttpCode > 0 || log.HttpCode == 304) {
		for _, contentType := range PVContentType {
			if strings.HasPrefix(log.ContentType, contentType) {
				v.PV[log.HttpCode]++
				break
			}
		}
	}
}

func (r *RegionPlugin) ToMessage(acc map[string]*pb.Region) proto.Message {
	result := make([]*pb.Region, len(acc))
	index := 0
	for _, v := range acc {
		result[index] = v
		index++
	}
	return &pb.RegionList{List: result}
}

func (r *RegionPlugin) PluginName() string {
	return "region"
}

func RegionAggregator() window.AggregatorFn[*format.Log, map[string]*pb.Region, *Output] {
	return &aggregator[*pb.Region]{Plugin: &RegionPlugin{}}
}
