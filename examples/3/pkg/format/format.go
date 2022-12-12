package format

import (
	"3/pkg/format/viewDB"
	"bt.baishancloud.com/log/bsip"
	"bytes"
	"fmt"
	"github.com/RuiFG/streaming/streaming-core/log"
	"github.com/pkg/errors"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var IPLib *bsip.IPLib
var ASNLib *bsip.IPLib
var ViewLibFn func() *bsip.IPLib

type Log struct {
	Raw string

	ClientIp          string
	Domain            string
	ContentType       string
	RequestTime       string //raw
	RequestMethod     string
	RequestUrl        string
	RequestVersion    string
	HttpCode          int64
	BytesSent         int64
	Referer           string
	UserAgent         string
	ResponseTime      int64
	BodyBytesSent     int64
	ContentLength     int64
	Range             string
	XForwardedFor     string
	Split1            []string
	Split2            []string
	XPeer             string
	DispatchDeleted   string
	InternalHitStatus string
	HierarchyStatus   string
	Ext               []string

	InternalExt InternalExt
}

type InternalExt struct {
	MinutelyTime int64
	NetIP        net.IP
	ParsedTime   time.Time
	ParsedUrl    *url.URL

	IsParent bool
	IsHit    bool
	IsHttps  bool
	IsIpv6   bool

	//region
	Country  string
	Isp      string
	Province string

	Asn  int64
	View string
}

func extractString(value string, lQuote, rQuote bool) (string, error) {
	if len(value) <= 0 {
		return "", fmt.Errorf("raw length <=0")
	}
	leftIndex := 0
	rightIndex := len(value)
	if lQuote {
		leftIndex += 1
	}
	if rQuote {
		rightIndex -= 1
	}
	if rightIndex-leftIndex > len(value) {
		return "", fmt.Errorf("index out of bounds, left:%d right:%d raw:%s", leftIndex, rightIndex, value)
	}
	return value[leftIndex:rightIndex], nil
}

func extractInt64(value string, lQuote, rQuote bool) (int64, error) {
	if result, err := extractString(value, lQuote, rQuote); err != nil {
		return 0, err
	} else {
		return strconv.ParseInt(result, 10, 64)
	}
}

func extractStringSlice(value string, lQuote, rQuote bool, sep string) ([]string, error) {
	if v, err := extractString(value, lQuote, rQuote); err != nil {
		return nil, err
	} else {
		return strings.Split(v, sep), nil
	}
}

func Format(raw string) (*Log, error) {
	raw = strings.TrimSpace(raw)
	tmp := strings.Split(raw, " ")

	if len(tmp) != 24 {
		return nil, errors.New("illegal field count")
	}
	var (
		formatLog = &Log{Raw: raw}
		err       error
	)

	if formatLog.ClientIp, err = extractString(tmp[0], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal NetIP")
	}
	if formatLog.Domain, err = extractString(tmp[1], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal Domain")
	}
	if formatLog.ContentType, err = extractString(tmp[2], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal ContentType")
	}
	if formatLog.RequestTime, err = extractString(tmp[3]+" "+tmp[4], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal ParsedTime")
	}
	if formatLog.RequestMethod, err = extractString(tmp[5], true, false); err != nil {
		return nil, errors.WithMessage(err, "illegal RequestMethod")
	}
	if formatLog.RequestUrl, err = extractString(tmp[6], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal ParsedUrl")
	}
	if formatLog.RequestVersion, err = extractString(tmp[7], false, true); err != nil {
		return nil, errors.WithMessage(err, "illegal RequestVersion")
	}
	if formatLog.HttpCode, err = extractInt64(tmp[8], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal HttpCode")
	}
	if formatLog.BytesSent, err = extractInt64(tmp[9], false, false); err != nil {
		return nil, errors.New("illegal byte sent format: " + tmp[8])
	}
	if formatLog.Referer, err = extractString(tmp[10], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal Referer")
	}
	if formatLog.UserAgent, err = extractString(tmp[11], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal UserAgent")
	}
	if formatLog.ResponseTime, err = extractInt64(tmp[12], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal ResponseTime")
	}
	if formatLog.BodyBytesSent, err = extractInt64(tmp[13], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal BodyBytesSent")
	}
	if formatLog.ContentLength, err = extractInt64(tmp[14], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal ContentLength")
	}
	if formatLog.Range, err = extractString(tmp[15], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal Range")
	}
	if formatLog.XForwardedFor, err = extractString(tmp[16], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal XForwardedFor")
	}
	if formatLog.Split1, err = extractStringSlice(tmp[17], false, false, "@"); err != nil {
		return nil, errors.WithMessage(err, "illegal Split1")
	}
	if formatLog.Split2, err = extractStringSlice(tmp[18], false, false, "@"); err != nil {
		return nil, errors.WithMessage(err, "illegal Split2")
	}
	if formatLog.XPeer, err = extractString(tmp[19], true, true); err != nil {
		return nil, errors.WithMessage(err, "illegal XPeer")
	}
	if formatLog.DispatchDeleted, err = extractString(tmp[20], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal DispatchDeleted")
	}
	if formatLog.InternalHitStatus, err = extractString(tmp[21], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal InternalHitStatus")
	}
	if formatLog.HierarchyStatus, err = extractString(tmp[22], false, false); err != nil {
		return nil, errors.WithMessage(err, "illegal HierarchyStatus")
	}
	if formatLog.Ext, err = extractStringSlice(tmp[23], false, false, "@_@"); err != nil {
		return nil, errors.WithMessage(err, "illegal Ext")
	}

	//InternalExt

	formatLog.InternalExt.NetIP = net.ParseIP(formatLog.ClientIp)
	if formatLog.InternalExt.NetIP == nil {
		return nil, errors.New("illegal ip address: " + formatLog.ClientIp)
	}

	if useServerIp(formatLog.InternalExt.NetIP) {
		if len(formatLog.Split1) >= 3 {
			server := strings.Split(formatLog.Split1[1], ":")
			if len(server) >= 2 {
				port := server[len(server)-1]
				serverIp := formatLog.Split1[1][0 : len(formatLog.Split1[1])-len(port)-1]
				serverNetIp := net.ParseIP(serverIp)
				if serverNetIp != nil {
					formatLog.InternalExt.NetIP = serverNetIp
					goto continueParse
				}
			}
		}
		log.Global().Warn("can't parse server ip, continue to use client ip")
	}
continueParse:
	formatLog.InternalExt.Isp, formatLog.InternalExt.Province, formatLog.InternalExt.Country = IPLib.QueryIP(formatLog.InternalExt.NetIP)
	if (formatLog.InternalExt.Country == "中国" || strings.HasPrefix(formatLog.InternalExt.Country, "CHINA")) &&
		formatLog.InternalExt.Province != "澳门" && formatLog.InternalExt.Province != "台湾" && formatLog.InternalExt.Province != "香港" {
		formatLog.InternalExt.Asn = -1
	} else {
		formatLog.InternalExt.Asn = int64(ASNLib.QueryASN(formatLog.InternalExt.NetIP))
		if ViewLibFn != nil {
			_, _, formatLog.InternalExt.View = ViewLibFn().QueryIP(formatLog.InternalExt.NetIP)
		}
	}
	formatLog.InternalExt.ParsedUrl, err = url.Parse(formatLog.RequestUrl)
	if err != nil {
		return nil, errors.New("illegal RequestUrl: " + formatLog.RequestUrl)
	}
	if formatLog.InternalExt.ParsedTime, err = time.Parse("02/Jan/2006:15:04:05 -0700", formatLog.RequestTime); err != nil {
		return nil, errors.New("illegal RequestTime: " + formatLog.RequestTime)
	}
	formatLog.InternalExt.MinutelyTime = formatLog.InternalExt.ParsedTime.Truncate(time.Minute).Unix()
	formatLog.InternalExt.IsIpv6 = formatLog.InternalExt.NetIP.To4() == nil
	formatLog.InternalExt.IsParent = formatLog.XPeer != `"-"`
	formatLog.InternalExt.IsHit = !strings.Contains(formatLog.InternalHitStatus, "MISS")
	formatLog.InternalExt.IsHttps = formatLog.InternalExt.ParsedUrl.Scheme == "https"
	return formatLog, nil
}

func Init(ipDBPath string, ipv6DBPath string, asnDBPath string,
	viewDBEndpoint, viewDBBucket, viewDBPrefix, viewDBPath string, viewDBDelay time.Duration) (err error) {
	IPLib, err = bsip.NewIPLib(ipDBPath, ipv6DBPath)
	if err != nil {
		return errors.WithMessage(err, "failed to init ip db")
	}

	if asnDBPath != "" {
		ASNLib, err = bsip.NewASN(asnDBPath)
		if err != nil {
			return errors.WithMessage(err, "failed to init asn db")
		}
	}
	if viewDBPath != "" {
		ViewLibFn, err = viewDB.InitViewDB(viewDBEndpoint, viewDBBucket, viewDBPrefix, viewDBPath, viewDBDelay)
		if err != nil {
			return errors.WithMessage(err, "failed to init view db")
		}
	}
	return nil
}

var (
	ipSegments = [][2]net.IP{
		//内网网段
		{net.IPv4(10, 0, 0, 0), net.IPv4(10, 255, 255, 255)},
		{net.IPv4(172, 16, 0, 0), net.IPv4(172, 31, 255, 255)},
		{net.IPv4(192, 168, 0, 0), net.IPv4(192, 168, 255, 255)},
		//共享地址
		{net.IPv4(100, 64, 0, 0), net.IPv4(100, 127, 255, 255)},
		//美国国防部
		{net.IPv4(21, 0, 0, 0), net.IPv4(21, 255, 255, 255)},
		//保留地址
		{net.IPv4(224, 0, 0, 0), net.IPv4(239, 255, 255, 255)},
		{net.IPv4(240, 0, 0, 0), net.IPv4(255, 255, 255, 255)},
		{net.IPv4(0, 0, 0, 0), net.IPv4(0, 255, 255, 255)},
		//本机地址
		{net.IPv4(127, 0, 0, 0), net.IPv4(127, 255, 255, 255)},
		//本地链路
		{net.IPv4(169, 254, 0, 0), net.IPv4(169, 254, 255, 255)},
	}
)

func useServerIp(ip net.IP) bool {
	for _, ipSegment := range ipSegments {
		if bytes.Compare(ip, ipSegment[0]) >= 0 && bytes.Compare(ip, ipSegment[1]) <= 0 {
			return true
		}
	}
	return false
}
