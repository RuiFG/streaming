package format

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	raw := "113.212.111.0 v9.tiktokcdn.com \"video/mp4\" [07/Dec/2022:18:16:02 +0800] \"GET https://v9.tiktokcdn.com/c24ef8c2ec33cf2bb140eb5e107e3204/6390bb0a/video/tos/alisg/tos-alisg-pve-0037c001/o4kBByjEISReDvsEeAAWiIUnQggQkAKBDKbmJV/?a=1233&ch=0&cr=3&dr=0&lr=all&cd=0%7C0%7C0%7C3&cv=1&br=720&bt=360&cs=2&ds=6&ft=iusKbyt4ZZo0PDOAspMaQ9dTY8jNJE.C0&mime_type=video_mp4&qs=4&rc=Zmg3OTo8ODs5ZDo7aWZnZEBpang1M2Q6ZmUzaDMzODczNEBgNi1fMV9iXjUxYmMtX19fYSMuXmRvcjRvbTVgLS1kMS1zcw%3D%3D&l=202212071010360101040280912F46DDBC&btag=80000&cc=5 HTTP/1.1\" 206 512964 \"-\" \"AVMDL-1.1.103.75-mt-boringssl-ANDROID,MDLTaskPlay,MDLGroup(1233)\" 31 512000 \"512000\" \"bytes=0-511999\" \"-\" 113.212.111.0:26453@122.10.250.42:443@-@BC71_SG-singapore-singapore-21-cache-3,+BC2_BD-Dhaka-Dhaka-1-cache-3,+BC42_BD-Dhaka-Dhaka-1-cache-2,+BC42_BD-Dhaka-Dhaka-1-cache-2@Wed,+07+Dec+2022+08:02:26+GMT@e9a3ea3d37527340c0b458bc1c3983a6@792@355@-@T@TLSv1.3@TLS_AES_256_GCM_SHA384@v9.tiktokcdn.com@245@0.045@1@-@1@-@-@-@-@-@1@-@-@0@11 122.10.250.42:18006@206@-@512000@0.031@0.000@0.001@-@512719@https@0@512719@-@2312025@- \"-\" - TCP_HIT NONE -@_@-@_@bytes+0-511999/561157@_@https@_@0.031@_@206@_@BC71_SG-singapore-singapore-21-cache-3,+BC2_BD-Dhaka-Dhaka-1-cache-3,+BC42_BD-Dhaka-Dhaka-1-cache-2,+BC42_BD-Dhaka-Dhaka-1-cache-2@_@@_@113.212.111.0@_@792@_@2022-12-07T18:16:02+08:00@_@v9.tiktokcdn.com@_@7128442415372011013T1670408083019T52949"
	Init("/Users/klein/GoLandProjects/streaming/examples/3/pkg/format/ipv46_cn.rev1.ipdbx", "",
		"/Users/klein/GoLandProjects/streaming/examples/3/pkg/format/log-format-asnip",
		"", "", "", "/Users/klein/GoLandProjects/streaming/examples/3/pkg/format/view.ipdbx", time.Hour)
	Format(raw)
}
