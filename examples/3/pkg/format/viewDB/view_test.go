package viewDB

import "testing"

func TestCheckAndUpate(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		bucket    string
		prefix    string
		localPath string
		want      bool
		wantErr   bool
	}{
		{
			name:      "t1",
			endpoint:  "ss.bscstorage.com",
			bucket:    "ipdbx",
			prefix:    "ipdb",
			localPath: "/tmp/view.ipdbx",
			want:      true,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckAndUpate(tt.endpoint, tt.bucket, tt.prefix, tt.localPath)
			t.Log(got, err)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckAndUpate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CheckAndUpate() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_listAllIPDB(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		bucket    string
		prefix    string
		localPath string
		want      string
		want1     bool
		wantErr   bool
	}{
		{
			name:      "t1",
			endpoint:  "ss.bscstorage.com",
			bucket:    "ipdbx",
			prefix:    "ipdb",
			localPath: "/tmp/view.ipdbx",
			want:      "",
			want1:     true,
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got1, err := listAllIPBDX(tt.endpoint, tt.bucket, tt.prefix)
			t.Logf("%v", *got1[0].Key)
			if (err != nil) != tt.wantErr {
				t.Errorf("getLatestIPDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
