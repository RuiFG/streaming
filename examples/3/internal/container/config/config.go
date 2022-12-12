package config

import (
	"3/pkg/config"
	"time"
)

var application = Application{
	Debug:          false,
	CheckpointsDir: ".",
	Plugins:        nil,
	Format:         nil,
	Geddon:         nil,
	Medivh:         nil,
}

type Application struct {
	Debug          bool     `mapstructure:"debug"`
	CheckpointsDir string   `mapstructure:"checkpoints_dir"`
	Plugins        []string `mapstructure:"plugins"`
	Format         *Format  `mapstructure:"format"`
	Geddon         *Geddon  `mapstructure:"geddon"`
	Medivh         []Medivh `mapstructure:"medivhs"`
}

type Geddon struct {
	Pattern      string        `mapstructure:"pattern"`
	Dir          string        `mapstructure:"dir"`
	ScanDuration time.Duration `mapstructure:"scan_duration"`
}

type Medivh struct {
	Name     string   `mapstructure:"name"`
	Servers  []string `mapstructure:"servers"`
	Receives []string `mapstructure:"receives"`
}

type Format struct {
	IPDB   string `mapstructure:"ip_db"`
	IPV6DB string `mapstructure:"ipv6_db"`
	ASNDB  string `mapstructure:"asn_db"`
	ViewDB struct {
		Endpoint string        `mapstructure:"endpoint"`
		Bucket   string        `mapstructure:"bucket"`
		Prefix   string        `mapstructure:"prefix"`
		Path     string        `mapstructure:"path"`
		Delay    time.Duration `mapstructure:"delay"`
	} `mapstructure:"view_db"`
}

func Get() Application {
	return application
}

func init() {
	if err := config.UnmarshalConfig(&application, "application", "application"); err != nil {
		panic(err)
	}
}
