package config

import (
	"github.com/spf13/viper"
)

// 读取本地config目录下的yml配置文件
func UnmarshalConfig(config interface{}, appName string, configName string) error {
	viper.SetEnvPrefix(appName)
	viper.SetConfigType("yml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config/")

	viper.SetConfigName(configName)
	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	viper.SetConfigName(extendConfigName(configName))
	//本地配置文件忽略错误
	_ = viper.MergeInConfig()
	if err := viper.Unmarshal(config); err != nil {
		return err
	}
	return nil
}

func extendConfigName(configName string) string {
	if viper.GetString("env") == "" {
		return configName
	} else {
		return configName + "-" + viper.GetString("env")
	}
}

func init() {
	viper.AutomaticEnv()
}
