package util

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

type ServerConfig struct {
	Address     string `yaml:"address"`
	Port        int    `yaml:"port"`
	MaxBodySize int64  `yaml:"max_body_size"`
}
type DatabaseConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Address  string `yaml:"address"`
	Net      string `yaml:"net"`
	Name     string `yaml:"dbname"`
}
type ElasticConfig struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}
type OpenIdConfig struct {
	JwksURL      string `yaml:"jwks_url"`
	JwksURLApiGw string `yaml:"jwks_url_api_gw"`
}
type SecurityConfig struct {
	AllowAnyJob []string `yaml:"allow_any_job"`
}
type ClusterConfig struct {
	Name        string `yaml:"name"`
	F7tURL      string `yaml:"f7t_url"`
	ElasticName string `yaml:"elastic_name"`
}
type Config struct {
	Server       ServerConfig   `yaml:"server"`
	Database     DatabaseConfig `yaml:"db"`
	Elastic      ElasticConfig  `yaml:"elastic"`
	OpenIdConfig `yaml:"openid"`
	Clusters     []ClusterConfig `yaml:"clusters"`
	Security     SecurityConfig  `yaml:"security"`
}

func ReadConfig(path string) *Config {
	var config Config
	data, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		log.Panicf("Error opening config file at path %v. err=%v", path, err)
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		log.Panicf("Error unmarshaling config file from YAML at path %v. err=%v", path, err)
	}

	// overwrite secrets from environment variables, if they are set (also overwrite even if the value is an empty string - if the env-variable exists, it will overwrite)
	if envvar, ok := os.LookupEnv("ELASTIC_PASSWORD"); ok {
		config.Elastic.Password = envvar
	}
	if envvar, ok := os.LookupEnv("HPCDATA_DATABASE_PASSWORD"); ok {
		config.Database.Password = envvar
	}

	if config.Database.Address == "" ||
		config.Database.User == "" ||
		config.Database.Password == "" ||
		config.Database.Net == "" ||
		config.Database.Name == "" {
		log.Fatalf("Database config section does not pass sanity checks. URL, Username and Password must all not be empty")
	}
	if config.Elastic.URL == "" ||
		config.Elastic.Username == "" ||
		config.Elastic.Password == "" {
		log.Fatalf("Elastic config section does not pass sanity checks. URL, Username and Password must all not be empty")
	}

	if config.OpenIdConfig.JwksURL == "" ||
		config.OpenIdConfig.JwksURLApiGw == "" {
		log.Fatalf("OpenId config section does not pass sanity checks. JwksURL and JwksURLApiGw are mandatory fields")
	}

	return &config
}

func (c *Config) GetClusterConfig(cluster string) (*ClusterConfig, error) {
	for _, cc := range c.Clusters {
		if cc.Name == cluster {
			return &cc, nil
		}
	}
	return nil, fmt.Errorf("Could not find cluster config for name=%s", cluster)
}

func (cfg *Config) GetDBPath() string {
	mysqlConfig := mysql.NewConfig()
	mysqlConfig.User = cfg.Database.User
	mysqlConfig.Passwd = cfg.Database.Password
	mysqlConfig.Addr = cfg.Database.Address
	mysqlConfig.DBName = cfg.Database.Name
	mysqlConfig.Net = cfg.Database.Net
	mysqlConfig.ParseTime = true
	mysqlConfig.Loc = time.Local
	return mysqlConfig.FormatDSN()
}
