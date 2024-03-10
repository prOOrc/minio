// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package redis

import (
	"crypto/tls"
	"crypto/x509"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/env"
	xnet "github.com/minio/pkg/net"
	"github.com/redis/go-redis/v9"
)

const (
	// Default values used while communicating with redis.
	defaultDialTimeout   = 5 * time.Second
	defaultDialKeepAlive = 30 * time.Second
)

// redis environment values
const (
	Endpoints     = "endpoints"
	DB            = "db"
	Password      = "password"
	ClientCert    = "client_cert"
	ClientCertKey = "client_cert_key"

	EnvRedisEndpoints     = "MINIO_REDIS_ENDPOINTS"
	EnvRedisDB            = "MINIO_REDIS_DB"
	EnvRedisPassword      = "MINIO_REDIS_PASSWORD"
	EnvRedisClientCert    = "MINIO_REDIS_CLIENT_CERT"
	EnvRedisClientCertKey = "MINIO_REDIS_CLIENT_CERT_KEY"
)

// DefaultKVS - default KV settings for redis.
var (
	DefaultKVS = config.KVS{
		config.KV{
			Key:   Endpoints,
			Value: "",
		},
		config.KV{
			Key:   DB,
			Value: "",
		},
		config.KV{
			Key:   Password,
			Value: "",
		},
	}
)

// Config - server redis config.
type Config struct {
	Enabled bool `json:"enabled"`
	redis.UniversalOptions
}

// New - initialize new redis client.
func New(cfg Config) (redis.UniversalClient, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	client := redis.NewUniversalClient(&cfg.UniversalOptions)
	return client, nil
}

func parseEndpoints(urls string) ([]string, bool, error) {
	redisURL := strings.Split(urls, config.ValueSeparator)
	endpoints := make([]string, 0, len(urls))
	
	var redisSecure bool
	for _, url := range redisURL {
		u, err := xnet.ParseURL(url)
		if err != nil {
			return nil, false, err
		}
		if u.Host == "" {
			return nil, false, config.Errorf("all endpoints should be not empty: %s", url)
		}
		if !(u.Scheme == "" || u.Scheme == "redis" || u.Scheme == "rediss") {
			return nil, false, config.Errorf("endpoint schema should be empty or redis or rediss: %s", url)
		}
		if redisSecure && u.Scheme == "redis" {
			return nil, false, config.Errorf("all endpoints should be rediss or redis: %s", url)
		}
		// If one of the endpoint is rediss, we will use rediss directly.
		redisSecure = redisSecure || u.Scheme == "rediss"
		endpoints = append(endpoints, u.Host)
	}

	return endpoints, redisSecure, nil
}

// Enabled returns if redis is enabled.
func Enabled(kvs config.KVS) bool {
	endpoints := kvs.Get(Endpoints)
	return endpoints != ""
}

// LookupConfig - Initialize new redis config.
func LookupConfig(kvs config.KVS, rootCAs *x509.CertPool) (Config, error) {
	cfg := Config{}
	if err := config.CheckValidKeys(config.RedisSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	endpoints := env.Get(EnvRedisEndpoints, kvs.Get(Endpoints))
	if endpoints == "" {
		return cfg, nil
	}

	redisEndpoints, redisSecure, err := parseEndpoints(endpoints)
	if err != nil {
		return cfg, err
	}

	cfg.Enabled = true
	cfg.DialTimeout = defaultDialTimeout

	cfg.Addrs = redisEndpoints
	dbString := env.Get(EnvRedisDB, kvs.Get(DB))
	var db int
	if dbString != "" {
		db, err = strconv.Atoi(dbString)
		if err != nil {
			return cfg, err
		}
	}
	cfg.DB = db
	cfg.Password = env.Get(EnvRedisPassword, kvs.Get(Password))

	if redisSecure {
		cfg.TLSConfig = &tls.Config{
			RootCAs: rootCAs,
		}
		// This is only to support client side certificate authentication
		// https://coreos.com/redis/docs/latest/op-guide/security.html
		redisClientCertFile := env.Get(EnvRedisClientCert, kvs.Get(ClientCert))
		redisClientCertKey := env.Get(EnvRedisClientCertKey, kvs.Get(ClientCertKey))
		if redisClientCertFile != "" && redisClientCertKey != "" {
			cfg.TLSConfig.GetClientCertificate = func(unused *tls.CertificateRequestInfo) (*tls.Certificate, error) {
				cert, err := tls.LoadX509KeyPair(redisClientCertFile, redisClientCertKey)
				return &cert, err
			}
		}
	}

	return cfg, nil
}
