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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/minio/minio/internal/logger"
	"github.com/redis/go-redis/v9"
)

var errRedisUnreachable = errors.New("redis is unreachable, please check your endpoints")

func redisErrToErr(err error) error {
	if err == nil {
		return nil
	}
	switch err {
	case context.DeadlineExceeded:
		return fmt.Errorf("%w", errRedisUnreachable)
	default:
		return fmt.Errorf("unexpected error %w from redis, please check your endpoints", err)
	}
}

func saveKeyRedisWithTTL(ctx context.Context, client redis.UniversalClient, key string, data []byte, ttl int64) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	expiration := time.Duration(ttl)*time.Second
	_, err := client.Set(timeoutCtx, key, string(data), expiration).Result()
	logger.LogIf(ctx, err)
	return redisErrToErr(err)
}

func saveKeyRedis(ctx context.Context, client redis.UniversalClient, key string, data []byte, opts ...options) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	if len(opts) > 0 {
		return saveKeyRedisWithTTL(ctx, client, key, data, opts[0].ttl)
	}
	_, err := client.Set(timeoutCtx, key, string(data), 0).Result()
	logger.LogIf(ctx, err)
	return redisErrToErr(err)
}

func deleteKeyRedis(ctx context.Context, client redis.UniversalClient, key string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()

	_, err := client.Del(timeoutCtx, key).Result()
	logger.LogIf(ctx, err)
	return redisErrToErr(err)
}

func readKeyRedis(ctx context.Context, client redis.UniversalClient, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, defaultContextTimeout)
	defer cancel()
	resp, err := client.Get(timeoutCtx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, errConfigNotFound
		}
		logger.LogIf(ctx, err)
		return nil, redisErrToErr(err)
	}
	return resp, nil
}
