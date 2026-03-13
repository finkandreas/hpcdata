package handler

import (
	"bytes"
	"context"
	"encoding/gob"
	"time"

	"cscs.ch/hpcdata/firecrest"
	"cscs.ch/hpcdata/util"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

var redis_client *redis.Client = nil
var redis_lock *redsync.Redsync = nil

func InitRedis(cfg util.RedisConfig) {
	redis_client = redis.NewClient(&redis.Options{
		Addr:     cfg.Address,
		Password: cfg.Password,
		DB:       0,
	})
	pool := goredis.NewPool(redis_client)
	redis_lock = redsync.New(pool)
	gob.Register(util.Job{})
	gob.Register(firecrest.UserInfo{})
}

// getter must return a pointer to the type of interest + a caching duration + error if no caching should be done
func get_cached[T any](job_key string, logger *zerolog.Logger, getter func() (*T, time.Duration, error)) (*T, error) {
	ctx := context.Background()
	if cached_job_data, err := redis_client.Get(ctx, job_key).Bytes(); err == nil {
		logger.Debug().Msgf("Found cached data for job_key=%v", job_key)
		buf := bytes.NewBuffer(cached_job_data)
		var ret T
		if err := gob.NewDecoder(buf).Decode(&ret); err != nil {
			// warn in the log file, but ignore error and fetch freshly from f7t/elastic
			logger.Warn().Err(err).Msgf("Could not decode cached data for job_key=%v", job_key)
		} else {
			return &ret, nil
		}
	}
	// we end up here if cached data is not found, or could not be used - get it from the real implementation
	if ret, cache_timeout, err := getter(); err != nil {
		logger.Warn().Err(err).Msgf("Got an error trying to receive data for caching. job_key=%v", job_key)
		return ret, err
	} else {
		buf := bytes.NewBuffer(nil)
		if err := gob.NewEncoder(buf).Encode(*ret); err != nil {
			logger.Error().Err(err).Msgf("Failed encoding cache value with gob encoding")
		} else {
			redis_client.Set(ctx, job_key, buf.Bytes(), cache_timeout)
		}
		return ret, nil
	}
}
