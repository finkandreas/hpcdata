package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/zerolog"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/handler"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type LimitBodyMiddleware struct {
	limit int64
}

func (lmb *LimitBodyMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, lmb.limit)
		next.ServeHTTP(w, r)
	})
}

func main() {
	logging.SetLogLevels(zerolog.WarnLevel, zerolog.DebugLevel, zerolog.InfoLevel)
	logger := logging.Get()

	var configpath string
	flag.StringVar(&configpath, "config", "config.yaml", "Path to config YAML file")
	flag.Parse()
	config := util.ReadConfig(configpath)

	db := util.NewDb(config.GetDBPath())

	esclient := elastic.NewClient(config)

	handler.PrepareJwksKeyfuncApiGw(config.OpenIdConfig.JwksURLApiGw)
	handler.PrepareJwksKeyfunc(config.OpenIdConfig.JwksURL)

	reqHandler := mux.NewRouter()
	reqHandler.HandleFunc("/metrics/{system_name}/{job_id}/capstor/global", handler.GetCapstorGlobalHandler(config, esclient))
	reqHandler.HandleFunc("/metrics/{system_name}/{job_id}/gpu/temperature", handler.GetGpuTemperatureHandler(config, esclient))
	reqHandler.HandleFunc("/metrics/{system_name}/{job_id}/{node_id}/gpu/temperature", handler.GetGpuTemperatureHandler(config, esclient))
	reqHandler.HandleFunc("/metrics/push", handler.GetPushMetricHandler(config, &db))
	reqHandler.PathPrefix("/").Handler(handler.CatchAllHandler{})

	// install middlewares that
	// - Restrict maximum body length to some reasonable size (1MB), we do NOT expect larger requests, thus it should be an error
	// - Log every request, with all headers and full body. This way we can reproduce every request
	loggingMiddleware := logging.RequestLoggingMiddleware{Logger: logger}
	limitBodyMiddleware := LimitBodyMiddleware{config.Server.MaxBodySize}
	reqHandler.Use(limitBodyMiddleware.Middleware)
	reqHandler.Use(loggingMiddleware.Middleware)

	listenAddress := fmt.Sprintf("%v:%v", config.Server.Address, config.Server.Port)
	server := &http.Server{
		Addr:              listenAddress,
		ReadHeaderTimeout: 1 * time.Second,
		Handler:           reqHandler,
	}
	log.Printf("Starting server on %v", listenAddress)
	log.Fatalf("Server stopped. err=%v", server.ListenAndServe())
}
