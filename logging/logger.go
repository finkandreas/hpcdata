package logging

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

var once sync.Once
var log zerolog.Logger
var consolelevel zerolog.Level
var filelevel zerolog.Level
var requestFileLevel zerolog.Level

type consoleLogHook struct {
	logger zerolog.Logger
}

func (h consoleLogHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level >= h.logger.GetLevel() {
		h.logger.WithLevel(level).Msg(msg)
	}
}

func SetLogLevels(console, file, requestFile zerolog.Level) {
	consolelevel = console
	filelevel = file
	requestFileLevel = requestFile
}

type notifyHook struct {
	notify_ch chan<- string
	level     zerolog.Level
}

func (n notifyHook) Run(e *zerolog.Event, level zerolog.Level, message string) {
	if level >= n.level {
		select {
		case n.notify_ch <- message:
		default:
			log.Debug().Msgf("Could not queue notification message in channel. buffer full.")
		}
	}
}

func SetupWithNotifications(notify_url string, level zerolog.Level) {
	if level <= zerolog.DebugLevel {
		// logging must be higher than DEBUG, because we will log errors happening during notifications as DEBUG messages
		log.Error().Msgf("You cannot setup notifications to setup at level=%v. Level must be at least INFO. Ignoring notification setup", level)
		return
	}
	if notify_url == "" {
		log.Error().Msg("Requesting to setup with notifications, but notify_url is empty")
		return
	}

	notify_ch := make(chan string, 100)
	hostname, _ := os.Hostname()
	no_notify_log := Get()
	log = no_notify_log.Hook(notifyHook{notify_ch, level}) //notify_url, hostname, level})

	// spawn coroutine responsible for doing the network sending
	go func() {
		type NotifyMessage struct {
			Summary string `json:"summary"`
			Body    string `json:"body"`
		}
		retryClient := retryablehttp.NewClient()
		retryClient.Logger = nil // disable logger on retryClient
		for {
			message := <-notify_ch
			prefix := ""
			if level >= zerolog.ErrorLevel {
				prefix = "🔴 Error from CI middleware: "
			} else if level >= zerolog.WarnLevel {
				prefix = "🟡 Warning from CI middleware: "
			} else {
				prefix = "🟢 Notification from CI middleware: "
			}

			summary := fmt.Sprintf("%v, pod: %v", prefix, hostname)
			data, err := json.Marshal(NotifyMessage{Summary: summary, Body: message})
			if err != nil {
				no_notify_log.Error().Msgf("Error marshaling data for log notification. err=%v", err)
			}
			if resp, err := retryClient.Post(notify_url, "application/json", data); err != nil {
				no_notify_log.Error().Msgf("Failed sending message to notify_url=%v. err=%v", notify_url, err)
			} else if resp.StatusCode >= 400 {
				no_notify_log.Error().Msgf("Failed seinding message to notify_url=%v with StatusCode=%v", notify_url, resp.StatusCode)
			}
		}
	}()
}

func Get() *zerolog.Logger {
	once.Do(func() {
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

		consolelog := zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
			Level(consolelevel).
			With().Timestamp().
			Logger()

		rotatedLogFile := lumberjack.Logger{
			Filename:   "log",
			MaxSize:    20, // megabytes
			MaxBackups: 20,
			MaxAge:     1000, //days
			LocalTime:  true, //use localtime timestamp when rotating
			Compress:   true, // disabled by default
		}
		log = zerolog.New(&rotatedLogFile).
			Level(filelevel).
			With().Timestamp().
			Caller().
			Logger().
			Hook(consoleLogHook{consolelog})
	})

	return &log
}
func Debug(msg string)                                { log.Debug().Msg(msg) }
func Debugf(msgf string, v ...interface{})            { log.Debug().Msgf(msgf, v...) }
func Info(msg string)                                 { log.Info().Msg(msg) }
func Infof(msgf string, v ...interface{})             { log.Info().Msgf(msgf, v...) }
func Warn(msg string)                                 { log.Warn().Msg(msg) }
func Warnf(msgf string, v ...interface{})             { log.Warn().Msgf(msgf, v...) }
func Error(err error, msg string)                     { log.Error().Err(err).Msg(msg) }
func Errorf(err error, msgf string, v ...interface{}) { log.Error().Err(err).Msgf(msgf, v...) }
