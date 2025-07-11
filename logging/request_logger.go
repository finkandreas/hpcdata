package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

type contextKey int

const correlationIdKey contextKey = 1

type handler_error struct {
	err        error
	usermsg    string
	statuscode int
}

func NewHandlerError(err error, usermsg string, statuscode int) handler_error {
	return handler_error{err: err, usermsg: usermsg, statuscode: statuscode}
}
func (h handler_error) Error() string {
	return h.err.Error()
}

type requestFileHook struct {
	logger zerolog.Logger
	xid    string
}

func (h requestFileHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level >= h.logger.GetLevel() {
		h.logger.WithLevel(level).Msgf("%v", msg)
	}
}

// a lot of code is inspired / stolen from "A complete guide to logging in Go with zerolog":
// https://betterstack.com/community/guides/logging/zerolog

// Log response code of requests
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newLoggingResponseWriter(w http.ResponseWriter) *loggingResponseWriter {
	return &loggingResponseWriter{w, http.StatusOK}
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	// remember response code and forward to the real response writer
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

type RequestLoggingMiddleware struct {
	Logger *zerolog.Logger
}

type ContextKey int

const ContextLogReqAsDebugKey ContextKey = 1

func (lm *RequestLoggingMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		lrw := newLoggingResponseWriter(w)

		correlationID := xid.New().String()
		r = r.WithContext(lm.Logger.With().Str("id", correlationID).Logger().WithContext(context.WithValue(r.Context(), correlationIdKey, correlationID)))
		w.Header().Add("X-Correlation-Id", correlationID)

		// only keep the last value of request headers (correctly, a request header should contain exactly one element)
		headers := zerolog.Dict()
		for k, v := range r.Header {
			if len(v) == 0 {
				lm.Logger.Error().Msgf("Header with key=%s has no value, not logging it as part of the request headers", k)
			} else {
				if len(v) > 1 {
					lm.Logger.Warn().Msgf("Header with key=%s has more than one value. All values=%v. Logging only last value as header value", k, v)
				}
				headers.Str(k, v[len(v)-1])
			}
		}

		// ReadAll() is used, since we use a middleware that transforms the body to a MaxBytesReader body, i.e. we  get an error if
		// a body is larger than the maximum accepted body size
		body, err := io.ReadAll(r.Body)
		if err != nil {
			content_length := r.Header["Content-Length"]
			lm.Logger.Error().
				Err(err).
				Str("method", r.Method).
				Str("path", r.RequestURI).
				Str("id", correlationID).
				Dict("headers", headers).
				Msgf("Error reading body: %v. Content-Length=%v", err, content_length)
			http.Error(w, "can't read body", http.StatusRequestEntityTooLarge)
			return
		}
		// And now set a new body, which will simulate the same data we read:
		r.Body = io.NopCloser(bytes.NewBuffer(body))

		defer func() {
			if panicVal := recover(); panicVal != nil {
				lrw.statusCode = http.StatusInternalServerError // ensure that the status code is updated
				response_msg := "Internal server error"
				if h_err, ok := panicVal.(handler_error); ok {
					// this is a known situation, i.e. we know the http statuscode and error message to return
					lrw.statusCode = h_err.statuscode
					response_msg = h_err.usermsg
				}
				response_msg_byte, _ := json.Marshal(map[string]any{
					"statuscode": lrw.statusCode,
					"message":    response_msg,
					"request-id": correlationID,
				})

				panicStr := fmt.Sprint(panicVal)
				stackbuf := make([]byte, 2048)
				n := runtime.Stack(stackbuf, false)
				stackbuf = stackbuf[:n]

				lm.Logger.Warn().
					Str("panic", panicStr).
					Bytes("stack", stackbuf).
					Str("method", r.Method).
					Str("path", r.RequestURI).
					Str("id", correlationID).
					Int("status_code", lrw.statusCode).
					Dur("elapsed", time.Since(start)).
					Dict("headers", headers).
					Bytes("body", body).
					Msgf("%s %s", r.Method, r.RequestURI)
				http.Error(lrw, string(response_msg_byte), lrw.statusCode)
				return
				//panic(panicVal) // continue panicking
			}

			logFct := lm.Logger.Info
			if val, ok := r.Context().Value(ContextLogReqAsDebugKey).(bool); ok && val {
				logFct = lm.Logger.Debug
			}
			logFct().
				Str("method", r.Method).
				Str("path", r.RequestURI).
				Str("id", correlationID).
				Int("status_code", lrw.statusCode).
				Dur("elapsed", time.Since(start)).
				Dict("headers", headers).
				Bytes("body", body).
				Msgf("%s %s", r.Method, r.RequestURI)
		}()

		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(lrw, r)
	})
}

// call this function as early as possible to enable logging also to an additional io.Writer
func LogReqHandlingToFile(r *http.Request, writer io.Writer) (*zerolog.Logger, string) {
	reqLogFileLogger := zerolog.New(zerolog.ConsoleWriter{Out: writer, TimeFormat: time.RFC3339}).Level(requestFileLevel).With().Timestamp().Logger()
	r = r.WithContext(GetReqLogger(r).Hook(requestFileHook{reqLogFileLogger, GetReqCorrelationID(r)}).WithContext(r.Context()))
	return GetReqLogger(r), GetReqCorrelationID(r)
}
func GetReqLogger(r *http.Request) *zerolog.Logger {
	return zerolog.Ctx(r.Context())
}
func GetReqCorrelationID(r *http.Request) string {
	return r.Context().Value(correlationIdKey).(string)
}
