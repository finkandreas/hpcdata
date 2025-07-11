package handler

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"
	"unsafe"

	"github.com/rs/zerolog"

	"cscs.ch/hpcdata/elastic"
	"cscs.ch/hpcdata/firecrest"
	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type handler_error struct {
	user_facing  string
	logfile_only string
}

func (e handler_error) Error() string {
	return e.user_facing
}

func herr(user_error, logfile_only string) error {
	return handler_error{user_facing: user_error, logfile_only: logfile_only}
}

type condition_error struct {
	Err string
}

func (e condition_error) Error() string {
	return "Some expected condition was not met. Err=" + e.Err
}

// panic-if-error
func pie(logFct func() *zerolog.Event, err error, msg string, statuscode int) {
	if err != nil {
		if msg == "" {
			msg = err.Error()
		}
		if err_herr, canConvert := err.(handler_error); canConvert {
			logFct().Err(fmt.Errorf(err_herr.logfile_only)).Type("error_type", err_herr).Msg(msg)
		} else {
			logFct().Err(err).Type("error_type", err).Msg(msg)
		}
		// logging/request_logger will recover from this panic, log it and write to the http.ResponseWriter
		panic(logging.NewHandlerError(err, msg, statuscode))
	}
}

// implements all security checks whether an API call is allowed to do an API call
// does not return anything, but panics if any error condition is encountered
func panic_if_no_access(r *http.Request, esclient *elastic.Client, config *util.Config) (*util.Job, time.Time, time.Time) {
	logger := logging.GetReqLogger(r)

	// check authentication
	_, err := validate_jwt(r)
	pie(logger.Warn, err, "JWT is invalid", http.StatusForbidden)

	jobid := r.URL.Query().Get("jobid")
	if jobid == "" {
		pie(logger.Warn, herr("The request parameter `jobid` is mandatory", fmt.Sprintf("jobid=`%s`", jobid)), "", http.StatusBadRequest)
	}

	cluster := r.URL.Query().Get("cluster")
	if cluster == "" {
		pie(logger.Warn, herr("The request parameter `cluster` is mandatory", fmt.Sprintf("cluster=`%s`", cluster)), "", http.StatusBadRequest)
	}

	cluster_config, err := config.GetClusterConfig(cluster)
	pie(logger.Warn, err, "", http.StatusBadRequest)

	auth := r.Header.Get("Authorization")
	f7t_client := firecrest.NewClient(cluster_config.F7tURL, cluster_config.Name, auth)
	user, err := f7t_client.UserInfo()
	pie(logger.Warn, err, "Failed fetching userinfo from FirecREST. Did you subscribe to the API?", http.StatusBadRequest)

	logger.Debug().Msgf("userinfo=%+v", user)

	job, err := get_job(jobid, cluster_config, f7t_client, esclient, config, logger)
	if errors.Is(err, util.ErrInvalidInput) {
		pie(logger.Warn, err, "", http.StatusBadRequest)
	} else {
		pie(logger.Error, err, "", http.StatusInternalServerError)
	}

	can_access := false
	for _, group := range user.Groups {
		if group.Name == job.Account || slices.Contains(config.Security.AllowAnyJob, group.Name) {
			can_access = true
			break
		}
	}
	if slices.Contains(config.Security.AllowAnyJob, user.User.Name) {
		can_access = true
	}

	if can_access == false {
		pie(logger.Warn, herr("You are not allowed to access the resource. The job's account does not match any of your groups", fmt.Sprintf("account=%v, user's groups=%+v", job.Account, user.Groups)), "", http.StatusUnauthorized)
	}

	zhTimezone, err := time.LoadLocation("Europe/Zurich")
	pie(logger.Error, err, "Failed getting Zurich timezone", http.StatusInternalServerError)

	from := job.Start
	from_query := r.URL.Query().Get("from")
	if from_query != "" {
		parsed_time, err := time.ParseInLocation("2006-01-02T15:04:05", from_query, zhTimezone)
		pie(logger.Warn, err, "Failed parsing `from` query. It must be in the format %Y-%m-%dT%H:%M:%S", http.StatusBadRequest)
		if parsed_time.Before(job.Start) {
			pie(logger.Warn, herr("Your `from` query is before the job's start time", fmt.Sprintf("from_query=%v, job.Start=%v", from_query, job.Start)), "", http.StatusBadRequest)
		}
		from = parsed_time
	}

	to := job.End
	to_query := r.URL.Query().Get("to")
	if to_query != "" {
		parsed_time, err := time.ParseInLocation("2006-01-02T15:04:05", to_query, zhTimezone)
		pie(logger.Warn, err, "Failed parsing `to` query. It must be in the format %Y-%m-%dT%H:%M:%S", http.StatusBadRequest)
		if parsed_time.After(job.End) {
			pie(logger.Warn, herr("Your `to` query is after the job's end time", fmt.Sprintf("to_query=%v, job.End=%v", to_query, job.End)), "", http.StatusBadRequest)
		}
		to = parsed_time
	}

	if from.After(to) {
		pie(logger.Warn, herr("Your `from` query is after your `to` query", fmt.Sprintf("from=%v, to=%v", from, to)), "", http.StatusBadRequest)
	}

	return job, from, to
}

// This is a helper struct to allow to jsonize an array []time.Time as an array unix epoch (i.e. a json array of integers)
type epochTime struct {
	time.Time
	// WARNING: Do not add any other fields. The struct must be bitwise the same as time.Time, because we cast it
	// to this type, to allow storing in JSON as epoch
	// See implementation `as_epoch_array` for the casting
}
func (e epochTime) MarshalJSON() ([]byte, error) {
    return fmt.Appendf(nil, "%d", e.Time.Unix()), nil
}
func as_epoch_array(in []time.Time) []epochTime {
	var t1 time.Time
	var t2 epochTime
	if unsafe.Sizeof(t1) != unsafe.Sizeof(t2) {
		// ensure that cast is valid
		panic("We cannot convert to epochTime, because the size of the two structs are not the same")
	}
	return unsafe.Slice((*epochTime)(unsafe.Pointer(&in[0])), len(in))
}
