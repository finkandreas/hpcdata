package util

import (
	"fmt"
	"io"
	"net/http"
	"time"

	cleanhttp "github.com/hashicorp/go-cleanhttp"
	"github.com/hashicorp/go-retryablehttp"
)

// use this if you do not need any additional headers for your request
var NoAdditionalHeaders = map[string]string{}

var retryClient = &retryablehttp.Client{
	HTTPClient:   cleanhttp.DefaultPooledClient(),
	Logger:       nil,
	RetryWaitMin: 1 * time.Second,
	RetryWaitMax: 3 * time.Second,
	RetryMax:     3,
	CheckRetry:   retryablehttp.DefaultRetryPolicy,
	Backoff:      retryablehttp.DefaultBackoff,
}

// helper struct which has the Body of the response automatically added under ResponseDa
type ResponseHelper struct {
	*http.Response
	ResponseData []byte
}

// helper to create an error if the statuscode >= 400, i.e. not successful
func CheckResponse(resp *ResponseHelper) error {
	if resp == nil {
		return fmt.Errorf("CheckResponse received a nil-response")
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("Request for %v failed with statuscode=%v msg=%v", resp.Request.URL.RequestURI(), resp.StatusCode, string(resp.ResponseData))
	}
	return nil
}

func DoRequest(method string, url string, headers map[string]string, data []byte) (*ResponseHelper, error) {
	req, err := retryablehttp.NewRequest(method, url, data)
	if err != nil {
		return nil, err
	}

	if headers != nil {
		for k, v := range headers {
			req.Header.Add(k, v)
		}
	}
	resp, err := retryClient.Do(req)
	if err != nil {
		return nil, err
	}

	var responseData []byte
	if resp.Body != nil {
		responseData, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
	}
	return &ResponseHelper{resp, responseData}, nil
}
