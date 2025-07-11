package firecrest

import (
	"encoding/json"
	"fmt"
	"time"

	"cscs.ch/hpcdata/logging"
	"cscs.ch/hpcdata/util"
)

type IdNamePair struct {
	Id   string `json:"id"` // the UID/GID, but returned as string
	Name string `json:"name"`
}
type UserInfo struct {
	Group  IdNamePair   `json:"group"`
	Groups []IdNamePair `json:"groups"`
	User   IdNamePair   `json:"user"`
}

type JobTaskStatus struct {
	ExitCode        int    `json:"exitCode"`
	InterruptSignal int    `json:"interruptSignal"`
	State           string `json:"state"`
	StateReason     string `json:"stateReason"`
}
type JobTaskTime struct {
	Elapsed   int `json:"elapsed"`
	Start     int `json:"start"`
	End       int `json:"end"`   // 0 if not finished yet
	Limit     int `json:"limit"` // 0 if not set
	Suspended int `json:"suspeended"`
}
type JobTask struct {
	Id     string        `json:"id"`
	Name   string        `json:"name"`
	Status JobTaskStatus `json:"status"`
	Time   JobTaskTime   `json:"time"`
}
type Job struct {
	Account        string        `json:"account"`
	NumNodes       int           `json:"allocationNodes"`
	Cluster        string        `json:"cluster"`
	Group          string        `json:"group"`
	JobId          int           `json:"jobId"`
	KillRequestUsr string        `json:"killRequestUser"`
	Name           string        `json:"name"`
	Nodes          string        `json:"nodes"`
	Partition      string        `json:"partition"`
	Priority       int           `json:"priority"`
	Status         JobTaskStatus `json:"status"`
	Tasks          []JobTask     `json:"tasks"`
	Time           JobTaskTime   `json:"time"`
	User           string        `json:"user"`
	WorkingDir     string        `json:"workingDirectory"`
}
type Jobs struct {
	Jobs []Job `json:"jobs"`
}

type Client struct {
	baseURL       string
	system        string
	authorization string
}

func NewClient(url, system, authorization string) *Client {
	return &Client{
		baseURL:       url,
		system:        system,
		authorization: authorization,
	}
}

func (f *Client) UserInfo() (UserInfo, error) {
	ret := UserInfo{}
	err := f._get(
		fmt.Sprintf("status/%v/userinfo", f.system),
		&ret,
	)
	return ret, err
}

func (f *Client) Job(jobid string) (Job, error) {
	ret := Jobs{}
	err := f._get(
		fmt.Sprintf("compute/%v/jobs/%v", f.system, jobid),
		&ret,
	)
	if err != nil {
		return Job{}, err
	}
	if len(ret.Jobs) != 1 {
		return Job{}, fmt.Errorf("Firecrest did not return exactly one job")
	}

	return ret.Jobs[0], err
}

func (f *Client) _get(endpoint string, ret any) error {
	start := time.Now()
	resp, err := util.DoRequest("GET",
		fmt.Sprintf("%v/%v", f.baseURL, endpoint),
		map[string]string{"Authorization": f.authorization},
		nil)

	// check if request failed
	if err != nil {
		return err
	}

	// check if request has success statuscode
	err = util.CheckResponse(resp)
	if err != nil {
		return err
	}

	// check if unmarshaling works
	logging.Debugf("Firecrest request to %v took %vms. Response data=%v", resp.Request.URL.String(), time.Since(start).Milliseconds(), string(resp.ResponseData))
	err = json.Unmarshal(resp.ResponseData, &ret)
	if err != nil {
		return err
	}

	return nil
}
