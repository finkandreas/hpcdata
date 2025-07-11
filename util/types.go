package util;

import (
	"time"
)

type Job struct {
	SlurmId int
	Account string
	Start   time.Time
	End     time.Time
}

