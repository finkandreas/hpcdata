package util;

import (
	"time"
)

type Node struct {
	Nid string
	Xname string
}
type Job struct {
	SlurmId int
	Account string
	Start   time.Time
	End     time.Time
	Nodes   []Node
}
