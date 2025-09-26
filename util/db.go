package util

import (
    "database/sql"

    "cscs.ch/hpcdata/logging"
)

type DB struct {
	db *sql.DB
}

func NewDb(dbpath string) DB {
	logger := logging.Get()
	logger.Debug().Msgf("Using database at path %v", dbpath)
	var err error
	var db DB
	db.db, err = sql.Open("mysql", dbpath)
	if err != nil {
		logger.Error().Err(err).Msg("Error opening database")
		panic(err)
	}
	return db
}

func (db DB) PushMetricData(timestamp int64, jobid, name, value, xname, node, context, cluster string) bool {
    res, err := db.db.Exec("insert into userdata (`timestamp`, jobid, name, value, xname, node, context, cluster) values (?,?,?,?,?,?,?,?)", timestamp, jobid, name, value, xname, node, context, cluster)
    if err != nil {
        logging.Errorf(err, "Failed adding userdata, timestamp=%v, jobid=%v, name=%v, value=%v, xname=%v, node=%v, context=%v, cluster=%v err=%v", timestamp, jobid, name, value, xname, node, context, cluster, err)
        return false
    }
    if num_changed, err := res.RowsAffected() ; err != nil {
        logging.Errorf(err, "Failed adding userdata, timestamp=%v, jobid=%v, name=%v, value=%v, xname=%v, node=%v, context=%v, cluster=%v err=%v", timestamp, jobid, name, value, xname, node, context, cluster, err)
        return false
    } else {
        return num_changed==1
    }
}
