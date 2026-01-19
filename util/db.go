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

func (db DB) GetMetricData(jobid, name, context, node, cluster string) ([]int64, []string, error) {
	rows, err := db.db.Query("select timestamp, value from userdata where jobid=? and name=? and context=? and node=? and cluster=? order by timestamp", jobid, name, context, node, cluster)
	if err != nil {
		logging.Error(err, "Failed query")
		return nil, nil, err
	}
	defer rows.Close()
	timestamps := []int64{}
	values := []string{}
	for rows.Next() {
		var timestamp int64
		var value string
		if err := rows.Scan(&timestamp, &value); err != nil {
			logging.Error(err, "Failed scanning row")
			return nil, nil, err
		}
		timestamps = append(timestamps, timestamp)
		values = append(values, value)
	}
	return timestamps, values, nil
}
