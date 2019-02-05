package core

const (
	logFileName = "DRReS.log"
	snapshotDir = "snapshots"
	lastCheckpointFileName = "last_checkpoint"  // position of the last "begin_checkpoint" entry in the log file

	cpFreq = 10  // how often make checkpoints; in seconds
	recordSize = 128  //size of records in a snapshot
)
