package core

const (
	//paths
	logFileName = "DRReS.log"
	backupFileName = "DRReS.backup"
	recoveredFileName = "DRReS.recovered"
	snapshotDir = "snapshots"
	lastCheckpointFileName = "last_checkpoint"  // position of the last "begin_checkpoint" entry in the log file

	//checkpointing
	cpFreq = 10  // how often to make checkpoints; in seconds
	recordSize = 128  //size of records in a snapshot

	//metrics
	throughputWindowSize = 5 // in seconds
)
