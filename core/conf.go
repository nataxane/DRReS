package core

const (
	//paths
	backupFileName = "DRReS.backup"
	lastCheckpointFileName = "last_checkpoint"  // position of the last "begin_checkpoint" entry in the log file
	logFileName = "DRReS.log"
	recoveredFileName = "DRReS.recovered"
	snapshotDir = "snapshots"
	statsFileName = "throughput_stats"

	//checkpointing
	cpFreq = 10  // how often to make checkpoints; in seconds
	recordSize = 128  //size of records in a snapshot

	//logging
	maxWriteQps = 15000
	logEntrySize = 128

	//compaction
	keepLastNCheckpoints = 5

	//metrics
	throughputWindowSize = 1 // in seconds

)
