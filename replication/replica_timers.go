package replication

import "time"

type replicaTimers struct {
	ping       tickTimeout
	prepare    tickTimeout
	abdication tickTimeout
	commit     tickTimeout
	exit       tickTimeout
	join       tickTimeout
	viewChange tickTimeout
	pulse      tickTimeout
	repair     tickTimeout
}

func newReplicaTimers(process ProcessConfig) (replicaTimers, error) {
	resolved := process.resolvedTimers()
	periods := [...]time.Duration{
		resolved.pingInterval,
		resolved.prepareTimeout,
		resolved.abdicationTimeout,
		resolved.commitInterval,
		resolved.exitViewTimeout,
		resolved.joinViewTimeout,
		resolved.viewChangeTimeout,
		resolved.pulseInterval,
		resolved.repairInterval,
	}
	timers := replicaTimers{}
	destinations := [...]*tickTimeout{
		&timers.ping,
		&timers.prepare,
		&timers.abdication,
		&timers.commit,
		&timers.exit,
		&timers.join,
		&timers.viewChange,
		&timers.pulse,
		&timers.repair,
	}
	for index := range periods {
		timeout, err := newTimeout(periods[index], process.Tick, 0)
		if err != nil {
			return replicaTimers{}, err
		}
		*destinations[index] = timeout
		destinations[index].Start()
	}
	return timers, nil
}
