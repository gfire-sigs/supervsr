package replication

import "time"

type replicaTimers struct {
	ping       tickTimeout
	prepare    tickTimeout
	abdication tickTimeout
	commit     tickTimeout
	exit       tickTimeout
	join       tickTimeout
	pulse      tickTimeout
	repair     tickTimeout
}

func newReplicaTimers(process ProcessConfig) (replicaTimers, error) {
	periods := [...]time.Duration{
		time.Second,
		250 * time.Millisecond,
		10 * time.Second,
		500 * time.Millisecond,
		500 * time.Millisecond,
		500 * time.Millisecond,
		100 * time.Millisecond,
		100 * time.Millisecond,
	}
	timers := replicaTimers{}
	destinations := [...]*tickTimeout{
		&timers.ping,
		&timers.prepare,
		&timers.abdication,
		&timers.commit,
		&timers.exit,
		&timers.join,
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
