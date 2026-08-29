package replication

import "time"

type replicaTimers struct {
	ping       Timeout
	prepare    Timeout
	abdication Timeout
	commit     Timeout
	exit       Timeout
	join       Timeout
	pulse      Timeout
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
	}
	timers := replicaTimers{}
	destinations := [...]*Timeout{
		&timers.ping,
		&timers.prepare,
		&timers.abdication,
		&timers.commit,
		&timers.exit,
		&timers.join,
		&timers.pulse,
	}
	for index := range periods {
		timeout, err := NewTimeout(periods[index], process.Tick, 0)
		if err != nil {
			return replicaTimers{}, err
		}
		*destinations[index] = timeout
		destinations[index].Start()
	}
	return timers, nil
}
