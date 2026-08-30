---- MODULE MPSCRing ----
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS Capacity, Producers, PushesPerProducer

ASSUME /\ Capacity \in Nat \ {0, 1}
       /\ Producers \subseteq STRING
       /\ Producers # {}
       /\ IsFiniteSet(Producers)
       /\ PushesPerProducer \in Nat \ {0}

Slots == 0..(Capacity - 1)
TotalItems == Cardinality(Producers) * PushesPerProducer
Positions == 0..(TotalItems - 1)
Items == Producers \X (1..PushesPerProducer)
NoItem == <<>>
ProducerPCs == {"start", "scan", "reserved", "written", "done"}
ConsumerPCs == {"idle", "check", "clear", "release", "advance"}

Slot(position) == position % Capacity

VARIABLES enqueue, dequeue, sequences, values, reservations, released,
          producerPC, producerNext, producerPosition,
          consumerPC, consumerPosition, consumerItem, consumed

vars == <<enqueue, dequeue, sequences, values, reservations, released,
          producerPC, producerNext, producerPosition,
          consumerPC, consumerPosition, consumerItem, consumed>>

CurrentItem(producer) == <<producer, producerNext[producer]>>

Init ==
    /\ enqueue = 0
    /\ dequeue = 0
    /\ sequences = [slot \in Slots |-> slot]
    /\ values = [slot \in Slots |-> NoItem]
    /\ reservations = [position \in Positions |-> NoItem]
    /\ released = {}
    /\ producerPC = [producer \in Producers |-> "start"]
    /\ producerNext = [producer \in Producers |-> 1]
    /\ producerPosition = [producer \in Producers |-> 0]
    /\ consumerPC = "idle"
    /\ consumerPosition = 0
    /\ consumerItem = NoItem
    /\ consumed = <<>>

ProducerStart(producer) ==
    /\ producerPC[producer] = "start"
    /\ producerNext[producer] <= PushesPerProducer
    /\ producerPC' = [producerPC EXCEPT ![producer] = "scan"]
    /\ producerPosition' = [producerPosition EXCEPT ![producer] = enqueue]
    /\ UNCHANGED <<enqueue, dequeue, sequences, values, reservations, released,
                   producerNext, consumerPC, consumerPosition, consumerItem, consumed>>

ProducerDone(producer) ==
    /\ producerPC[producer] = "start"
    /\ producerNext[producer] = PushesPerProducer + 1
    /\ producerPC' = [producerPC EXCEPT ![producer] = "done"]
    /\ UNCHANGED <<enqueue, dequeue, sequences, values, reservations, released,
                   producerNext, producerPosition,
                   consumerPC, consumerPosition, consumerItem, consumed>>

ProducerReserve(producer) ==
    LET position == producerPosition[producer]
        slot == Slot(position)
    IN /\ producerPC[producer] = "scan"
       /\ position \in Positions
       /\ sequences[slot] = position
       /\ enqueue = position
       /\ reservations[position] = NoItem
       /\ enqueue' = enqueue + 1
       /\ reservations' = [reservations EXCEPT ![position] = CurrentItem(producer)]
       /\ producerPC' = [producerPC EXCEPT ![producer] = "reserved"]
       /\ UNCHANGED <<dequeue, sequences, values, released,
                      producerNext, producerPosition,
                      consumerPC, consumerPosition, consumerItem, consumed>>

ProducerReload(producer) ==
    LET position == producerPosition[producer]
        sequence == sequences[Slot(position)]
    IN /\ producerPC[producer] = "scan"
       /\ \/ sequence > position
          \/ /\ sequence = position
             /\ enqueue # position
       /\ producerPosition' = [producerPosition EXCEPT ![producer] = enqueue]
       /\ UNCHANGED <<enqueue, dequeue, sequences, values, reservations, released,
                      producerPC, producerNext,
                      consumerPC, consumerPosition, consumerItem, consumed>>

ProducerFull(producer) ==
    LET position == producerPosition[producer]
    IN /\ producerPC[producer] = "scan"
       /\ sequences[Slot(position)] < position
       /\ producerPC' = [producerPC EXCEPT ![producer] = "start"]
       /\ UNCHANGED <<enqueue, dequeue, sequences, values, reservations, released,
                      producerNext, producerPosition,
                      consumerPC, consumerPosition, consumerItem, consumed>>

ProducerWrite(producer) ==
    LET position == producerPosition[producer]
        slot == Slot(position)
    IN /\ producerPC[producer] = "reserved"
       /\ values[slot] = NoItem
       /\ values' = [values EXCEPT ![slot] = CurrentItem(producer)]
       /\ producerPC' = [producerPC EXCEPT ![producer] = "written"]
       /\ UNCHANGED <<enqueue, dequeue, sequences, reservations, released,
                      producerNext, producerPosition,
                      consumerPC, consumerPosition, consumerItem, consumed>>

ProducerPublish(producer) ==
    LET position == producerPosition[producer]
        slot == Slot(position)
    IN /\ producerPC[producer] = "written"
       /\ sequences' = [sequences EXCEPT ![slot] = position + 1]
       /\ producerNext' = [producerNext EXCEPT ![producer] = @ + 1]
       /\ producerPC' = [producerPC EXCEPT ![producer] = "start"]
       /\ UNCHANGED <<enqueue, dequeue, values, reservations, released,
                      producerPosition,
                      consumerPC, consumerPosition, consumerItem, consumed>>

ProducerStep(producer) ==
    \/ ProducerStart(producer)
    \/ ProducerDone(producer)
    \/ ProducerReserve(producer)
    \/ ProducerReload(producer)
    \/ ProducerFull(producer)
    \/ ProducerWrite(producer)
    \/ ProducerPublish(producer)

ConsumerStart ==
    /\ consumerPC = "idle"
    /\ consumerPC' = "check"
    /\ consumerPosition' = dequeue
    /\ UNCHANGED <<enqueue, dequeue, sequences, values, reservations, released,
                   producerPC, producerNext, producerPosition,
                   consumerItem, consumed>>

ConsumerMiss ==
    /\ consumerPC = "check"
    /\ sequences[Slot(consumerPosition)] # consumerPosition + 1
    /\ consumerPC' = "idle"
    /\ UNCHANGED <<enqueue, dequeue, sequences, values, reservations, released,
                   producerPC, producerNext, producerPosition,
                   consumerPosition, consumerItem, consumed>>

ConsumerHit ==
    LET slot == Slot(consumerPosition)
    IN /\ consumerPC = "check"
       /\ sequences[slot] = consumerPosition + 1
       /\ values[slot] # NoItem
       /\ consumerPC' = "clear"
       /\ consumerItem' = values[slot]
       /\ UNCHANGED <<enqueue, dequeue, sequences, values, reservations, released,
                      producerPC, producerNext, producerPosition,
                      consumerPosition, consumed>>

ConsumerClear ==
    /\ consumerPC = "clear"
    /\ values' = [values EXCEPT ![Slot(consumerPosition)] = NoItem]
    /\ consumerPC' = "release"
    /\ UNCHANGED <<enqueue, dequeue, sequences, reservations, released,
                   producerPC, producerNext, producerPosition,
                   consumerPosition, consumerItem, consumed>>

ConsumerRelease ==
    /\ consumerPC = "release"
    /\ sequences' = [sequences EXCEPT ![Slot(consumerPosition)] = consumerPosition + Capacity]
    /\ released' = released \cup {consumerPosition}
    /\ consumerPC' = "advance"
    /\ UNCHANGED <<enqueue, dequeue, values, reservations,
                   producerPC, producerNext, producerPosition,
                   consumerPosition, consumerItem, consumed>>

ConsumerAdvance ==
    /\ consumerPC = "advance"
    /\ dequeue' = consumerPosition + 1
    /\ consumed' = Append(consumed, consumerItem)
    /\ consumerItem' = NoItem
    /\ consumerPC' = "idle"
    /\ UNCHANGED <<enqueue, sequences, values, reservations, released,
                   producerPC, producerNext, producerPosition,
                   consumerPosition>>

ConsumerStep ==
    \/ ConsumerStart
    \/ ConsumerMiss
    \/ ConsumerHit
    \/ ConsumerClear
    \/ ConsumerRelease
    \/ ConsumerAdvance

Next ==
    \/ \E producer \in Producers : ProducerStep(producer)
    \/ ConsumerStep

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A producer \in Producers : WF_vars(ProducerStep(producer))
    /\ WF_vars(ConsumerStep)

TypeOK ==
    /\ enqueue \in Nat
    /\ dequeue \in Nat
    /\ sequences \in [Slots -> Nat]
    /\ values \in [Slots -> Items \cup {NoItem}]
    /\ reservations \in [Positions -> Items \cup {NoItem}]
    /\ released \subseteq Positions
    /\ producerPC \in [Producers -> ProducerPCs]
    /\ producerNext \in [Producers -> 1..(PushesPerProducer + 1)]
    /\ producerPosition \in [Producers -> Nat]
    /\ consumerPC \in ConsumerPCs
    /\ consumerPosition \in Nat
    /\ consumerItem \in Items \cup {NoItem}
    /\ consumed \in Seq(Items)

CursorBounds ==
    /\ dequeue <= enqueue
    /\ enqueue <= TotalItems
    /\ dequeue <= TotalItems
    /\ enqueue <= dequeue + Capacity + 1

ReservationDense ==
    \A position \in Positions :
        (reservations[position] # NoItem) <=> (position < enqueue)

ReleasedPrefix ==
    LET prefix == {position \in Positions : position < dequeue}
    IN released = prefix \cup
                  (IF consumerPC = "advance" THEN {consumerPosition} ELSE {})

ReservationInjective ==
    \A left, right \in Positions :
        /\ reservations[left] # NoItem
        /\ reservations[right] # NoItem
        /\ reservations[left] = reservations[right]
        => left = right

NoOverwriteBeforeRelease ==
    \A older, newer \in Positions :
        /\ older < newer
        /\ Slot(older) = Slot(newer)
        /\ reservations[newer] # NoItem
        => older \in released

ProducerOwnership ==
    \A producer \in Producers :
        LET position == producerPosition[producer]
            slot == Slot(position)
        IN /\ (producerPC[producer] = "reserved" =>
                  /\ position < enqueue
                  /\ reservations[position] = CurrentItem(producer)
                  /\ values[slot] = NoItem
                  /\ sequences[slot] = position)
           /\ (producerPC[producer] = "written" =>
                  /\ position < enqueue
                  /\ reservations[position] = CurrentItem(producer)
                  /\ values[slot] = CurrentItem(producer)
                  /\ sequences[slot] = position)

PublishedValues ==
    \A position \in Positions :
        /\ reservations[position] # NoItem
        /\ position \notin released
        /\ sequences[Slot(position)] = position + 1
        /\ ~(/\ consumerPC = "release"
             /\ consumerPosition = position)
        => values[Slot(position)] = reservations[position]

ActiveValuesOwned ==
    \A slot \in Slots :
        values[slot] # NoItem =>
            \E position \in Positions :
                /\ position \notin released
                /\ Slot(position) = slot
                /\ reservations[position] = values[slot]

ConsumerReadOwnership ==
    consumerPC \in {"clear", "release", "advance"} =>
        /\ consumerPosition < enqueue
        /\ reservations[consumerPosition] # NoItem
        /\ consumerItem = reservations[consumerPosition]

ConsumerFIFO ==
    /\ Len(consumed) = dequeue
    /\ \A index \in 1..Len(consumed) :
           consumed[index] = reservations[index - 1]

AllItemsEventuallyConsumed == <>(Len(consumed) = TotalItems)

=============================================================================
