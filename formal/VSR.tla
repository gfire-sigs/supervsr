---- MODULE VSR ----
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS N, Values, Sessions, Requests, Blocks, Releases,
          MaxOps, ViewMax, PipelineMax, ResourceMax, ReplicationQuorumMax,
          GenerationMax, NoChecksum, NoValue, ConfigurationID, FrameEpoch,
          FirstRequest, SecondRequest

ASSUME /\ N \in Nat \ {0}
       /\ MaxOps \in Nat \ {0}
       /\ ViewMax \in Nat
       /\ PipelineMax \in Nat \ {0}
       /\ ResourceMax \in Nat \ {0}
       /\ ReplicationQuorumMax \in Nat \ {0}
       /\ GenerationMax \in Nat \ {0}
       /\ Values # {}
       /\ Sessions # {}
       /\ Releases \subseteq Nat \ {0}
       /\ Requests # {}
       /\ Blocks # {}
       /\ Releases # {}
       /\ NoChecksum \notin (0..ViewMax) \X (1..MaxOps) \X Values \X Sessions \X Requests
       /\ NoValue \notin Values
       /\ FirstRequest \in Requests
       /\ SecondRequest \in Requests
       /\ (MaxOps > 1 => FirstRequest # SecondRequest)

Replicas == 0..(N - 1)
Ops == 1..MaxOps
Views == 0..ViewMax
Normal == "normal"
ViewChange == "view-change"
Crashed == "crashed"
Statuses == {Normal, ViewChange, Crashed}

Min(a, b) == IF a <= b THEN a ELSE b
Max(a, b) == IF a >= b THEN a ELSE b
SetMax(set) == CHOOSE element \in set : \A other \in set : element >= other
SetMin(set) == CHOOSE element \in set : \A other \in set : element <= other
Qr == IF N = 2 THEN 2 ELSE Min(ReplicationQuorumMax, (N + 1) \div 2)
Qv == IF N = 2 THEN 2 ELSE N - Qr + 1
Qn == N - Qr + 1
Primary(v) == v % N

Checksum(v, op, value, session, request) == <<v, op, value, session, request>>
Checksums == {NoChecksum} \cup
             {Checksum(v, op, value, session, request) :
                v \in Views, op \in Ops, value \in Values,
                session \in Sessions, request \in Requests}
Record(v, op, value, session, request, parent) ==
    [view |-> v,
     op |-> op,
     value |-> value,
     session |-> session,
     request |-> request,
     parent |-> parent,
     checksum |-> Checksum(v, op, value, session, request)]
Records == [view : Views,
            op : Ops,
            value : Values,
            session : Sessions,
            request : Requests,
            parent : Checksums,
            checksum : Checksums]
BoundedLogs == {entries \in Seq(Records) : Len(entries) <= MaxOps}

VARIABLES log, status, view, durableView, logView,
          commitMin, commitMax, durable, prepareOK,
          committed, commitHistory, completedViews, canonical, executed,
          sessionCommit, replies,
          faulty, authoritative, nacks, joinContributions,
          resourceUse, releasedDurable, reused,
          currentRelease, upgradeReady,
          generation, completionState,
          membership, configuration, frameEpoch,
          crashedPrimaryViews

vars == <<log, status, view, durableView, logView,
          commitMin, commitMax, durable, prepareOK,
          committed, commitHistory, completedViews, canonical, executed,
          sessionCommit, replies,
          faulty, authoritative, nacks, joinContributions,
          resourceUse, releasedDurable, reused,
          currentRelease, upgradeReady,
          generation, completionState,
          membership, configuration, frameEpoch,
          crashedPrimaryViews>>

Init ==
    /\ log = [r \in Replicas |-> <<>>]
    /\ status = [r \in Replicas |-> Normal]
    /\ view = [r \in Replicas |-> 0]
    /\ durableView = [r \in Replicas |-> 0]
    /\ logView = [r \in Replicas |-> 0]
    /\ commitMin = [r \in Replicas |-> 0]
    /\ commitMax = [r \in Replicas |-> 0]
    /\ durable = {}
    /\ prepareOK = {}
    /\ committed = [op \in Ops |-> NoChecksum]
    /\ commitHistory = {}
    /\ completedViews = {}
    /\ canonical = [v \in Views |-> <<>>]
    /\ executed = [r \in Replicas |-> <<>>]
    /\ sessionCommit = [s \in Sessions |-> [request \in Requests |-> NoChecksum]]
    /\ replies = {}
    /\ faulty = {}
    /\ authoritative = [r \in Replicas |-> [op \in Ops |-> NoChecksum]]
    /\ nacks = {}
    /\ joinContributions = {}
    /\ resourceUse = [r \in Replicas |-> 0]
    /\ releasedDurable = {}
    /\ reused = {}
    /\ currentRelease = SetMin(Releases)
    /\ upgradeReady = {}
    /\ generation = [r \in Replicas |-> 0]
    /\ completionState = [r \in Replicas |-> 0]
    /\ membership = [r \in Replicas |-> r]
    /\ configuration = ConfigurationID
    /\ frameEpoch = FrameEpoch
    /\ crashedPrimaryViews = {}

CoreUnchanged ==
    UNCHANGED <<status, view, durableView, logView, commitMin, commitMax,
                committed, commitHistory, completedViews, canonical, executed,
                sessionCommit, replies, crashedPrimaryViews>>
StorageUnchanged == UNCHANGED <<log, durable, prepareOK>>
EvidenceUnchanged == UNCHANGED <<faulty, authoritative, nacks, joinContributions>>
ResourceUnchanged == UNCHANGED <<resourceUse, releasedDurable, reused,
                                  currentRelease, upgradeReady>>
RuntimeUnchanged == UNCHANGED <<generation, completionState>>
IdentityUnchanged == UNCHANGED <<membership, configuration, frameEpoch>>
Prepare(r, value, session, request) ==
    /\ r \in Replicas
    /\ value \in Values
    /\ session \in Sessions
    /\ request \in Requests
    /\ status[r] = Normal
    /\ r = Primary(view[r])
    /\ Len(log[r]) < MaxOps
    /\ Len(log[r]) - commitMin[r] < PipelineMax
    /\ LET op == Len(log[r]) + 1
           parent == IF op = 1 THEN NoChecksum ELSE log[r][op - 1].checksum
       IN log' = [log EXCEPT ![r] = Append(@, Record(view[r], op, value, session, request, parent))]
    /\ CoreUnchanged
    /\ UNCHANGED <<durable, prepareOK>>
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

Replicate(from, to) ==
    /\ from \in Replicas
    /\ to \in Replicas \ {from}
    /\ status[to] # Crashed
    /\ Len(log[to]) < Len(log[from])
    /\ LET op == Len(log[to]) + 1
           entry == log[from][op]
           parent == IF op = 1 THEN NoChecksum ELSE log[to][op - 1].checksum
       IN /\ entry.parent = parent
          /\ log' = [log EXCEPT ![to] = Append(@, entry)]
    /\ CoreUnchanged
    /\ UNCHANGED <<durable, prepareOK>>
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

MakeDurable(r, op) ==
    /\ r \in Replicas
    /\ op \in 1..Len(log[r])
    /\ durable' = durable \cup {<<r, op, log[r][op].checksum>>}
    /\ UNCHANGED <<log, prepareOK>>
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

Acknowledge(r, op, checksum) ==
    /\ <<r, op, checksum>> \in durable
    /\ status[r] = Normal
    /\ op <= Len(log[r])
    /\ log[r][op].checksum = checksum
    /\ log[r][op].view = view[r]
    /\ prepareOK' = prepareOK \cup {<<r, op, checksum>>}
    /\ UNCHANGED <<log, durable>>
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

PrepareOKVoters(op, checksum) == {r \in Replicas : <<r, op, checksum>> \in prepareOK}
PreparedQuorum(op, checksum) == Cardinality(PrepareOKVoters(op, checksum)) >= Qr
MaxCommitted == Cardinality({op \in Ops : committed[op] # NoChecksum})

Commit(r) ==
    /\ r \in Replicas
    /\ status[r] = Normal
    /\ r = Primary(view[r])
    /\ commitMin[r] < Len(log[r])
    /\ LET op == commitMin[r] + 1
           entry == log[r][op]
           checksum == entry.checksum
       IN /\ PreparedQuorum(op, checksum)
          /\ sessionCommit[entry.session][entry.request] \in {NoChecksum, checksum}
          /\ committed' = IF committed[op] = NoChecksum
                             THEN [committed EXCEPT ![op] = checksum]
                             ELSE committed
          /\ commitHistory' = commitHistory \cup {<<op, checksum>>}
          /\ commitMin' = [commitMin EXCEPT ![r] = op]
          /\ commitMax' = [commitMax EXCEPT ![r] = Max(@, op)]
          /\ executed' = [executed EXCEPT ![r] = Append(@, checksum)]
          /\ sessionCommit' = [sessionCommit EXCEPT ![entry.session][entry.request] = checksum]
          /\ replies' = replies \cup {<<entry.session, entry.request, checksum>>}
    /\ UNCHANGED <<log, durable, prepareOK>>
    /\ UNCHANGED <<status, view, durableView, logView, completedViews, canonical,
                    crashedPrimaryViews>>
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

LearnCommit(r) ==
    /\ r \in Replicas
    /\ status[r] # Crashed
    /\ commitMin[r] < MaxCommitted
    /\ LET op == commitMin[r] + 1
       IN /\ op <= Len(log[r])
          /\ committed[op] # NoChecksum
          /\ log[r][op].checksum = committed[op]
          /\ commitMin' = [commitMin EXCEPT ![r] = op]
          /\ commitMax' = [commitMax EXCEPT ![r] = Max(@, MaxCommitted)]
          /\ executed' = [executed EXCEPT ![r] = Append(@, committed[op])]
    /\ UNCHANGED <<log, durable, prepareOK>>
    /\ UNCHANGED <<status, view, durableView, logView, committed, commitHistory,
                    completedViews, canonical, sessionCommit, replies,
                    crashedPrimaryViews>>
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

StartViewChange(r, newView) ==
    /\ r \in Replicas
    /\ newView \in Views
    /\ newView > view[r]
    /\ status' = [status EXCEPT ![r] = ViewChange]
    /\ view' = [view EXCEPT ![r] = newView]
    /\ UNCHANGED <<durableView, logView, commitMin, commitMax, committed,
                    commitHistory, completedViews, canonical, executed,
                    sessionCommit, replies, crashedPrimaryViews>>
    /\ StorageUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

PersistView(r) ==
    /\ r \in Replicas
    /\ status[r] = ViewChange
    /\ durableView[r] < view[r]
    /\ durableView' = [durableView EXCEPT ![r] = view[r]]
    /\ UNCHANGED <<status, view, logView, commitMin, commitMax, committed,
                    commitHistory, completedViews, canonical, executed,
                    sessionCommit, replies, crashedPrimaryViews>>
    /\ StorageUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

ContainsChecksum(entries, op, checksum) ==
    Len(entries) >= op /\ entries[op].checksum = checksum
ExtendsCanonical(entries, targetView) ==
    /\ Len(entries) >= Len(canonical[targetView])
    /\ \A op \in 1..Len(canonical[targetView]) :
          entries[op].checksum = canonical[targetView][op].checksum
PreservesCommitted(entries) ==
    \A op \in Ops :
        committed[op] = NoChecksum \/ ContainsChecksum(entries, op, committed[op])
InstallLog(r, targetView) ==
    IF ExtendsCanonical(log[r], targetView) /\ PreservesCommitted(log[r])
    THEN log[r]
    ELSE canonical[targetView]
ViewReports(targetView) ==
    {r \in Replicas :
        status[r] = ViewChange /\ view[r] = targetView /\ durableView[r] = targetView}
ReportPreservesPrepared(report) ==
    \A op \in Ops, checksum \in Checksums :
        PreparedQuorum(op, checksum) => ContainsChecksum(log[report], op, checksum)
SafeReporters(targetView) ==
    {r \in ViewReports(targetView) : ReportPreservesPrepared(r)}
ReportHeadView(report) ==
    IF Len(log[report]) = 0 THEN 0 ELSE log[report][Len(log[report])].view
SelectedLength(targetView) ==
    IF SafeReporters(targetView) = {}
    THEN 0
    ELSE SetMax({Len(log[r]) : r \in SafeReporters(targetView)})
LengthRankedReporters(targetView) ==
    {r \in SafeReporters(targetView) : Len(log[r]) = SelectedLength(targetView)}
SelectedLogView(targetView) ==
    IF LengthRankedReporters(targetView) = {}
    THEN 0
    ELSE SetMax({ReportHeadView(r) : r \in LengthRankedReporters(targetView)})
ViewRankedReporters(targetView) ==
    {r \in LengthRankedReporters(targetView) :
        ReportHeadView(r) = SelectedLogView(targetView)}
SelectedReporter(targetView) ==
    IF ViewRankedReporters(targetView) = {}
    THEN 0
    ELSE SetMin(ViewRankedReporters(targetView))
SelectedCanonical(targetView) ==
    IF SafeReporters(targetView) = {} THEN <<>> ELSE log[SelectedReporter(targetView)]

CompleteView(targetView) ==
    /\ targetView \in Views \ {0}
    /\ targetView \notin completedViews
    /\ Cardinality(ViewReports(targetView)) >= Qv
    /\ completedViews' = completedViews \cup {targetView}
    /\ canonical' = [canonical EXCEPT ![targetView] = SelectedCanonical(targetView)]
    /\ UNCHANGED <<status, view, durableView, logView, commitMin, commitMax,
                    committed, commitHistory, executed, sessionCommit, replies,
                    crashedPrimaryViews>>
    /\ StorageUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

InstallView(r, targetView) ==
    /\ r \in Replicas
    /\ targetView \in completedViews
    /\ status[r] = ViewChange
    /\ view[r] = targetView
    /\ durableView[r] = targetView
    /\ <<r, targetView>> \notin crashedPrimaryViews
    /\ commitMin[r] <= Len(InstallLog(r, targetView))
    /\ r = Primary(targetView) => commitMin[r] = MaxCommitted
    /\ status' = [status EXCEPT ![r] = Normal]
    /\ logView' = [logView EXCEPT ![r] = targetView]
    /\ log' = [log EXCEPT ![r] = InstallLog(r, targetView)]
    /\ commitMax' = [commitMax EXCEPT ![r] = MaxCommitted]
    /\ UNCHANGED <<view, durableView, commitMin, committed, commitHistory,
                    completedViews, canonical, executed, sessionCommit, replies,
                    crashedPrimaryViews>>
    /\ UNCHANGED <<durable, prepareOK>>
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

Crash(r) ==
    /\ r \in Replicas
    /\ status[r] # Crashed
    /\ status' = [status EXCEPT ![r] = Crashed]
    /\ crashedPrimaryViews' = IF r = Primary(view[r])
                                 THEN crashedPrimaryViews \cup {<<r, view[r]>>}
                                 ELSE crashedPrimaryViews
    /\ UNCHANGED <<view, durableView, logView, commitMin, commitMax, committed,
                    commitHistory, completedViews, canonical, executed,
                    sessionCommit, replies>>
    /\ StorageUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

Recover(r) ==
    /\ r \in Replicas
    /\ status[r] = Crashed
    /\ LET fenced == <<r, view[r]>> \in crashedPrimaryViews
           installed == durableView[r] = view[r] /\ logView[r] = view[r]
           recoveredView == IF fenced THEN view[r] + 1 ELSE view[r]
       IN /\ recoveredView \in Views
          /\ view' = [view EXCEPT ![r] = recoveredView]
          /\ status' = [status EXCEPT ![r] = IF fenced \/ ~installed THEN ViewChange ELSE Normal]
    /\ UNCHANGED <<durableView, logView, commitMin, commitMax, committed,
                    commitHistory, completedViews, canonical, executed,
                    sessionCommit, replies, crashedPrimaryViews>>
    /\ StorageUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged
Repair(r, op, checksum) ==
    /\ r \in Replicas
    /\ op \in 1..Len(log[r])
    /\ checksum = log[r][op].checksum
    /\ durable' = durable \cup {<<r, op, checksum>>}
    /\ UNCHANGED <<log, prepareOK>>
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

FaultSlot(r, op) ==
    /\ r \in Replicas
    /\ op \in Ops
    /\ faulty' = faulty \cup {<<r, op>>}
    /\ UNCHANGED <<authoritative, nacks, joinContributions>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

RecordAuthority(r, op) ==
    /\ r \in Replicas
    /\ authoritative[r][op] = NoChecksum
    /\ op \in 1..Len(log[r])
    /\ authoritative' = [authoritative EXCEPT ![r][op] = log[r][op].checksum]
    /\ UNCHANGED <<faulty, nacks, joinContributions>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

ObserveAdvertised(r, op, advertised) ==
    /\ r \in Replicas
    /\ op \in Ops
    /\ advertised \in Checksums \ {NoChecksum}
    /\ LET known == authoritative[r][op]
       IN /\ nacks' = IF known # NoChecksum /\ known # advertised
                          THEN nacks \cup {<<r, op, advertised>>}
                          ELSE nacks
          /\ joinContributions' = IF known = advertised
                                     THEN joinContributions \cup {<<r, op, "present">>}
                                     ELSE IF known # NoChecksum
                                          THEN joinContributions \cup {<<r, op, "nack">>}
                                          ELSE joinContributions
    /\ UNCHANGED <<faulty, authoritative>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

ContributeCommittedProof(r, op) ==
    /\ r \in Replicas
    /\ op \in Ops
    /\ committed[op] # NoChecksum
    /\ joinContributions' = joinContributions \cup {<<r, op, "proof">>}
    /\ UNCHANGED <<faulty, authoritative, nacks>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ ResourceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

AllocateResource(r) ==
    /\ r \in Replicas
    /\ resourceUse[r] < ResourceMax
    /\ resourceUse' = [resourceUse EXCEPT ![r] = @ + 1]
    /\ UNCHANGED <<releasedDurable, reused, currentRelease, upgradeReady>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

FreeResource(r) ==
    /\ r \in Replicas
    /\ resourceUse[r] > 0
    /\ resourceUse' = [resourceUse EXCEPT ![r] = @ - 1]
    /\ UNCHANGED <<releasedDurable, reused, currentRelease, upgradeReady>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

PublishRelease(block) ==
    /\ block \in Blocks
    /\ releasedDurable' = releasedDurable \cup {block}
    /\ UNCHANGED <<resourceUse, reused, currentRelease, upgradeReady>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

ReuseBlock(block) ==
    /\ block \in releasedDurable
    /\ reused' = reused \cup {block}
    /\ UNCHANGED <<resourceUse, releasedDurable, currentRelease, upgradeReady>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

MarkUpgradeReady(r, release) ==
    /\ r \in Replicas
    /\ release \in Releases
    /\ release > currentRelease
    /\ upgradeReady' = upgradeReady \cup {<<r, release>>}
    /\ UNCHANGED <<resourceUse, releasedDurable, reused, currentRelease>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

TransitionRelease(release) ==
    /\ release \in Releases
    /\ release > currentRelease
    /\ \A r \in Replicas : <<r, release>> \in upgradeReady
    /\ currentRelease' = release
    /\ UNCHANGED <<resourceUse, releasedDurable, reused, upgradeReady>>
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ RuntimeUnchanged
    /\ IdentityUnchanged

InvalidateGeneration(r) ==
    /\ r \in Replicas
    /\ generation[r] < GenerationMax
    /\ generation' = [generation EXCEPT ![r] = @ + 1]
    /\ UNCHANGED completionState
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ IdentityUnchanged

ApplyCompletion(r, submittedGeneration) ==
    /\ r \in Replicas
    /\ completionState[r] < GenerationMax
    /\ submittedGeneration = generation[r]
    /\ completionState' = [completionState EXCEPT ![r] = @ + 1]
    /\ UNCHANGED generation
    /\ StorageUnchanged
    /\ CoreUnchanged
    /\ EvidenceUnchanged
    /\ ResourceUnchanged
    /\ IdentityUnchanged

StaleCompletion(r, submittedGeneration) ==
    /\ r \in Replicas
    /\ submittedGeneration \in 0..(generation[r] + 1)
    /\ submittedGeneration # generation[r]
    /\ UNCHANGED vars

Next ==
    \/ \E r \in Replicas, value \in Values, session \in Sessions, request \in Requests :
          Prepare(r, value, session, request)
    \/ \E from \in Replicas, to \in Replicas : Replicate(from, to)
    \/ \E r \in Replicas, op \in Ops : MakeDurable(r, op)
    \/ \E r \in Replicas, op \in Ops, checksum \in Checksums : Acknowledge(r, op, checksum)
    \/ \E r \in Replicas : Commit(r)
    \/ \E r \in Replicas : LearnCommit(r)
    \/ \E r \in Replicas, targetView \in Views : StartViewChange(r, targetView)
    \/ \E r \in Replicas : PersistView(r)
    \/ \E targetView \in Views : CompleteView(targetView)
    \/ \E r \in Replicas, targetView \in Views : InstallView(r, targetView)
    \/ \E r \in Replicas : Crash(r) \/ Recover(r)
    \/ \E r \in Replicas, op \in Ops, checksum \in Checksums : Repair(r, op, checksum)
    \/ \E r \in Replicas, op \in Ops : FaultSlot(r, op) \/ RecordAuthority(r, op)
    \/ \E r \in Replicas, op \in Ops, checksum \in Checksums : ObserveAdvertised(r, op, checksum)
    \/ \E r \in Replicas, op \in Ops : ContributeCommittedProof(r, op)
    \/ \E r \in Replicas : AllocateResource(r) \/ FreeResource(r)
    \/ \E block \in Blocks : PublishRelease(block) \/ ReuseBlock(block)
    \/ \E r \in Replicas, release \in Releases : MarkUpgradeReady(r, release)
    \/ \E release \in Releases : TransitionRelease(release)
    \/ \E r \in Replicas : InvalidateGeneration(r)
    \/ \E r \in Replicas :
          \E submittedGeneration \in 0..(generation[r] + 1) :
              ApplyCompletion(r, submittedGeneration) \/ StaleCompletion(r, submittedGeneration)
AuthorityPairs ==
    {<<r, op>> \in Replicas \X Ops : authoritative[r][op] # NoChecksum}


EvidenceActive ==
    faulty # {} \/ nacks # {} \/ joinContributions # {} \/
    \E r \in Replicas, op \in Ops : authoritative[r][op] # NoChecksum
ResourceActive == \E r \in Replicas : resourceUse[r] # 0
BlockActive == releasedDurable # {} \/ reused # {}
UpgradeActive == currentRelease # SetMin(Releases) \/ upgradeReady # {}
RuntimeActive ==
    \E r \in Replicas : generation[r] # 0 \/ completionState[r] # 0
AuxiliaryModes ==
    (IF EvidenceActive THEN {"evidence"} ELSE {}) \cup
    (IF ResourceActive THEN {"resource"} ELSE {}) \cup
    (IF BlockActive THEN {"block"} ELSE {}) \cup
    (IF UpgradeActive THEN {"upgrade"} ELSE {}) \cup
    (IF RuntimeActive THEN {"runtime"} ELSE {})
EvidenceConsensusBase ==
    /\ \A r \in Replicas :
          status[r] = Normal /\ view[r] = 0 /\ commitMin[r] = 0 /\ commitMax[r] = 0 /\
          Len(log[r]) <= 1
    /\ durable = {}
    /\ prepareOK = {}
    /\ commitHistory = {}
    /\ completedViews = {}
    /\ replies = {}
ConsensusIdle ==
    /\ \A r \in Replicas :
          log[r] = <<>> /\ status[r] = Normal /\ view[r] = 0 /\
          commitMin[r] = 0 /\ commitMax[r] = 0
    /\ durable = {}
    /\ prepareOK = {}
    /\ commitHistory = {}
    /\ completedViews = {}
    /\ replies = {}
StateConstraint ==
    /\ Cardinality(AuxiliaryModes) <= 1
    /\ Cardinality(faulty) <= 1
    /\ Cardinality(AuthorityPairs) <= 1
    /\ Cardinality(nacks) <= 1
    /\ Cardinality(joinContributions) <= 1
    /\ EvidenceActive => EvidenceConsensusBase
    /\ (ResourceActive \/ BlockActive \/ UpgradeActive \/ RuntimeActive) => ConsensusIdle
ExpectedRequest(op) == IF op = 1 THEN FirstRequest ELSE SecondRequest
ViewAdvanced == \E r \in Replicas : view[r] > 0

ConsensusStateConstraint ==
    /\ ~EvidenceActive /\ ~ResourceActive /\ ~BlockActive /\ ~UpgradeActive /\ ~RuntimeActive
    /\ \A r \in Replicas :
          /\ status[r] # Crashed
          /\ \A op \in 1..Len(log[r]) : log[r][op].request = ExpectedRequest(op)
          /\ Len(log[r]) < 2 \/ log[r][2].view = 1
    /\ ViewAdvanced => committed[1] # NoChecksum

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ log \in [Replicas -> BoundedLogs]
    /\ status \in [Replicas -> Statuses]
    /\ view \in [Replicas -> Views]
    /\ durableView \in [Replicas -> Views]
    /\ logView \in [Replicas -> Views]
    /\ commitMin \in [Replicas -> 0..MaxOps]
    /\ commitMax \in [Replicas -> 0..MaxOps]
    /\ committed \in [Ops -> Checksums]
    /\ canonical \in [Views -> BoundedLogs]
    /\ executed \in [Replicas -> Seq(Checksums)]
    /\ resourceUse \in [Replicas -> 0..ResourceMax]

ViewStateInvariant ==
    \A r \in Replicas :
        /\ durableView[r] <= view[r]
        /\ logView[r] <= view[r]
        /\ status[r] = Normal => view[r] = durableView[r] /\ view[r] = logView[r]
        /\ commitMin[r] <= commitMax[r]
        /\ commitMin[r] <= Len(log[r])
        /\ Len(log[r]) - commitMin[r] <= PipelineMax
        /\ status[r] = Normal /\ r = Primary(view[r]) => commitMin[r] = commitMax[r]

LogChainInvariant ==
    \A r \in Replicas : \A op \in 1..Len(log[r]) :
        /\ log[r][op].op = op
        /\ (op = 1 => log[r][op].parent = NoChecksum)
        /\ (op > 1 => log[r][op].parent = log[r][op - 1].checksum)

NoConflictingCommittedOperations ==
    \A op \in Ops : Cardinality({checksum \in Checksums : <<op, checksum>> \in commitHistory}) <= 1

CommittedOperationsAreContiguous ==
    \A op \in Ops : committed[op] # NoChecksum =>
        \A prior \in 1..op : committed[prior] # NoChecksum
LaterViewsContainCommittedPrefix ==
    \A targetView \in completedViews : \A op \in Ops :
        IF committed[op] = NoChecksum
        THEN TRUE
        ELSE committed[op][1] >= targetView \/
             ContainsChecksum(canonical[targetView], op, committed[op])

PrepareOKImpliesDurableRecoverability == prepareOK \subseteq durable
PreparedQuorumIsUnique ==
    \A op \in Ops :
        Cardinality({checksum \in Checksums : PreparedQuorum(op, checksum)}) <= 1
ReportRankingIsTotal ==
    \A targetView \in Views :
        Cardinality(ViewReports(targetView)) >= Qv => SafeReporters(targetView) # {}
SelectedCanonicalPreservesPreparedEvidence ==
    \A targetView \in Views :
        Cardinality(ViewReports(targetView)) >= Qv =>
            \A op \in Ops, checksum \in Checksums :
                PreparedQuorum(op, checksum) =>
                    ContainsChecksum(SelectedCanonical(targetView), op, checksum)


FaultyUnknownSlotsNeverNack ==
    \A r \in Replicas, op \in Ops :
        <<r, op>> \in faulty /\ authoritative[r][op] = NoChecksum =>
            ~\E advertised \in Checksums : <<r, op, advertised>> \in nacks

NackRequiresKnownDifferentHeader ==
    \A nack \in nacks :
        LET r == nack[1] op == nack[2] advertised == nack[3]
        IN authoritative[r][op] # NoChecksum /\ authoritative[r][op] # advertised

JoinContributionRequiresEvidence ==
    \A contribution \in joinContributions :
        LET r == contribution[1] op == contribution[2] kind == contribution[3]
        IN \/ kind = "present" /\ authoritative[r][op] # NoChecksum
           \/ kind = "nack" /\ \E advertised \in Checksums : <<r, op, advertised>> \in nacks
           \/ kind = "proof" /\ committed[op] # NoChecksum

ExecutionIsGaplessAndExactlyOnce ==
    \A r \in Replicas :
        /\ Len(executed[r]) = commitMin[r]
        /\ \A op \in 1..commitMin[r] : executed[r][op] = committed[op]

ReplyMatchesCommittedSessionRequest ==
    \A reply \in replies :
        LET session == reply[1] request == reply[2] checksum == reply[3]
        IN sessionCommit[session][request] = checksum

MembershipAndIdentityInvariant ==
    /\ membership = [r \in Replicas |-> r]
    /\ configuration = ConfigurationID
    /\ frameEpoch = FrameEpoch

ResourcesRemainBounded == \A r \in Replicas : resourceUse[r] <= ResourceMax
BlockReuseRequiresDurableRelease == reused \subseteq releasedDurable
PrimaryCrashRequiresHigherView ==
    \A fence \in crashedPrimaryViews :
        LET r == fence[1] oldView == fence[2]
        IN ~(status[r] = Normal /\ view[r] = oldView)
ReleaseTransitionRequiresAllUpgradeReady ==
    \A release \in Releases : release <= currentRelease =>
        release = SetMin(Releases) \/ \A r \in Replicas : <<r, release>> \in upgradeReady
GenerationStateInvariant ==
    \A r \in Replicas : completionState[r] \in 0..GenerationMax /\ generation[r] \in 0..GenerationMax
QuorumIntersection == Qr + Qv > N /\ Qr + Qn > N

RepairDoesNotAdvanceHead ==
    [][\A r \in Replicas, op \in Ops, checksum \in Checksums :
        Repair(r, op, checksum) => UNCHANGED log]_vars
StaleCompletionDoesNotMutateState ==
    [][\A r \in Replicas :
        \A submittedGeneration \in 0..(generation[r] + 1) :
            submittedGeneration # generation[r] =>
                (StaleCompletion(r, submittedGeneration) => UNCHANGED vars)]_vars
CompletedCanonicalIsImmutable ==
    [][\A targetView \in completedViews : UNCHANGED canonical[targetView]]_vars

=============================================================================
