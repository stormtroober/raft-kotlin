package org.example

import kotlin.concurrent.thread

class RaftNode(val id: Int, var peers: List<RaftNode>) {
    // Node state variables
    var state = NodeState.FOLLOWER
    var currentTerm = 0
    var votedFor: Int? = null
    var log = mutableListOf<LogEntry>()
    var commitIndex = 0
    var nextIndex = mutableMapOf<Int, Int>() // For leaders, index for next log entry to send to each peer
    var matchIndex = mutableMapOf<Int, Int>() // For leaders, index of highest log entry known to be replicated
    val heartbeatFrequency = 1000L // 1 second
    @Volatile private var running = true
    @Volatile private var connected = true

    init {
        // Initialize node as a follower with term 0
        becomeFollower(0)
    }

    // Data classes for Raft messages and log entries
    data class LogEntry(val term: Int, val command: String)
    data class AppendEntries(val term: Int, val leaderId: Int, val prevLogIndex: Int, val prevLogTerm: Int, val entries: List<LogEntry>, val leaderCommit: Int)
    data class RequestVote(val term: Int, val candidateId: Int, val lastLogIndex: Int, val lastLogTerm: Int)
    data class VoteResponse(val term: Int, val granted: Boolean)
    data class AppendEntriesResponse(val term: Int, val success: Boolean)

    // Transition to follower state
    fun becomeFollower(term: Int) {
        println("Node $id becoming follower with term $term")
        state = NodeState.FOLLOWER
        currentTerm = term
        votedFor = null
    }

    // Start an election to become the leader
    fun startElection() {
        println("Node $id starting election")
        state = NodeState.CANDIDATE
        currentTerm += 1
        votedFor = id
        var votesReceived = 1 // Voting for itself

        // Request votes from peers
        for (peer in peers) {
            val vote = peer.requestVote(RequestVote(currentTerm, id, log.size - 1, if (log.isNotEmpty()) log.last().term else 0))
            if (vote.granted) votesReceived += 1
        }

        // Check if received majority votes
        if (votesReceived > peers.size / 2) {
            becomeLeader()
        } else {
            becomeFollower(currentTerm)
        }
    }

    // Transition to leader state
    private fun becomeLeader() {
        println("Node $id becoming leader")
        state = NodeState.LEADER
        for (peer in peers) {
            nextIndex[peer.id] = log.size
            matchIndex[peer.id] = 0
        }

        // Start sending heartbeats to peers
        thread {
            while (state == NodeState.LEADER && running && connected) {
                sendHeartbeats()
                Thread.sleep(heartbeatFrequency) // Heartbeat interval
            }
        }
    }

    // Send heartbeats to all peers
    private fun sendHeartbeats() {
        for (peer in peers) {
            val prevLogIndex = if (log.isNotEmpty()) log.size - 1 else -1
            val prevLogTerm = if (log.isNotEmpty()) log.last().term else 0
            peer.appendEntries(
                AppendEntries(
                    currentTerm,
                    id,
                    prevLogIndex,
                    prevLogTerm,
                    emptyList(),
                    commitIndex
                )
            )
        }
    }

    // Append a new log entry and replicate it to followers
    fun appendLogEntry(command: String) {
        val newEntry = LogEntry(currentTerm, command)
        log.add(newEntry)
        for (peer in peers) {
            val prevLogIndex = if (log.size > 1) log.size - 2 else -1
            val prevLogTerm = if (log.size > 1) log[log.size - 2].term else 0
            peer.appendEntries(
                AppendEntries(
                    currentTerm,
                    id,
                    prevLogIndex,
                    prevLogTerm,
                    listOf(newEntry),
                    commitIndex
                )
            )
        }
    }

    // Handle AppendEntries RPC from leader
    fun appendEntries(request: AppendEntries): AppendEntriesResponse {
        println("[NODE $id] Received append entries from node ${request.leaderId} for term ${request.term}")
        // If the term in the request is greater than the current term, update the current term and become a follower
        if (request.term > currentTerm) {
            becomeFollower(request.term)
        }

        // If the term in the request is less than the current term, reject the request
        if (request.term < currentTerm) {
            return AppendEntriesResponse(currentTerm, false)
        }

        // Check if the log contains an entry at prevLogIndex whose term matches prevLogTerm
        if (request.prevLogIndex >= 0 && (log.size <= request.prevLogIndex || log[request.prevLogIndex].term != request.prevLogTerm)) {
            return AppendEntriesResponse(currentTerm, false)
        }

        // Append any new entries not already in the log
        val newEntriesStartIndex = request.prevLogIndex + 1
        if (newEntriesStartIndex < log.size) {
            log = log.subList(0, newEntriesStartIndex)
        }
        log.addAll(request.entries)

        // If leaderCommit is greater than commitIndex, set commitIndex to the minimum of leaderCommit and the index of the last new entry
        if (request.leaderCommit > commitIndex) {
            commitIndex = minOf(request.leaderCommit, log.size - 1)
        }

        // Return success response
        return AppendEntriesResponse(currentTerm, true)
    }

    // Handle RequestVote RPC from candidate
    fun requestVote(vote: RequestVote): VoteResponse {
        println("Node $id received vote request from ${vote.candidateId} for term ${vote.term}")
        if (vote.term > currentTerm) {
            becomeFollower(vote.term)
        }

        if (vote.term < currentTerm) {
            return VoteResponse(currentTerm, false)
        }

        if (votedFor == null || votedFor == vote.candidateId) {
            votedFor = vote.candidateId
            return VoteResponse(currentTerm, true)
        }

        return VoteResponse(currentTerm, false)
    }

    // Disconnect the node from the cluster
    fun disconnect() {
        println("Node $id disconnecting from the cluster")
        connected = false
        running = false
        if (state == NodeState.LEADER) {
            for (peer in peers) {
                peer.startElection()
            }
        }
        for (peer in peers) {
            peer.peers = peer.peers.filter { it.id != this.id }
        }
    }

    // Stop the node
    fun stop() {
        running = false
    }
}