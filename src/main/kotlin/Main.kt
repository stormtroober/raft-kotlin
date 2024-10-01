package org.example

fun main() {
    val node1 = RaftNode(1, emptyList())
    val node2 = RaftNode(2, emptyList())
    val node3 = RaftNode(3, emptyList())
    val node4 = RaftNode(4, emptyList())

    val nodes = listOf(node1, node2, node3, node4)
    // Link nodes as peers
    nodes.forEach { it.peers = nodes.filter { peer -> peer.id != it.id } }

    // Start election on one of the nodes
    node1.startElection()

    // Simulate disconnection of the leader
    Thread.sleep(5000)
    node1.disconnect()
}