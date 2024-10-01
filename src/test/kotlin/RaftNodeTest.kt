import org.example.NodeState
import org.example.RaftNode
import org.example.RaftNode.LogEntry
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.BeforeEach

class RaftNodeTest {
    private lateinit var node1: RaftNode
    private lateinit var node2: RaftNode
    private lateinit var node3: RaftNode
    private lateinit var node4: RaftNode
    private lateinit var nodes: List<RaftNode>

    @BeforeEach
    fun setUp() {
        node1 = RaftNode(1, emptyList())
        node2 = RaftNode(2, emptyList())
        node3 = RaftNode(3, emptyList())
        node4 = RaftNode(4, emptyList())

        nodes = listOf(node1, node2, node3, node4)
        nodes.forEach { it.peers = nodes.filter { peer -> peer.id != it.id } }
    }

    @Test
    fun testLeaderElection() {
        node1.startElection()
        assertEquals(NodeState.LEADER, node1.state)
    }

    @Test
    fun testDisconnection() {
        node1.startElection()
        assertEquals(NodeState.LEADER, node1.state)

        node1.disconnect()
        Thread.sleep(2000) // Wait for election to complete

        val newLeader = nodes.firstOrNull { it.state == NodeState.LEADER }
        assertNotNull(newLeader)
        assertNotEquals(node1, newLeader)
    }

    @Test
    fun testReconnection() {
        node1.startElection()
        assertEquals(NodeState.LEADER, node1.state)

        node1.disconnect()
        Thread.sleep(2000) // Wait for election to complete

        val newLeader = nodes.firstOrNull { it.state == NodeState.LEADER }
        assertNotNull(newLeader)
    }

    @Test
    fun testLogReplication() {
        node1.startElection()
        assertEquals(NodeState.LEADER, node1.state)

        node1.appendLogEntry("command1")
        Thread.sleep(2000) // Wait for log replication

        for (node in nodes.filter { it.id != node1.id }) {
            assertEquals(1, node.log.size)
            assertEquals("command1", node.log[0].command)
        }
    }
}