package kotlinx.coroutines.experimental.channels_new

import kotlinx.coroutines.experimental.*
import java.util.concurrent.Phaser
import kotlin.test.*

class RendezvousChannelSelectTest {
    private fun newChannel() = RendezvousChannel<Int>(segmentSize = 2)

    @Test
    fun `SPSC stress test with select on main and dummy channels`(): Unit = runBlocking {
        val q = newChannel()
        val dummy = newChannel()
        val n = 100_000
        launch {
            repeat(n) { i ->
                select {
                    q.onSend(i) {}
                    dummy.onReceive { fail("Impossible") }
                }
            }
        }
        repeat(n) { i ->
            select {
                q.onReceive { received -> assertEquals(i, received) }
                dummy.onReceive { fail("Impossible") }
            }
        }
    }


    @Test
    fun `MPMC stress test with select for send only`() {
        val n = 100_000
        val k = 10
        val q = newChannel()
        val dummy = newChannel()
        val done = Phaser(2 * k + 1)
        repeat(k) {
            launch {
                repeat(n) { i ->
                    selectUnbiased<Unit> {
                        q.onSend(i) {}
                        dummy.onReceive {}
                    }
                }
                done.arrive()
            }
        }
        repeat(k) {
            launch {
                repeat(n) { q.receive() }
                done.arrive()
            }
        }
        done.arriveAndAwaitAdvance()
    }

    @Test
    fun `MPMC stress test using two channels with unbiased select`() {
        val n = 100_000
        val k = 10
        val q1 = newChannel()
        val q2 = newChannel()
        val done = Phaser(2 * k + 1)
        repeat(k) {
            launch {
                repeat(n) { i ->
                    selectUnbiased<Unit> {
                        q1.onSend(i) {}
                        q2.onSend(i) {}
                    }
                }
                done.arrive()
            }
        }
        repeat(k) {
            launch {
                repeat(n) { i ->
                    selectUnbiased<Unit> {
                        q1.onReceive {}
                        q2.onReceive {}
                    }
                }
                done.arrive()
            }
        }
        done.arriveAndAwaitAdvance()
    }

    @Test
    fun `check a dummy channel is cleaned after many selects`() {
        val n = 100_000
        val q = newChannel()
        val dummy = newChannel()
        val done = Phaser(3)
        // Add an initial waiting continuation to `dummy`
        launch { dummy.receive() }
        // Do work
        launch {
            repeat(n) { i ->
                selectUnbiased<Unit> {
                    q.onSend(i) {}
                    dummy.onReceive {}
                }
            }
            done.arrive()
        }
        launch {
            repeat(n) { q.receive() }
            done.arrive()
        }
        done.arriveAndAwaitAdvance()
        // Check that `dummy` channel has at most two node
        val head = dummy.head()
        val headNext = head.next()
        val tail = dummy.tail()
        assertTrue(head === tail || headNext === tail, "Dummy channel is not cleaned: " +
                "head=${head.id}, headNext=$headNext, tail=${tail.id}")
    }
}