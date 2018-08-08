package kotlinx.coroutines.experimental.channels_new

import kotlinx.coroutines.experimental.TestBase
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.yield
import kotlin.coroutines.experimental.coroutineContext
import kotlin.test.*

class RendezvousChannelStackTest  : TestBase() {
    @Test
    fun testSimple() = runTest {
        val q = RendezvousChannelStack<Int>()
        expect(1)
        val sender = launch(coroutineContext) {
            expect(4)
            q.send(1) // suspend -- the first to come to rendezvous
            expect(7)
            q.send(2) // does not suspend -- receiver is there
            expect(8)
        }
        expect(2)
        val receiver = launch(coroutineContext) {
            expect(5)
            check(q.receive() == 1) // does not suspend -- sender was there
            expect(6)
            check(q.receive() == 2) // suspends
            expect(9)
        }
        expect(3)
        sender.join()
        receiver.join()
        finish(10)
    }

    @Test
    fun testOfferAndPool() = runTest {
        val q = RendezvousChannelStack<Int>()
        assertFalse(q.offer(1))
        expect(1)
        launch(coroutineContext) {
            expect(3)
            assertEquals(null, q.poll())
            expect(4)
            assertEquals(2, q.receive())
            expect(7)
            assertEquals(null, q.poll())
            yield()
            expect(9)
            assertEquals(3, q.poll())
            expect(10)
        }
        expect(2)
        yield()
        expect(5)
        assertTrue(q.offer(2))
        expect(6)
        yield()
        expect(8)
        q.send(3)
        finish(11)
    }

    @Test
    fun testStress() = runTest {
        val n = 100_000
        val q = RendezvousChannelStack<Int>()
        val sender = launch(coroutineContext) {
            for (i in 1..n) q.send(i)
            expect(2)
        }
        val receiver = launch(coroutineContext) {
            for (i in 1..n) check(q.receive() == i)
            expect(3)
        }
        expect(1)
        sender.join()
        receiver.join()
        finish(4)
    }
}