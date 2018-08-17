package benchmarks.channel_new

import kotlinx.coroutines.experimental.CoroutineDispatcher
import kotlinx.coroutines.experimental.channels_new.RendezvousChannel
import kotlinx.coroutines.experimental.channels_new.select
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.scheduling.ExperimentalCoroutineDispatcher
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import java.util.concurrent.Phaser
import java.util.concurrent.TimeUnit
import kotlin.math.max

@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MICROSECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MICROSECONDS)
@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
open class ChannelProdConsBenchmark {
    @Param("false", "true")
    private var _1_withSelect: Boolean = false
    @Param("0", "1000")
    private var _2_coroutines: Int = 0
        @Param("1", "2", "4", "6", "8", "12", "16", "18", "24", "32", "36", "48", "64", "72", "96", "108", "128", "144")
    private var _3_parallelism: Int = 0

    private lateinit var dispatcher: CoroutineDispatcher

    @Setup
    fun setup() {
        dispatcher = ExperimentalCoroutineDispatcher(corePoolSize = _3_parallelism)
    }

    @Benchmark
    fun spmc() {
        if (_2_coroutines != 0) return
        val producers = max(1, _3_parallelism - 1)
        val consumers = 1
        if (newAlgo) runNew(producers, consumers) else runOld(producers, consumers)
    }

    @Benchmark
    fun mpmc() {
        val producers = if (_2_coroutines == 0) (_3_parallelism + 1) / 2 else _2_coroutines / 2
        val consumers = producers
        if (newAlgo) runNew(producers, consumers) else runOld(producers, consumers)
    }

    fun runOld(producers: Int, consumers: Int) {
        val n = approxBatchSize / producers * producers
        val c = kotlinx.coroutines.experimental.channels.RendezvousChannel<Int>()
        val phaser = Phaser(producers + consumers + 1)
        // Run producers
        repeat(producers) {
            launch(dispatcher) {
                val dummy = if (_1_withSelect) kotlinx.coroutines.experimental.channels.RendezvousChannel<Int>() else null
                repeat(n / producers) {
                    produceOld(c, it, dummy)
                }
                phaser.arrive()
            }
        }
        // Run consumers
        repeat(consumers) {
            launch(dispatcher) {
                val dummy = if (_1_withSelect) kotlinx.coroutines.experimental.channels.RendezvousChannel<Int>() else null
                repeat(n / consumers) {
                    consumeOld(c, dummy)
                }
                phaser.arrive()
            }
        }
        // Wait until work is done
        phaser.arriveAndAwaitAdvance()
    }

    fun runNew(producers: Int, consumers: Int) {
        val n = approxBatchSize / producers * producers
        val c = newChannel()
        val phaser = Phaser(producers + consumers + 1)
        // Run producers
        repeat(producers) {
            launch(dispatcher) {
                val dummy = if (_1_withSelect) newChannel() else null
                repeat(n / producers) {
                    produceNew(c, it, dummy)
                }
                phaser.arrive()
            }
        }
        // Run consumers
        repeat(consumers) {
            launch(dispatcher) {
                val dummy = if (_1_withSelect) newChannel() else null
                repeat(n / consumers) {
                    consumeNew(c, dummy)
                }
                phaser.arrive()
            }
        }
        // Wait until work is done
        phaser.arriveAndAwaitAdvance()
    }

    private suspend fun produceNew(c: RendezvousChannel<Int>,  it: Int, dummy: RendezvousChannel<Int>?) {
        if (_1_withSelect) {
            select<Unit> {
                c.onSend(it) {}
                dummy!!.onReceive {}
            }
        } else {
            c.send(it)
        }
        Blackhole.consumeCPU(work)
    }

    private suspend fun produceOld(c: kotlinx.coroutines.experimental.channels.RendezvousChannel<Int>,  it: Int, dummy: kotlinx.coroutines.experimental.channels.RendezvousChannel<Int>?) {
        if (_1_withSelect) {
            kotlinx.coroutines.experimental.selects.select<Unit> {
                c.onSend(it) {}
                dummy!!.onReceive {}
            }
        } else {
            c.send(it)
        }
        Blackhole.consumeCPU(work)
    }

    private suspend fun consumeNew(c: RendezvousChannel<Int>, dummy: RendezvousChannel<Int>?) {
        if (_1_withSelect) {
            select<Unit> {
                c.onReceive {}
                dummy!!.onReceive {}
            }
        } else {
            c.receive()
        }
        Blackhole.consumeCPU(work)
    }

    private suspend fun consumeOld(c: kotlinx.coroutines.experimental.channels.RendezvousChannel<Int>, dummy: kotlinx.coroutines.experimental.channels.RendezvousChannel<Int>?) {
        if (_1_withSelect) {
            kotlinx.coroutines.experimental.selects.select<Unit> {
                c.onReceive {}
                dummy!!.onReceive {}
            }
        } else {
            c.receive()
        }
        Blackhole.consumeCPU(work)
    }

    private fun newChannel() = RendezvousChannel<Int>(spinThreshold = 0)
}

const val newAlgo = true
const val work = 100L
const val approxBatchSize = 100000