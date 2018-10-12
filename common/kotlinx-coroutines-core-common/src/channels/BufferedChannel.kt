package kotlinx.coroutines.experimental.channels

import kotlinx.atomicfu.AtomicRef
import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.getAndUpdate
import kotlinx.atomicfu.loop
import kotlinx.coroutines.experimental.CancellableContinuation
import kotlinx.coroutines.experimental.CancellableContinuationImpl
import kotlinx.coroutines.experimental.internal.Symbol
import kotlinx.coroutines.experimental.selects.SelectClause1
import kotlinx.coroutines.experimental.selects.SelectClause2
import kotlinx.coroutines.experimental.suspendAtomicCancellableCoroutine

/**
 * TODO
 */
class BufferedChannel<E>(val capacity: Int) : Channel<E> {

    init {
        require(capacity >= 0) { "Invalid channel capacity: $capacity, should be >=0" }
    }

    /**
     * Stores senders-receivers balance (`senders - receivers`), Each [send] and [receive]
     * (or [receiveOrNull]) operation uses `Fetch-And-Add` to update this balance. However,
     * in case of using the channel inside the [select] expression or by [offer] and [poll]
     * operations, 'Compare-And-Set' is used instead.
     */
    private val balance = atomic(0L)

    /**
     * Indicates if this channel is cancelled or not. In case it is cancelled,
     * it stores either an exception if it was cancelled with or `null` if
     * this channel was cancelled without error. Stores [EMPTY] if this
     * channel is not cancelled.
     */
    private val closeCause = atomic<Any?>(EMPTY)

    private val receiveException: Throwable
        get() = (closeCause.value as Throwable?) ?: ClosedReceiveChannelException(DEFAULT_CLOSE_MESSAGE)
    private val sendException: Throwable
        get() = (closeCause.value as Throwable?) ?: ClosedSendChannelException(DEFAULT_CLOSE_MESSAGE)

    /**
     * This field stores a close handler.
     */
    private val closeHandler = atomic<Any?>(null)


    /**
     * Stores either senders (continuations or select instances) or element to be sent.
     */
    private val senders = Queue()
    /**
     * Stores receivers (continuations or select instances).
     */
    private val receivers = Queue()

    override val isFull: Boolean
        get() {
            val balance = balance.value
            return !isClosedForSend && balance >= capacity
        } // senders - receivers >= capacity

    override val isEmpty: Boolean
        get() {
            val balance = balance.value
            return !isClosedForSend && balance <= 0
        } // senders <= receivers

    override val isClosedForSend: Boolean
        get() = closeCause.value !== EMPTY

    override val isClosedForReceive: Boolean
        get() = senders.isClosedForDequeue


    /**
     * Tries to resume a removed by [Queue.dequeue] operation continuation or select instance
     * with the specified value, and returns the sent via it element (see [CancellableContinuationImpl.element]).
     * In case [dequeue] returned [TRY_AGAIN] and it is passed to this function or resuming fails,
     * [TRY_AGAIN] is returned. If [dequeue] returned a sent element, it is just returned without any actions.
     */
    private fun tryResume(c: Any?, value: Any?): Any? {
        when {
            c === TRY_AGAIN -> return TRY_AGAIN
            c is CancellableContinuation<*> -> {
                c as CancellableContinuationImpl<in Any?>
                val result = c.element
                c.element = null
                val resumeToken = c.tryResume(value) ?: return TRY_AGAIN
                c.completeResume(resumeToken)
                return result
            }
            else -> return c // `c` is a sent element
        }
    }

    override suspend fun send(element: E) {
        val result = sendInternal(element)
        if (result === CLOSED) throw sendException
    }

    private suspend fun sendInternal(element: E): Any? = suspendAtomicCancellableCoroutine(holdCancellability = true) sc@{ curCont ->
        try_again@ while (true) {
            // Check if this channel is not cancelled.
            if (closeCause.value !== EMPTY) {
                curCont.resume(CLOSED)
                return@sc
            }
            // Increase the `senders - receivers` balance and get the previous value.
            val balance = this.balance.getAndIncrement()
            // Determine should we remove a receiver or
            // store the element or the current continuation to the senders queue.
            if (balance < 0) {
                // `senders < receivers` before this operation, try to make a rendezvous.
                val c = this.receivers.dequeue()
                if (tryResume(c, element) === TRY_AGAIN) continue@try_again
                curCont.resume(null)
                return@sc
            } else {
                // `senders >= receivers` before this operation, and we need either add the element to the senders queue
                // (in case `balance` is less than this channel capacity) or add the current continuation to it. In case
                // of adding the current continuation to the queue, the element should be stored into it (and cleaned up
                // if the addition fails).
                if (balance < capacity) {
                    if (!senders.enqueue(element)) continue@try_again
                    curCont.resume(null)
                    return@sc
                } else {
                    curCont as CancellableContinuationImpl<*>
                    curCont.element = element
                    if (senders.enqueue(curCont)) {
                        curCont.initCancellability()
                        return@sc
                    } else {
                        curCont.element = null
                        continue@try_again
                    }
                }
            }
        }
    }

    override fun offer(element: E): Boolean {
        try_again@ while (true) {
            // Check if this channel is not cancelled.
            if (closeCause.value !== EMPTY) {
                throw sendException
            }
            // Increase the `senders - receivers` balance and get the previous value.
            val balance = this.balance.value
            // Determine should we remove a receiver or
            // store the element or the current continuation to the senders queue.
            if (balance < 0) {
                if (!this.balance.compareAndSet(balance, balance + 1)) continue@try_again
                // `senders < receivers` before this operation, try to make a rendezvous.
                val c = this.receivers.dequeue()
                if (tryResume(c, element) === TRY_AGAIN) continue@try_again
                return true
            } else {
                // `senders >= receivers` before this operation, and we need either add the element to the senders queue
                // (in case `balance` is less than this channel capacity) or add the current continuation to it. In case
                // of adding the current continuation to the queue, the element should be stored into it (and cleaned up
                // if the addition fails).
                if (balance < capacity) {
                    if (!this.balance.compareAndSet(balance, balance + 1)) continue@try_again
                    if (!senders.enqueue(element)) continue@try_again
                    return true
                } else return false
            }
        }
    }

    override val onSend: SelectClause2<E, SendChannel<E>>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.


    override suspend fun receive(): E {
        val result = receiveInternal()
        if (result === CLOSED) throw this.receiveException
        return result as E
    }

    override suspend fun receiveOrNull(): E? {
        val result = receiveInternal()
        if (result === CLOSED) {
            val closeCause = closeCause.value ?: return null
            throw closeCause as Throwable
        }
        return result as E
    }

    private suspend fun receiveInternal(): Any? = suspendAtomicCancellableCoroutine(holdCancellability = true) sc@ { curCont ->
        try_again@ while (true) {
            val makeRendezvous: Boolean
            if (closeCause.value !== EMPTY) {
                if (senders.isClosedForDequeue) {
                    curCont.resume(CLOSED)
                    return@sc
                }
                // This channel is cancelled, but some senders can be in the `senders` queue.
                makeRendezvous = true
            } else {
                val balance = this.balance.getAndDecrement()
                makeRendezvous = balance > 0
            }
            if (makeRendezvous) {
                val c = this.senders.dequeue()
                val result = tryResume(c, Unit)
                if (result === TRY_AGAIN) continue@try_again
                curCont.resume(result)
                return@sc
            } else {
                if (receivers.enqueue(curCont)) {
                    curCont.initCancellability()
                    return@sc
                } else continue@try_again
            }
        }
    }

    override fun poll(): E? {
        try_again@ while (true) {
            val makeRendezvous: Boolean
            if (closeCause.value !== EMPTY) {
                if (senders.isClosedForDequeue) {
                    if (closeCause.value == null) return null
                    throw closeCause.value as Throwable
                }
                // This channel is cancelled, but some senders can be in the `senders` queue.
                makeRendezvous = true
            } else {
                val balance = this.balance.value
                if (balance > 0) {
                    if (!this.balance.compareAndSet(balance, balance - 1)) continue@try_again
                    makeRendezvous = true
                } else {
                    makeRendezvous = false
                }
            }
            if (makeRendezvous) {
                val c = this.senders.dequeue()
                val result = tryResume(c, Unit)
                if (result === TRY_AGAIN) continue@try_again
                return result as E?
            } else return null
        }
    }

    override val onReceiveOrNull: SelectClause1<E?>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.


    override val onReceive: SelectClause1<E>
        get() = TODO("not implemented") //To change initializer of created properties use File | Settings | File Templates.


    override fun close(cause: Throwable?): Boolean {
        val closedByThisOperation = closeCause.compareAndSet(EMPTY, cause)
        if (!closedByThisOperation) return false
        senders.closeForEnqueue()
        receivers.closeForEnqueueAndClean()
        invokeCloseHandler()
        return true
    }

    private fun invokeCloseHandler() {
        val closeHandler = closeHandler.getAndUpdate {
            if (it === null) CLOSE_HANDLER_CLOSED
            else CLOSE_HANDLER_INVOKED
        } ?: return
        closeHandler as (cause: Throwable?) -> Unit
        val closeCause = closeCause.value as Throwable?
        closeHandler(closeCause)
    }

    override fun cancel(cause: Throwable?): Boolean {
        // Phase 1. Close this channel.
        val closedByThisOperation = close(cause)
        // Phase 2. Remove all senders (including sent elements).
        senders.closeForEnqueueAndClean()
        // Return `true` if this channel is cancelled by this operation.
        return closedByThisOperation
    }

    override fun invokeOnClose(handler: (cause: Throwable?) -> Unit) {
        if (closeHandler.compareAndSet(null, handler)) {
            // Handler has been successfully set, finish the operation.
            return
        }
        // Either handler was set already or this channel is cancelled.
        // Read the value of [closeHandler] and either throw [IllegalStateException]
        // or invoke the handler respectively.
        val curHandler = closeHandler.value
        when (curHandler) {
            CLOSE_HANDLER_CLOSED -> {
                // In order to be sure that our handler is the only one, we have to change the
                // [closeHandler] value to `INVOKED`. If this CAS fails, another handler has been
                // executed and an [IllegalStateException] should be thrown.
                if (closeHandler.compareAndSet(CLOSE_HANDLER_CLOSED, CLOSE_HANDLER_INVOKED)) {
                    handler(closeCause.value as Throwable?)
                } else {
                    throw IllegalStateException("Another handler was already registered and successfully invoked")
                }
            }
            CLOSE_HANDLER_INVOKED -> {
                throw IllegalStateException("Another handler was already registered and successfully invoked")
            }
            else -> {
                throw IllegalStateException("Another handler was already registered: $curHandler")
            }
        }
    }

}

/**
 * This queue stores senders (or elements to be sent) or receivers, and supports
 * `enqueue`, `dequeue`, and `cancel` operations. In case of invoking
 * `dequeue` on the empty queue, it marks the next slot as BROKEN and
 * the following `enqueue` operation should fail. The additional `cancel`
 * operation marks this queue as cancelled, after that `enqueue` should always
 * return `false` and `dequeue` should always return the special [CLOSED] token.
 *
 * = IMPLEMENTATION NOTES =
 * TODO
 */
private class Queue {
    private val deqIdx = atomic(0L)
    private val enqIdx = atomic(0L)
    private val closed = atomic(false)

    private val head: AtomicRef<QNode>
    private val tail: AtomicRef<QNode>

    init {
        val emptyNode = QNode(0)
        head = atomic(emptyNode)
        tail = atomic(emptyNode)
    }

    /**
     * `true` if this queue does not contain any values. However,
     * it can contain [TAKEN] values only and all further dequeue
     * [dequeue] invocations return [TRY_AGAIN] while this property
     * is `false`. Therefore, it can't be used to detect if this queue
     * contains any non-taken values.
     */
    val isEmpty: Boolean get() = deqIdx.value >= enqIdx.value

    /**
     * `true` if this queue is closed and it either is empty or
     * contains [TAKEN] values only.
     */
    val isClosedForDequeue: Boolean
        get() {
            if (!closed.value) return false
            var head = this.head.value
            var i = this.deqIdx.value
            head = getHead(head, i / NODE_SIZE)
            while (i < this.enqIdx.value && head.readItem((i % NODE_SIZE).toInt(), TAKEN) === TAKEN) {
                this.deqIdx.compareAndSet(i, i + 1)
                i++
                head = getHead(head, i / NODE_SIZE)
            }
            return i >= this.enqIdx.value
        }

    /**
     * Tries to add the specified continuation, select instance, or
     * element ([x]) to this queue. It increments [enqIdx] at first, and gets
     * the slot to work with. After that, it tries to CAS this slot from
     * `null` to [x] and return either `true` or `false` depending on the CAS
     * success (an opposite [dequeue] operation can change it to [TAKEN]).
     *
     * This operation returns `false` if this queue is [closed].
     */
    fun enqueue(x: Any?): Boolean {
        // Check if this queue is not closed at first.
        // Return `false` in this case immediately.
        if (closed.value) return false
        // Claim the slot
        var tail = this.tail.value
        val i = this.enqIdx.getAndIncrement()
        if (closed.value) return false
        tail = getTail(tail, i / NODE_SIZE)
        // Try to CAS this slot from `null` to `x`. This CAS can fail
        // in case `dequeue` for this slot has been already invoked.
        return tail.casItem((i % NODE_SIZE).toInt(), EMPTY, x)
    }

    /**
     * Tries to dequeue the first value from this queue. It increments
     * [deqIdx] and either removes and returns an element from this slot,
     * or in case it is empty, changes it to [TAKEN] and returns [TRY_AGAIN].
     */
    fun dequeue(): Any? {
        // Get the slot to work with
        var head = this.head.value
        val i = this.deqIdx.getAndIncrement()
        head = getHead(head, i / NODE_SIZE)
        val indexInHead = (i % NODE_SIZE).toInt()
        // Read the value in the slot and either remove and return it
        // (CAS: x -> TAKEN) or mark it as broken (null -> TAKEN)
        // and return TRY_AGAIN.
        while (true) {
            val x = head.readItem(indexInHead, TAKEN)
            if (x === TAKEN) return TRY_AGAIN
            if (head.casItem(indexInHead, x, TAKEN)) return x
        }
    }

    /**
     * After this function is invoked, all further [enqueue]
     * invocations return `false` and all further [dequeue]
     * invocations return [TRY_AGAIN].
     */
    fun closeForEnqueue() {
        closed.value = true
    }

    /**
     * Invokes [closeForEnqueue] and cleans this queue,
     * cancelling all waiting coroutines. In order to
     * guarantee linearizability, it removes queue elements
     * from the end to the beginning (in the reverse order).
     */
    fun closeForEnqueueAndClean() {
        closeForEnqueue()
        val enqIdx = enqIdx.value
        var cur: QNode? = getTail(tail.value, enqIdx / NODE_SIZE)
        while (cur != null) {
            for (i in NODE_SIZE - 1 downTo 0) {
                cur.cancel(i)
            }
            cur = cur.prev.value
        }
    }

    private fun getHead(curHead: QNode, id: Long): QNode {
        if (curHead.id == id) return curHead
        val newHead = findOrCreateNode(curHead, id)
        moveHeadForward(newHead)
        return newHead
    }

    private fun getTail(curTail: QNode, id: Long): QNode {
        return findOrCreateNode(curTail, id)
    }

    private fun findOrCreateNode(startNode: QNode, id: Long): QNode {
        var cur = startNode
        while (cur.id < id) {
            var curNext = cur.next.value
            if (curNext == null) {
                val newTail = QNode(cur.id + 1)
                newTail.prev.value = cur
                if (cur.next.compareAndSet(null, newTail)) {
                    moveTailForward(newTail)
                    curNext = newTail
                } else {
                    curNext = cur.next.value!!
                }
            }
            cur = curNext
        }
        return cur
    }

    /**
     * Replaces [tail] with a new one
     * if [QNode.id] of the new one is greater.
     */
    private fun moveTailForward(newTail: QNode) {
        this.tail.loop { curTail ->
            if (curTail.id > newTail.id) return
            if (this.tail.compareAndSet(curTail, newTail)) return
        }
    }

    /**
     * Replaces [head] with a new one
     * if [QNode.id] of the new one is greater.
     */
    private fun moveHeadForward(newHead: QNode) {
        newHead.prev.value = null
        this.head.loop { curHead ->
            if (curHead.id > newHead.id) return
            if (this.head.compareAndSet(curHead, newHead)) return
        }
    }
}

private class QNode(val id: Long) {
//    val data = arrayOfNulls<Any?>(NODE_SIZE)
    val data = atomic<Any?>(EMPTY)
    val next = atomic<QNode?>(null)

    val cleaned = atomic(0) // TODO remove cleaned nodes!
    val prev = atomic<QNode?>(null)

    fun readItem(index: Int, replaceIfEmpty: Any): Any? {
        try_again@ while (true) {
            val x = getItem(index)
            return when {
                x === EMPTY -> {
                    if (casItem(index, EMPTY, replaceIfEmpty)) replaceIfEmpty
                    else continue@try_again
                }
                else -> x
            }
        }
    }

    fun cancel(i: Int) {
        val x = readItem(i, TAKEN)
        when {
            x === TAKEN -> return
            else -> {
                if (casItem(i, x, TAKEN)) {
                    if (x is CancellableContinuation<*>) {
                        x as CancellableContinuationImpl<in Any>
                        x.resume(CLOSED)
                    }
                }
            }
        }
    }

    inline fun getItem(i: Int): Any? = data.value
    inline fun casItem(i: Int, expected: Any?, new: Any?) = data.compareAndSet(expected, new)
}

// Number of continuations (elements, select instances)
// to be stored in each queue node.
private val NODE_SIZE = 1
// Special values for Any? cells
private val EMPTY = Symbol("EMPTY")
private val TAKEN = Symbol("TAKEN")
// Special values for `CLOSE_HANDLER`
private val CLOSE_HANDLER_CLOSED = Symbol("CLOSE_HANDLER_CLOSED")
private val CLOSE_HANDLER_INVOKED = Symbol("CLOSE_HANDLER_INVOKED")
// Returns if the channel is closed
private val CLOSED = Symbol("CLOSED")
// Returns if the attempt fails
private val TRY_AGAIN = Symbol("TRY_AGAIN")