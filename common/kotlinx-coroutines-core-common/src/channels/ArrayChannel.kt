/*
 * Copyright 2016-2018 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.experimental.channels

import kotlinx.coroutines.experimental.internal.*
import kotlinx.coroutines.experimental.selects.*

/**
 * Channel with array buffer of a fixed [capacity].
 * Sender suspends only when buffer is fully and receiver suspends only when buffer is empty.
 *
 * This channel is created by `Channel(capacity)` factory function invocation.
 *
 * This implementation uses lock to protect the buffer, which is held only during very short buffer-update operations.
 * The lists of suspended senders or receivers are lock-free.
 */
public open class ArrayChannel<E>(
    /**
     * Buffer capacity.
     */
    val capacity: Int
) : Channel<E> by BufferedChannel<E>(capacity)
