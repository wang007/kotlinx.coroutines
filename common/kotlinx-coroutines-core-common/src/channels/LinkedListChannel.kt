/*
 * Copyright 2016-2018 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.experimental.channels

import kotlinx.coroutines.experimental.selects.*

/**
 * Channel with linked-list buffer of a unlimited capacity (limited only by available memory).
 * Sender to this channel never suspends and [offer] always returns `true`.
 *
 * This channel is created by `Channel(Channel.UNLIMITED)` factory function invocation.
 *
 * This implementation is fully lock-free.
 */
public open class LinkedListChannel<E> : Channel<E> by BufferedChannel<E>(Channel.UNLIMITED)

