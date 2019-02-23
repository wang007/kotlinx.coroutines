/*
 * Copyright 2016-2019 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines

import kotlin.jvm.*
import kotlin.reflect.*

/**
 * A builder object for coroutine of type [C] with body block working in scope [S] and returning [T].
 * It provides a DSL to attach [catch] and [finally] blocks to the coroutine before it is [build].
 * 
 * Each call of [catch] and [finally] adds to the body coroutine in the corresponding exception handling
 * logic and returns a new instance of `CoroutineBuilder`. An instance of coroutine builder itself
 * is immutable and encapsulates all the parameters of the corresponding builder function call.
 * A call to [build] calls the coroutine builder. 
 */
public abstract class CoroutineBuilder<C, S, T> internal constructor(
    /**
     * @suppress It is not supposed to be extended outside of this module.
     */
    @JvmField
    @InternalCoroutinesApi
    private val current: Any // suspend S.() -> T | Clauses
) {
    @Suppress("UNCHECKED_CAST")
    protected val currentBlock: suspend S.() -> T
        get() = when (current) {
            is Clauses<*, *> -> current.block as suspend S.() -> T
            else -> current as suspend S.() -> T
        }

    @Suppress("UNCHECKED_CAST")
    private val currentClauses: Clauses<S, T>
        get() = when (current) {
            is Clauses<*, *> -> current as Clauses<S, T>
            else -> Clauses(current as suspend S.() -> T)
        }

    internal abstract fun updateBlock(block: Any /* suspend S.() -> T | Clauses */): CoroutineBuilder<C, S, T>

    /**
     * Wraps the coroutine code block of this builder with the `try/catch` block for exceptions of type [E],
     * executing the specified [handler] code when exception is caught with the exception as parameter.
     *
     * The resulting code is semantically equivalent to the following, albeit the
     * actual implementation is significantly more efficient and uses internal mechanisms to
     * catch exceptions of all the children coroutines in the code:
     *
     * ```
     * try {
     *     // coroutineScope to catch exceptions of all children coroutines, too
     *     coroutineScope {
     *         originalCodeBlock()
     *     }
     * } catch(cause: E) {
     *     // withContext to execute handler even when this coroutine is cancelled
     *     withContext(NonCancellable) {
     *         handler(cause)
     *     }
     * }
     * ```
     *
     * Calling `catch` multiple times adds `catch` clauses to the above `try/catch` construct.
     * Trying to add `catch` block after `finally` produces [IllegalStateException].
     */
    @Suppress("UNCHECKED_CAST")
    public inline fun <reified E : Throwable> catch(
        noinline handler: suspend CoroutineScope.(cause: E) -> T
    ): CoroutineBuilder<C, S, T> =
        catchImpl(E::class, handler as suspend CoroutineScope.(cause: Throwable) -> T)

    /**
     * Wraps the coroutine code block of this builder with the `try/finally` block,
     * executing the specified [handler] code with the failure exception as parameter.
     *
     * The resulting code is semantically equivalent to the following, albeit the
     * actual implementation is significantly more efficient and uses internal mechanisms to
     * catch exceptions of all the children coroutines in the code:
     *
     * ```
     * var cause: Throwable? = null
     * try {
     *     // coroutineScope to catch exceptions of all children coroutines, too
     *     coroutineScope {
     *         originalCodeBlock()
     *     }
     * } catch(e: Throwable) {
     *     cause = e // remember what exception was the cause
     * } finally {
     *     // withContext to execute handler even when this coroutine is cancelled
     *     withContext(NonCancellable) {
     *         handler(cause)
     *     }
     *     cause?.let { throw it } // rethrow original exception (if it was caught)
     * }
     * ```
     *
     * Trying to add more than one `finally` block produces [IllegalStateException].
     */
    public fun finally(
        handler: suspend CoroutineScope.(cause: Throwable?) -> Unit
    ): CoroutineBuilder<C, S, T> =
        updateBlock(currentClauses.finally(handler))

    @PublishedApi
    internal fun catchImpl(
        exception: KClass<out Throwable>,
        handler: suspend CoroutineScope.(cause: Throwable) -> T
    ): CoroutineBuilder<C, S, T> =
        updateBlock(currentClauses.catch(exception, handler))

    /**
     * Calls the coroutine builder and return the resulting instance of coroutine.
     */
    public abstract fun build(): C

    private class CatchClause<T>(
        @JvmField val exception: KClass<out Throwable>,
        @JvmField val handler: suspend CoroutineScope.(cause: Throwable) -> T
    )

    private class Clauses<S, T>(
        private val originalBlock: suspend S.() -> T,
        private val catches: List<CatchClause<T>> = emptyList(),
        private val finally: (suspend CoroutineScope.(cause: Throwable?) -> Unit)? = null
    ) {
        fun finally(
            handler: suspend CoroutineScope.(cause: Throwable?) -> Unit
        ): Clauses<S, T> {
            check(finally == null) { "CoroutineBuilder can have at most one 'finally' clause" }
            return Clauses(originalBlock, catches, handler)
        }

        fun catch(
            exception: KClass<out Throwable>,
            handler: suspend CoroutineScope.(cause: Throwable) -> T
        ): Clauses<S, T> {
            check(finally == null) { "CoroutineBuilder cannot have 'catch' after 'finally' clause" }
            return Clauses(originalBlock, catches + CatchClause(exception, handler), finally)
        }

        val block: suspend S.() -> T
            get() = {
                var state = try {
                    originalBlock()
                } catch (e: Throwable) {
                    CompletedExceptionally(e)
                }
                state = awaitFinalStateCompletion(state)
                if (state is CompletedExceptionally) {
                    val cause = state.cause
                    // Only one (first matching) catch block fires
                    catches.find { it.exception.isInstance(cause) }?.let { catch ->
                        // Catch block always replaces result of call
                        state = try {
                            withNonCancellableContext(cause, catch.handler)
                        } catch (e: Throwable) {
                            CompletedExceptionally(e)
                        }
                    }
                }
                if (finally != null) {
                    val cause = (state as? CompletedExceptionally)?.cause
                    // Finally block replaces result of call only on its failure
                    try {
                        withNonCancellableContext(cause, finally)
                    } catch (e: Throwable) {
                        state = CompletedExceptionally(e)
                    }
                }
                (state as? CompletedExceptionally)?.let { throw it.cause }
                @Suppress("UNCHECKED_CAST")
                state as T
            }
    }
}

