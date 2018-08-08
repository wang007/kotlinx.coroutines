package kotlinx.coroutines.experimental.channels_new

suspend inline fun <R> select(crossinline builder: SelectBuilder<R>.() -> Unit): R {
    val select = SelectInstance<R>()
    builder(select)
    return select.select()
}

suspend inline fun <R> selectUnbiased(crossinline builder: SelectBuilder<R>.() -> Unit): R {
    val select = SelectInstance<R>()
    builder(select)
//    select.shuffleAlternatives()
    return select.select()
}

interface SelectBuilder<RESULT> {
    operator fun <FUNC_RESULT> Param0RegInfo<FUNC_RESULT>.invoke(block: (FUNC_RESULT) -> RESULT)

    operator fun <PARAM, FUNC_RESULT> Param1RegInfo<FUNC_RESULT>.invoke(param: PARAM, block: (FUNC_RESULT) -> RESULT)
    operator fun <FUNC_RESULT> Param1RegInfo<FUNC_RESULT>.invoke(block: (FUNC_RESULT) -> RESULT) = invoke(null, block)
}

// channel, selectInstance, element -> is selected by this alternative
typealias RegFunc = Function3<Channel<*>, SelectInstance<*>, Any?, RegResult?>

class Param0RegInfo<FUNC_RESULT>(
        val channel: Any,
        val regFunc: RegFunc,
        val actFunc: ActFunc<FUNC_RESULT>
)

class Param1RegInfo<FUNC_RESULT>(
        val channel: Any,
        val regFunc: RegFunc,
        val actFunc: ActFunc<FUNC_RESULT>
)

class RegResult(val cleanable: Cleanable, val index: Int)

// continuation, result (usually a received element), block
typealias ActFunc<FUNC_RESULT> = Function2<Any?, Function1<FUNC_RESULT, Any?>, Any?>

class SelectInstance<RESULT> : SelectBuilder<RESULT> {
    override fun <FUNC_RESULT> Param0RegInfo<FUNC_RESULT>.invoke(block: (FUNC_RESULT) -> RESULT) {
        TODO("not implemented")
    }

    override fun <PARAM, FUNC_RESULT> Param1RegInfo<FUNC_RESULT>.invoke(param: PARAM, block: (FUNC_RESULT) -> RESULT) {
        TODO("not implemented")
    }

    suspend fun select(): RESULT {
        TODO()
    }
}