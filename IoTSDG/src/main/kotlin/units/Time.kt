package units

import org.apache.logging.log4j.LogManager
import kotlin.math.pow

private val logger = LogManager.getLogger()

class Time(private val time: Double, private val unit: Unit) {

    constructor(time: Int, unit: Unit) : this(time.toDouble(), unit)

    enum class Unit {
        MS, S, MIN, H
    }

    /**
     * Depicts the factor of [time] related to the internal default unit SECONDS.
     */
    private val factor = when (unit) {
        Unit.MS -> 10.0.pow(-3)
        Unit.S -> 1.0
        Unit.MIN -> 60.0
        Unit.H -> 60.0 * 60.0
    }

    fun d(targetUnit: Unit): Double {
        return when (targetUnit) {
            Unit.MS -> (time * factor * 10.0.pow(3))
            Unit.S -> (time * factor)
            Unit.MIN -> (time * factor / 60.0)
            Unit.H -> (time * factor / 60.0 / 60.0)
        }
    }

    fun i(targetUnit: Unit): Int {
        return d(targetUnit).toInt()
    }

    operator fun plus(otherTime: Time): Time {
        // pick smaller unit as new internal unit
        val newUnit = if (unit < otherTime.unit) unit else otherTime.unit

        val e1 = d(newUnit)
        val e2 = otherTime.d(newUnit)

        return Time(e1 + e2, newUnit)
    }

    operator fun compareTo(otherTime: Time): Int {
        // idea: compare smallest time unit
        return d(Unit.MS).compareTo(otherTime.d(Unit.MS))
    }

    override fun toString(): String {
        return "~${i(Unit.S)}s"
    }

}

fun main() {
    logger.info("Conversions")

    logger.info("11 seconds:")
    printAllUnits(Time(11, Time.Unit.S))
    logger.info("1377 milli-seconds:")
    printAllUnits(Time(1377, Time.Unit.MS))
    logger.info("12 min:")
    printAllUnits(Time(12, Time.Unit.MIN))
    logger.info("12.5 min:")
    printAllUnits(Time(12.5, Time.Unit.MIN))
    logger.info("2 hours:")
    printAllUnits(Time(2, Time.Unit.H))

    logger.info("\nComparision")
    logger.info("11 seconds is smaller than 0.5min: {}", Time(11, Time.Unit.S) < Time(0.5, Time.Unit.MIN))
    logger.info("1 hours is larger than 59min: {}", Time(1, Time.Unit.H) > Time(59, Time.Unit.MIN))
    logger.info("1 hours <= 60min: {}", Time(1, Time.Unit.H) <= Time(60, Time.Unit.MIN))

    logger.info("\nAddition")
    var t1 = Time(2, Time.Unit.S)
    t1 += Time(5, Time.Unit.MS)
    logger.info("2s + 5ms = 2005ms -> {}ms", t1.i(Time.Unit.MS))
}

private fun printAllUnits(time: Time) {
    logger.info("\t{}ms", time.d(Time.Unit.MS))
    logger.info("\t{}s", time.d(Time.Unit.S))
    logger.info("\t{}min", time.d(Time.Unit.MIN))
    logger.info("\t{}h", time.d(Time.Unit.H))
}