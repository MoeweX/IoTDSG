package units

import org.apache.logging.log4j.LogManager

private val logger = LogManager.getLogger()

class Distance(private val distance: Double, private val unit: Unit) {

    constructor(distance: Int, unit: Unit) : this(distance.toDouble(), unit)

    enum class Unit {
        M, KM
    }

    /**
     * Depicts the factor of [distance] related to the internal default unit METER.
     */
    private val factor = when (unit) {
        Unit.M -> 1.0
        Unit.KM -> 1000.0
    }

    fun d(targetUnit: Unit): Double {
        return when (targetUnit) {
            Unit.M -> (distance * factor)
            Unit.KM -> (distance * factor / 1000.0)
        }
    }

    fun i(targetUnit: Unit): Int {
        return d(targetUnit).toInt()
    }

    operator fun plus(otherDistance: Distance): Distance {
        // pick smaller unit as new internal unit
        val newUnit = if (unit < otherDistance.unit) unit else otherDistance.unit

        val e1 = d(newUnit)
        val e2 = otherDistance.d(newUnit)

        return Distance(e1 + e2, newUnit)
    }

    operator fun compareTo(otherDistance: Distance): Int {
        // idea: compare smallest time unit
        return d(Unit.M).compareTo(otherDistance.d(Unit.M))
    }

    override fun toString(): String {
        return "~${i(Unit.M)}m"
    }

}

fun main() {
    logger.info("100 meters:")
    printAllUnits(Distance(100, Distance.Unit.M))
    logger.info("3400 meters:")
    printAllUnits(Distance(3400, Distance.Unit.M))
    logger.info("2.7 kilometers:")
    printAllUnits(Distance(2.7, Distance.Unit.KM))
    logger.info("11 kilometers:")
    printAllUnits(Distance(11, Distance.Unit.KM))

    logger.info("\nComparision")
    logger.info("100 meters is smaller than 0.4km: {}",
            Distance(100, Distance.Unit.M) < Distance(0.4, Distance.Unit.KM))
    logger.info("1km is larger than 999 meters: {}", Distance(1, Distance.Unit.KM) > Distance(999, Distance.Unit.M))

    logger.info("\nAddition")
    var d1 = Distance(2, Distance.Unit.KM)
    d1 += Distance(5, Distance.Unit.M)
    logger.info("2km + 5m = 2005m -> {}m", d1.i(Distance.Unit.M))
}

private fun printAllUnits(distance: Distance) {
    logger.info("\t{}m", distance.d(Distance.Unit.M))
    logger.info("\t{}km", distance.d(Distance.Unit.KM))
}