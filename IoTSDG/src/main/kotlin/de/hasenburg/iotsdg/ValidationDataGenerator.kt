package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils
import units.Time
import java.io.File
import kotlin.random.Random

private val logger = LogManager.getLogger()

// -------- Brokers --------
private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(39.961332, -82.999083), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
private val clientsPerBrokerArea = listOf(200, 200, 200)

// -------- Geofences -------- values are in degree
private const val subscriptionGeofenceDiameter = 50.0 * DistanceUtils.KM_TO_DEG
private const val messageGeofenceDiameter = 50.0 * DistanceUtils.KM_TO_DEG

// -------- Others  --------
private const val directoryPath = "./validationData"
private const val topic = "data"
private const val payloadSize = 20 // byte

fun main() {
    validateBrokersDoNotOverlap(brokerAreas)
    prepareDir(directoryPath)

    val stats = Stats()
    val setup = getSetupString("de.hasenburg.iotsdg.ValidationDataGeneratorKt")
    logger.info(setup)
    File("$directoryPath/00_summary.txt").writeText(setup)

    for (b in 0..2) {
        // pick a broker
        val broker = getBrokerTriple(b, brokerNames, brokerAreas, clientsPerBrokerArea)

        logger.info("Calculating actions for broker ${broker.first}")

        // loop through clients for broker
        for (c in 1..broker.third) {

            if ((100.0 * c / broker.third) % 5.0 == 0.0) {
                logger.info("Finished ${100 * c / broker.third}%")
            }

            val clientName = randomName()
            logger.debug("Calculating actions for client $clientName")
            val file = File("$directoryPath/${broker.first}-0_$clientName.csv")
            val writer = file.bufferedWriter()

            writer.write(getHeader())

            // ping (0 - 5000 ms)
            var location = Location.randomInGeofence(broker.second)
            writer.write(calculatePingAction(Random.nextInt(0, 5000), location, stats))

            // subscribe (10000 - 15000 ms)
            writer.write(calculateSubscribeActions(Random.nextInt(10000, 15000), location, stats))

            // 5 publish (20000 - 55000 ms)
            writer.write(calculatePublishActions(5, 20000, 55000, location, stats))

            // ping (60000 - 65000 ms)
            location = Location.randomInGeofence(broker.second)
            writer.write(calculatePingAction(Random.nextInt(60000, 65000), location, stats))
            // WARNING: I AM NOT INSIDE MY OWN SUBSCRIPTION GEOFENCE ANYMORE! -> messages might not be delivered to anyone

            // 5 publish (70000 - 105000 ms)
            writer.write(calculatePublishActions(5, 70000, 105000, location, stats))

            // subscribe (110000 - 115000 ms)
            writer.write(calculateSubscribeActions(Random.nextInt(110000, 115000), location, stats))

            // 5 publish (120000 - 155000 ms)
            writer.write(calculatePublishActions(5, 120000, 155000, location, stats))

            // final ping as last message
            writer.write(calculatePingAction(155000, location, stats))

            writer.flush()
            writer.close()
        }
    }

    val timeToRunPerClient = Time(155, Time.Unit.S)
    val output = stats.getSummary(clientsPerBrokerArea, timeToRunPerClient)

    logger.info(output)
    File("$directoryPath/00_summary.txt").appendText(output)
}

private fun calculatePingAction(timestamp: Int, location: Location, stats: Stats): String {
    stats.addPingMessage()
    return "$timestamp;${location.lat};${location.lon};ping;;;\n"

}

private fun calculateSubscribeActions(timestamp: Int, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    val geofence = Geofence.circle(location, subscriptionGeofenceDiameter)
    actions.append("$timestamp;${location.lat};${location.lon};subscribe;" + "$topic;${geofence.wktString};\n")
    stats.addSubscriptionGeofenceOverlaps(geofence, brokerAreas)
    stats.addSubscribeMessage()

    return actions.toString()
}

private fun calculatePublishActions(@Suppress("SameParameterValue") count: Int, startTime: Int, endTime: Int,
                                    location: Location, stats: Stats): String {
    val actions = StringBuilder()

    val gap = (endTime - startTime) / count
    val geofence = Geofence.circle(location, messageGeofenceDiameter)

    for (i in 0 until count) {
        val start = startTime + i * gap
        val end = startTime + (i + 1) * gap
        val timestamp = Random.nextInt(start, end)

        actions.append("$timestamp;${location.lat};${location.lon};publish;" + "$topic;${geofence.wktString};$payloadSize\n")
        stats.addMessageGeofenceOverlaps(geofence, brokerAreas)
        stats.addPublishMessage()
        stats.addPayloadSize(payloadSize)
    }

    return actions.toString()
}