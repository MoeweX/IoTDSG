package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.iotsdg.utility.*
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils
import java.io.File
import kotlin.random.Random

private val logger = LogManager.getLogger()

// -------- Brokers --------
private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(-82.999083, 39.961332), 5.0),
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

// -------- Stats  --------
private var numberOfPingMessages = 0
private var numberOfPublishedMessages = 0
private var numberOfSubscribeMessages = 0
private var totalPayloadSize = 0 // byte
private var numberOfOverlappingSubscriptionGeofences = 0
private var numberOfOverlappingMessageGeofences = 0

fun main() {
    // pre-check, message geofences do not overlap
    for (ba in brokerAreas) {
        var numberOfOverlaps = 0
        for (baI in brokerAreas) {
            if (ba.intersects(baI)) {
                numberOfOverlaps++
            }
        }

        if (numberOfOverlaps > 1) {
            logger.fatal("Brokers should not overlap!")
            System.exit(1)
        }
    }

    // make sure dir exists, delete old content
    val dir = File(directoryPath)
    if (dir.exists()) {
        logger.info("Deleting old content")
        dir.deleteRecursively()
    }
    dir.mkdirs()

    val setup = getSetupString()
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
            writer.write(calculatePingActions(Random.nextInt(0, 5000), location))

            // subscribe (10000 - 15000 ms)
            writer.write(calculateSubscribeActions(Random.nextInt(10000, 15000), location))

            // 5 publish (20000 - 55000 ms)
            writer.write(calculatePublishActions(5, 20000, 55000, location))

            // ping (60000 - 65000 ms)
            location = Location.randomInGeofence(broker.second)
            writer.write(calculatePingActions(Random.nextInt(60000, 65000), location))
            // WARNING: I AM NOT INSIDE MY OWN SUBSCRIPTION GEOFENCE ANYMORE! -> messages might not be delivered to anyone

            // 5 publish (70000 - 105000 ms)
            writer.write(calculatePublishActions(5, 70000, 105000, location))

            // subscribe (110000 - 115000 ms)
            writer.write(calculateSubscribeActions(Random.nextInt(110000, 115000), location))

            // 5 publish (120000 - 155000 ms)
            writer.write(calculatePublishActions(5, 120000, 155000, location))

            writer.flush()
            writer.close()
        }
    }

    val timeToRunPerClient = 155

    val output = """Data set characteristics:
    Number of ping messages: $numberOfPingMessages (${numberOfPingMessages / timeToRunPerClient} messages/s)
    Number of subscribe messages: $numberOfSubscribeMessages (${numberOfSubscribeMessages / timeToRunPerClient} messages/s)
    Number of publish messages: $numberOfPublishedMessages (${numberOfPublishedMessages / timeToRunPerClient} messages/s)
    Publish payload size: ${totalPayloadSize / 1000.0} KB (${totalPayloadSize / numberOfPublishedMessages} byte/message)
    Number of subscription geofence broker overlaps: $numberOfOverlappingSubscriptionGeofences
    Number of message geofence broker overlaps: $numberOfOverlappingMessageGeofences"""

    logger.info(output)
    File("$directoryPath/00_summary.txt").appendText(output)
}

private fun getSetupString(): String {
    // there should be another solution in the future: https://stackoverflow.com/questions/33907095/kotlin-how-can-i-use-reflection-on-packages
    val c = Class.forName("de.hasenburg.iotsdg.ValidationDataGeneratorKt")
    val stringBuilder = java.lang.StringBuilder("Setup:\n")

    for (field in c.declaredFields) {
        if (field.name.contains("logger") || field.name.contains("numberOf") || field.name.contains("clientDistanceTravelled") || field.name.contains(
                        "totalPayloadSize")) {
            // pass
        } else {
            stringBuilder.append("\t").append(field.name).append(": ").append(field.get(c)).append("\n")
        }
    }
    return stringBuilder.toString()
}

private fun calculatePingActions(timestamp: Int, location: Location): String {
    numberOfPingMessages++
    return "$timestamp;${location.lat};${location.lon};ping;;;\n"

}

fun calculateMessageOverlaps(geofence: Geofence, brokerAreas: List<Geofence>): Int {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    return intersects
}

private fun calculateSubscribeActions(timestamp: Int, location: Location): String {
    val actions = StringBuilder()

    val geofence = Geofence.circle(location, subscriptionGeofenceDiameter)
    numberOfOverlappingSubscriptionGeofences += calculateMessageOverlaps(geofence, brokerAreas)
    actions.append("$timestamp;${location.lat};${location.lon};subscribe;" + "$topic;${geofence.wktString};\n")
    numberOfSubscribeMessages++

    return actions.toString()
}

private fun calculatePublishActions(@Suppress("SameParameterValue") count: Int, startTime: Int, endTime: Int, location: Location): String {
    val actions = StringBuilder()

    val gap = (endTime - startTime) / count
    val geofence = Geofence.circle(location, messageGeofenceDiameter)

    for (i in 0..count-1) {
        val start = startTime + i * gap
        val end = startTime + (i+1) * gap
        val timestamp = Random.nextInt(start, end)

        numberOfOverlappingMessageGeofences += calculateMessageOverlaps(geofence, brokerAreas)
        actions.append("$timestamp;${location.lat};${location.lon};publish;" + "$topic;${geofence.wktString};$payloadSize\n")
        numberOfPublishedMessages++
        totalPayloadSize += payloadSize
    }

    return actions.toString()
}