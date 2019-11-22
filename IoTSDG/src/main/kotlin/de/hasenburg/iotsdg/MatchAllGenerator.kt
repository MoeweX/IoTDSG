package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils
import units.Time
import units.Time.Unit.MS
import java.io.File
import kotlin.random.Random

private val logger = LogManager.getLogger()

// -------- Brokers --------
val nClients = 100

// -------- Publishers --------
private val minPubTimeGap = Time(1, Time.Unit.S)
private val maxPubTimeGap = Time(5, Time.Unit.S)
private const val topic = "data"
private const val payloadSize = 20 // byte

// -------- Geofences -------- values are in degree
private val geofenceCenter = Location(20.0, 20.0)
private const val geofenceDiameter = 50.0 * DistanceUtils.KM_TO_DEG

// -------- Others  --------
private const val directoryPath = "./match_all"
private val warmupTime = Time(5, Time.Unit.S)
private val timeToRunPerClient = Time(1, Time.Unit.MIN)

fun main() {
    prepareDir(directoryPath)

    val stats = Stats()
    val setup = getSetupString("de.hasenburg.iotsdg.MatchAllGeneratorKt")
    logger.info(setup)
    File("$directoryPath/00_summary.txt").writeText(setup)

    logger.info("Calculating actions")
    val geofence = Geofence.circle(geofenceCenter, geofenceDiameter)

    // loop through clients for broker
    for (c in 1..nClients) {

        if ((100.0 * c / nClients) % 5.0 == 0.0) {
            logger.info("Finished ${100 * c / nClients}%")
        }

        val clientName = randomName()
        logger.debug("Calculating actions for client $clientName")
        val file = File("$directoryPath/$clientName.csv")
        val writer = file.bufferedWriter()

        writer.write(getHeader())

        val timestamp = Time(Random.nextInt(0, warmupTime.i(MS) - 1), MS)
        val location = Location.randomInGeofence(geofence)

        // put location
        writer.write(calculatePingAction(timestamp, location, stats))

        // create subscription
        writer.write(calculateSubscribeActions(Time(timestamp.i(MS) + 1, MS), Location.randomInGeofence(geofence), geofence, stats))

        // publish until end
        writer.write(calculatePublishActions(warmupTime, timeToRunPerClient, location, geofence, stats))

        writer.flush()
        writer.close()
    }

    val timeToRunPerClient = Time(155, Time.Unit.S)
    val output = stats.getSummary(nClients, timeToRunPerClient)

    logger.info(output)
    File("$directoryPath/00_summary.txt").appendText(output)
}

private fun calculatePingAction(timestamp: Time, location: Location, stats: Stats): String {
    stats.addPingMessage()
    return "${timestamp.i(MS)};${location.lat};${location.lon};ping;;;\n"
}

private fun calculateSubscribeActions(timestamp: Time, location: Location, geofence: Geofence, stats: Stats): String {
    val actions = StringBuilder()

    actions.append("${timestamp.i(MS)};${location.lat};${location.lon};subscribe;" + "$topic;${geofence.wktString};\n")
    stats.addSubscribeMessage()

    return actions.toString()
}

private fun calculatePublishActions(startTime: Time, endTime: Time, location: Location, geofence: Geofence,
                                    stats: Stats): String {
    val actions = StringBuilder()

    var timestamp = startTime + Time(Random.nextInt(minPubTimeGap.i(MS), maxPubTimeGap.i(MS)), MS)

    while (endTime > timestamp) {
        actions.append("${timestamp.i(MS)};${location.lat};${location.lon};publish;" + "$topic;${geofence.wktString};$payloadSize\n")
        stats.addPublishMessage()
        stats.addPayloadSize(payloadSize)

        timestamp = Time(timestamp.i(MS) + Random.nextInt(minPubTimeGap.i(MS), maxPubTimeGap.i(MS)), MS)
    }

    return actions.toString()
}