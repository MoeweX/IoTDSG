package de.hasenburg.iotsdg.first

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils.KM_TO_DEG
import java.io.File
import de.hasenburg.iotsdg.utility.*
import kotlin.random.Random
import de.hasenburg.iotsdg.stats.Stats

private val logger = LogManager.getLogger()

// -------- Travel --------
private const val minTravelSpeed = 2 // km/h
private const val maxTravelSpeed = 8 // km/h
private const val minTravelTime = 5 // sec
private const val maxTravelTime = 10 // sec

// -------- Brokers --------
private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(-82.999083, 39.961332), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
// to split the workload evenly across multiple machines for a given broker
private val workloadMachinePerBroker = listOf(2, 2, 2)
private val clientsPerBrokerArea = listOf(100, 100, 100)
//private val clientsPerBrokerArea = listOf(1000, 750, 1000)


// -------- Geofences -------- values are in degree
private const val roadConditionSubscriptionGeofenceDiameter = 0.5 * KM_TO_DEG
private const val roadConditionMessageGeofenceDiameter = 0.5 * KM_TO_DEG
private const val minTextBroadcastSubscriptionGeofenceDiameter = 1.0 * KM_TO_DEG
private const val maxTextBroadcastSubscriptionGeofenceDiameter = 50.0 * KM_TO_DEG
private const val minTextBroadcastMessageGeofenceDiameter = 1.0 * KM_TO_DEG
private const val maxTextBroadcastMessageGeofenceDiameter = 50.0 * KM_TO_DEG

// -------- Messages  --------
private const val roadConditionPublicationProbability = 10 // %
private const val textBroadcastPublicationProbability = 50 // %
private const val roadConditionPayloadSize = 100 // byte
private const val minTextBroadcastPayloadSize = 10 // byte
private const val maxTextBroadcastPayloadSize = 1000 // byte

// -------- Others  --------
private const val directoryPath = "./hikingData"
private const val roadConditionTopic = "road"
private const val textBroadcastTopic = "text"
private const val subscriptionRenewalDistance = 50 // m
private const val timeToRunPerClient = 1800 // s


/**
 * Steps:
 *  1. pick broker
 *  2. pick a client -> random name
 *  3. loop:
 *    - calculate next location and timestamp
 *    - send: ping, subscribe, publish
 */
fun main() {
    // pre-check, message geofences do not overlap
    if (checkBrokerOverlap(brokerAreas)) {
        logger.fatal("Brokers should not overlap!")
        System.exit(1)
    }
    // Stats variable to indicate for the scenario
    val stat = Stats()
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

        var currentWorkloadMachine: Int

        // loop through clients for broker
        for (c in 0..broker.third) {
            // determine current workload machine
            if (workloadMachinePerBroker.get(b) == 0) {
                logger.info("Skipping actions for broker ${broker.first} as it does not have any workload machines")
                break
            }
            currentWorkloadMachine = c % workloadMachinePerBroker.get(b)

            if ((100.0 * c / broker.third) % 5.0 == 0.0) {
                logger.info("Finished ${100 * c / broker.third}%")
            }

            val clientName = randomName()
            val clientDirection = Random.nextDouble(0.0, 360.0)
            logger.debug("Calculating actions for client $clientName which travels in $clientDirection")
            var location = Location.randomInGeofence(broker.second)
            var lastUpdatedLocation = location // needed to determine if subscription should be updated
            var timestamp = 0
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_$clientName.csv")
            val writer = file.bufferedWriter()

            // this geofence is only calculated once per client
            val geofenceTB = Geofence.circle(location,
                    Random.nextDouble(minTextBroadcastSubscriptionGeofenceDiameter,
                            maxTextBroadcastSubscriptionGeofenceDiameter))
            writer.write(getHeader())

            // create initial subscriptions
            writer.write(calculateSubscribeActions(timestamp, location, geofenceTB, stat))
            timestamp++ // 1 second of nothing

            // generate actions until time reached
            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePingActions(timestamp, location, stat))
                val travelledDistance = location.distanceKmTo(lastUpdatedLocation) * 1000.0
                if (travelledDistance >= subscriptionRenewalDistance) {
                    logger.debug("Renewing subscription for client $clientName")
                    writer.write(calculateSubscribeActions(timestamp, location, geofenceTB, stat))
                    lastUpdatedLocation = location
                }
                writer.write(calculatePublishActions(timestamp, location, stat))

                val travelTime = Random.nextInt(minTravelTime, maxTravelTime)
                location = calculateNextLocation(broker.second,
                        location,
                        travelTime,
                        clientDirection,
                        minTravelSpeed,
                        maxTravelSpeed,
                        stat)
                timestamp += travelTime
            }

            writer.flush()
            writer.close()
        }
    }
    val output = getOutput(clientsPerBrokerArea, timeToRunPerClient, stat)
    logger.info(output)
    File("$directoryPath/00_summary.txt").appendText(output)
}

private fun getSetupString(): String {
    // there should be another solution in the future: https://stackoverflow.com/questions/33907095/kotlin-how-can-i-use-reflection-on-packages
    val c = Class.forName("de.hasenburg.iotsdg.first.HikingKt")
    val stringBuilder = java.lang.StringBuilder("Setup:\n")

    for (field in c.declaredFields) {
        if (field.name.contains("logger") || field.name.contains("numberOf") || field.name.contains("clientDistanceTravelled") || field.name.contains(
                        "totalPayloadSize")) {

        } else {
            stringBuilder.append("\t").append(field.name).append(": ").append(field.get(c)).append("\n")
        }
    }
    return stringBuilder.toString()

}

private fun calculateSubscribeActions(timestamp: Int, location: Location, geofenceTB: Geofence, stat: Stats): String {
    val actions = StringBuilder()

    // road condition
    val geofenceRC = Geofence.circle(location, roadConditionSubscriptionGeofenceDiameter)
    checkSubscriptionGeofenceBrokerOverlap(geofenceRC, brokerAreas, stat)
    actions.append("${timestamp * 1000 + 1};${location.lat};${location.lon};subscribe;" + "$roadConditionTopic;${geofenceRC.wktString};\n")
    stat.addSubscribeMessages()

    // text broadcast
    actions.append("${timestamp * 1000 + 2};${location.lat};${location.lon};subscribe;" + "$textBroadcastTopic;${geofenceTB.wktString};\n")
    checkSubscriptionGeofenceBrokerOverlap(geofenceTB, brokerAreas, stat)
    stat.addSubscribeMessages()

    return actions.toString()
}

private fun calculatePublishActions(timestamp: Int, location: Location, stat: Stats): String {
    val actions = StringBuilder()

    // road condition
    if (getTrueWithChance(roadConditionPublicationProbability)) {
        val geofenceRC = Geofence.circle(location, roadConditionMessageGeofenceDiameter)
        checkMessageGeofenceBrokerOverlap(geofenceRC, brokerAreas, stat)
        actions.append("${timestamp * 1000 + 3};${location.lat};${location.lon};publish;" + "$roadConditionTopic;${geofenceRC.wktString};$roadConditionPayloadSize\n")
        stat.addPublishMessages()
        stat.addPayloadSize(roadConditionPayloadSize)
    }

    // text broadcast
    if (getTrueWithChance(textBroadcastPublicationProbability)) {
        val geofenceTB = Geofence.circle(location,
                Random.nextDouble(minTextBroadcastMessageGeofenceDiameter, maxTextBroadcastMessageGeofenceDiameter))
        checkMessageGeofenceBrokerOverlap(geofenceTB, brokerAreas, stat)
        val payloadSize = Random.nextInt(minTextBroadcastPayloadSize, maxTextBroadcastPayloadSize)
        actions.append("${timestamp * 1000 + 4};${location.lat};${location.lon};publish;" + "$textBroadcastTopic;${geofenceTB.wktString};$payloadSize\n")
        stat.addPublishMessages()
        stat.addPayloadSize(payloadSize)
    }
    return actions.toString()
}