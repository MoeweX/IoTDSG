package de.hasenburg.iotsdg.hiking

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.iotsdg.openData.getHeader
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils.KM_TO_DEG
import java.io.File
import kotlin.random.Random

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

// -------- Stats  --------
private var numberOfPingMessages = 0
private var numberOfPublishedMessages = 0
private var numberOfSubscribeMessages = 0
private var clientDistanceTravelled = 0.0 // in km
private var totalPayloadSize = 0 // byte
private var numberOfOverlappingSubscriptionGeofences = 0
private var numberOfOverlappingMessageGeofences = 0

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
        val broker = getBrokerTriple(b)

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
            writer.write(calculateSubscribeActions(timestamp, location, geofenceTB))
            timestamp++ // 1 second of nothing

            // generate actions until time reached
            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePingActions(timestamp, location))
                val travelledDistance = location.distanceKmTo(lastUpdatedLocation) * 1000.0
                if (travelledDistance >= subscriptionRenewalDistance) {
                    logger.debug("Renewing subscription for client $clientName")
                    writer.write(calculateSubscribeActions(timestamp, location, geofenceTB))
                    lastUpdatedLocation = location
                }
                writer.write(calculatePublishActions(timestamp, location))

                val travelTime = Random.nextInt(minTravelTime, maxTravelTime)
                location = calculateNextLocation(broker.second, location, travelTime, clientDirection)
                timestamp += travelTime
            }

            writer.flush()
            writer.close()
        }
    }

    val distancePerClient = clientDistanceTravelled / clientsPerBrokerArea.stream().mapToInt { it }.sum()
    val output = """Data set characteristics:
    Number of ping messages: $numberOfPingMessages (${numberOfPingMessages / timeToRunPerClient} messages/s)
    Number of subscribe messages: $numberOfSubscribeMessages (${numberOfSubscribeMessages / timeToRunPerClient} messages/s)
    Number of publish messages: $numberOfPublishedMessages (${numberOfPublishedMessages / timeToRunPerClient} messages/s)
    Publish payload size: ${totalPayloadSize / 1000.0} KB (${totalPayloadSize / numberOfPublishedMessages} byte/message)
    Client distance travelled: ${clientDistanceTravelled}km ($distancePerClient km/client)
    Client average speed: ${distancePerClient / timeToRunPerClient * 3600} km/h
    Number of subscription geofence broker overlaps: $numberOfOverlappingSubscriptionGeofences
    Number of message geofence broker overlaps: $numberOfOverlappingMessageGeofences"""

    logger.info(output)
    File("$directoryPath/00_summary.txt").appendText(output)
}

fun getSetupString(): String {
    // there should be another solution in the future: https://stackoverflow.com/questions/33907095/kotlin-how-can-i-use-reflection-on-packages
    val c = Class.forName("de.hasenburg.iotsdg.hiking.HikingGeneratorKt")
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

fun checkSubscriptionGeofenceBrokerOverlap(geofence: Geofence) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    numberOfOverlappingSubscriptionGeofences += intersects
}

fun checkMessageGeofenceBrokerOverlap(geofence: Geofence) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    numberOfOverlappingMessageGeofences += intersects
}

/**
 * @param travelTime - in seconds
 * @param clientDirection - the general direction, deviates by +/- 10
 */
fun calculateNextLocation(brokerGeofence: Geofence, location: Location, travelTime: Int,
                          clientDirection: Double): Location {
    var nextLocation: Location

    var relaxFactor = 1.0

    while (true) {
        // calculate travelled distance
        val travelSpeed = Random.nextInt(minTravelSpeed, maxTravelSpeed)
        val distance = travelSpeed * (travelTime / 60.0 / 60.0)
        logger.trace("Travelling with $travelSpeed km/h for $travelTime seconds which leads to ${distance * 1000}m.")

        // choose a direction (roughly in the direction of the client
        val direction = Random.nextDouble(clientDirection - 10.0, clientDirection + 10.0)

        nextLocation = Location.locationInDistance(location, distance, direction)

        // in case we are at the edge of a geofence, we need to relax it a little bit otherwise this will be an
        // infinite loop
        relaxFactor += 1.0

        if (relaxFactor > 30) {
            // let's go back by 180 degree
            nextLocation = Location.locationInDistance(location, distance, direction + 180.0)
        } else if (relaxFactor > 32) {
            logger.warn("Location $location cannot be used to find another location.")
            return location
        }

        // only stop when we found the next location
        if (brokerGeofence.contains(nextLocation)) {
            clientDistanceTravelled += distance
            return nextLocation
        }
    }
}


fun getBrokerTriple(i: Int): Triple<String, Geofence, Int> {
    return Triple(brokerNames[i], brokerAreas[i], clientsPerBrokerArea[i])
}

fun getHeader(): String {
    return "timestamp(ms);latitude;longitude;action_type;topic;geofence;payload_size\n"
}

fun calculatePingActions(timestamp: Int, location: Location): String {
    numberOfPingMessages++
    return "${timestamp * 1000};${location.lat};${location.lon};ping;;;\n"
}

fun calculateSubscribeActions(timestamp: Int, location: Location, geofenceTB: Geofence): String {
    val actions = StringBuilder()

    // road condition
    val geofenceRC = Geofence.circle(location, roadConditionSubscriptionGeofenceDiameter)
    checkSubscriptionGeofenceBrokerOverlap(geofenceRC)
    actions.append("${timestamp * 1000 + 1};${location.lat};${location.lon};subscribe;" + "$roadConditionTopic;${geofenceRC.wktString};\n")
    numberOfSubscribeMessages++

    // text broadcast
    actions.append("${timestamp * 1000 + 2};${location.lat};${location.lon};subscribe;" + "$textBroadcastTopic;${geofenceTB.wktString};\n")
    checkSubscriptionGeofenceBrokerOverlap(geofenceTB)
    numberOfSubscribeMessages++

    return actions.toString()
}

fun calculatePublishActions(timestamp: Int, location: Location): String {
    val actions = StringBuilder()

    // road condition
    if (getTrueWithChance(roadConditionPublicationProbability)) {
        val geofenceRC = Geofence.circle(location, roadConditionMessageGeofenceDiameter)
        checkMessageGeofenceBrokerOverlap(geofenceRC)
        actions.append("${timestamp * 1000 + 3};${location.lat};${location.lon};publish;" + "$roadConditionTopic;${geofenceRC.wktString};$roadConditionPayloadSize\n")
        numberOfPublishedMessages++
        totalPayloadSize += roadConditionPayloadSize
    }

    // text broadcast
    if (getTrueWithChance(textBroadcastPublicationProbability)) {
        val geofenceTB = Geofence.circle(location,
                Random.nextDouble(minTextBroadcastMessageGeofenceDiameter, maxTextBroadcastMessageGeofenceDiameter))
        checkMessageGeofenceBrokerOverlap(geofenceTB)
        val payloadSize = Random.nextInt(minTextBroadcastPayloadSize, maxTextBroadcastPayloadSize)
        actions.append("${timestamp * 1000 + 4};${location.lat};${location.lon};publish;" + "$textBroadcastTopic;${geofenceTB.wktString};$payloadSize\n")
        totalPayloadSize += payloadSize
        numberOfPublishedMessages++
    }
    return actions.toString()
}


/**
 * Returns true with the given chance.
 *
 * @param chance - the chance to return true (0 - 100)
 * @return true, if lucky
 */
fun getTrueWithChance(chance: Int): Boolean {
    @Suppress("NAME_SHADOWING") var chance = chance
    // normalize
    if (chance > 100) {
        chance = 100
    } else if (chance < 0) {
        chance = 0
    }
    val random = Random.nextInt(100) + 1 // not 0
    return random <= chance
}