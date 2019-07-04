package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import java.io.File
import kotlin.random.Random

private val logger = LogManager.getLogger()

/**
 * @return a header for a CSV file
 */
fun getHeader(): String {
    return "timestamp(ms);latitude;longitude;action_type;topic;geofence;payload_size\n"
}

/**
 * @param i - index of the selected broker
 * @return Triple(brokerName, brokerArea, clientsUsingThisBroker)
 */
fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>,
                    clientsPerBrokerArea: List<Int>): Triple<String, Geofence, Int> {
    return Triple(brokerNames[i], brokerAreas[i], clientsPerBrokerArea[i])
}

/**
 * @param i - index of the selected broker
 * @return Triple(brokerName, brokerArea, Pair(subscribersUsingThisBroker, publishersUsingThisBroker))
 */
fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>, subsPerBrokerArea: List<Int>,
                    pubsPerBrokerArea: List<Int>): Triple<String, Geofence, Pair<Int, Int>> {
    return Triple(brokerNames[i], brokerAreas[i], Pair(subsPerBrokerArea[i], pubsPerBrokerArea[i]))
}

fun getCurrentWorkloadMachine(clientIndex: Int, brokerName: String, workloadMachines: Int, clientsAtThisBroker: Int): Int {
    // determine current workload machine
    if (workloadMachines == 0) {
        logger.info("Skipping actions for broker $brokerName as it does not have any workload machines")
        return 0
    }
    val currentWorkloadMachine = clientIndex % workloadMachines

    if ((100.0 * clientIndex / clientsAtThisBroker) % 5.0 == 0.0) {
        logger.info("Finished ${100 * clientIndex / clientsAtThisBroker}%")
    }

    return currentWorkloadMachine
}

/**
 * Counts and stores the number of overlapping subscription geofences.
 *
 * @param stats - [Stats] object storing the results
 */
fun addStat_subscriptionGeofenceOverlaps(geofence: Geofence, brokerAreas: List<Geofence>, stats: Stats) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    stats.addSubscriptionGeofence(intersects)
}

/**
 * Counts and stores the number of overlapping message geofences.
 *
 * @param stats - [Stats] object storing the results
 */
fun addStat_messageGeofenceOverlaps(geofence: Geofence, brokerAreas: List<Geofence>, stats: Stats) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    stats.addMessageGeofence(intersects)
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

/**
 * Calculate a new location based upon
 * TravelDistance, previous location and Direction
 * New Location should also be within the same broker area
 */
fun calculateNextLocation(brokerGeofence: Geofence, location: Location, clientDirection: Double,
                          minTravelDistance: Double, maxTravelDistance: Double, stat: Stats): Location {
    var nextLocation: Location

    var relaxFactor = 1.0

    while (true) {
        // calculate travelled distance
        val distance = Random.nextDouble(minTravelDistance, maxTravelDistance)
        val logger = LogManager.getLogger()
        logger.trace("Travelling for ${distance * 1000}m.")

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
            stat.addClientDistanceTravelled(distance)
            return nextLocation
        }
    }
}

/**
 * Calculates new Location based upon
 * TravelSpeed, previous location and Direction
 * New Location should be within the same broker area
 */
fun calculateNextLocation(brokerGeofence: Geofence, location: Location, travelTime: Int, clientDirection: Double,
                          minTravelSpeed: Int, maxTravelSpeed: Int, stat: Stats): Location {
    var nextLocation: Location

    var relaxFactor = 1.0

    while (true) {
        // calculate travelled distance
        val travelSpeed = Random.nextInt(minTravelSpeed, maxTravelSpeed)
        val distance = travelSpeed * (travelTime / 60.0 / 60.0)
        val logger = LogManager.getLogger()
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
            stat.addClientDistanceTravelled(distance)
            return nextLocation
        }
    }
}

/**
 * Validates that the given [brokerAreas] do not overlap. If they do, kills the program.
 */
fun validateBrokersDoNotOverlap(brokerAreas: List<Geofence>) {
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
}

fun prepareDir(directoryPath: String) {
    val dir = File(directoryPath)
    if (dir.exists()) {
        logger.info("Deleting old content")
        dir.deleteRecursively()
    }
    dir.mkdirs()
}

// TODO add methods with units to Stats object, use these to generate summary

fun getSummary(subsPerBrokerArea: List<Int>, pubsPerBrokerArea: List<Int>, timeToRunPerClient: Int,
               stat: Stats): String {

    val subscribers = subsPerBrokerArea.stream().mapToInt { it }.sum()
    val publishers = pubsPerBrokerArea.stream().mapToInt { it }.sum()
    val clients = subscribers + publishers

    val distancePerClient = stat.getClientDistanceTravelled() / clients
    return """Data set characteristics:
    Number of ping messages: ${stat.getNumberOfPingMessages()} (${stat.getNumberOfPingMessages() / clients} messages/per_subscriber)
    Number of subscribe messages: ${stat.getNumberOfSubscribeMessages()} (${stat.getNumberOfSubscribeMessages() / subscribers} messages/per_subscriber)
    Number of publish messages: ${stat.getNumberOfPublishedMessages()} (${stat.getNumberOfPublishedMessages() / publishers} messages/per_publisher)
    Publish payload size: ${stat.getTotalPayloadSize() / 1000.0} KB (${stat.getTotalPayloadSize() / stat.getNumberOfPublishedMessages()}
    byte/message)
    Client distance travelled: ${stat.getClientDistanceTravelled()}km ($distancePerClient km/client)
    Client average speed: ${distancePerClient / timeToRunPerClient * 3600} km/h
    Number of message geofence broker overlaps: ${stat.getNumberOfOverlappingMessageGeofences()}
    Number of subscription geofence broker overlaps: ${stat.getNumberOfOverlappingSubscriptionGeofences()}"""
}

fun getSummary(clientsPerBrokerArea: List<Int>, timeToRunPerClient: Int, stat: Stats): String {
    val distancePerClient = stat.getClientDistanceTravelled() / clientsPerBrokerArea.stream().mapToInt { it }.sum()
    return """Data set characteristics:
    Number of ping messages: ${stat.getNumberOfPingMessages()} (${stat.getNumberOfPingMessages() / timeToRunPerClient}
    messages/s)
    Number of subscribe messages: ${stat.getNumberOfSubscribeMessages()} (${stat.getNumberOfSubscribeMessages() / timeToRunPerClient}
    messages/s)
    Number of publish messages: ${stat.getNumberOfPublishedMessages()} (${stat.getNumberOfPublishedMessages() / timeToRunPerClient}
    messages/s)
    Publish payload size: ${stat.getTotalPayloadSize() / 1000.0} KB (${stat.getTotalPayloadSize() / stat.getNumberOfPublishedMessages()}
    byte/message)
    Client distance travelled: ${stat.getClientDistanceTravelled()}km ($distancePerClient km/client)
    Client average speed: ${distancePerClient / timeToRunPerClient * 3600} km/h
    Number of subscription geofence broker overlaps: ${stat.getNumberOfOverlappingSubscriptionGeofences()}
    Number of message geofence broker overlaps: ${stat.getNumberOfOverlappingMessageGeofences()}"""
}