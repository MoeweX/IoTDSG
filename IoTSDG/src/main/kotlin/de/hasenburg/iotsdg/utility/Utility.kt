package de.hasenburg.iotsdg.utility

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import kotlin.random.Random
import org.apache.logging.log4j.LogManager
import de.hasenburg.iotsdg.stats.Stats

// Stats
private var numberOfPingMessages = 0
private var clientDistanceTravelled = 0.0
private var numberOfPublishedMessages = 0
private var totalPayloadSize = 0
private var numberOfOverlappingSubscriptionGeofences = 0
private var numberOfSubscribeMessages = 0
private var numberOfOverlappingMessageGeofences = 0

//returns the title row
fun getHeader(): String {
    return "timestamp(ms);latitude;longitude;action_type;topic;geofence;payload_size\n"
}

// returns a triple of brokername, broker area and clients in the area
fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>,
                    clientsPerBrokerArea: List<Int>): Triple<String, Geofence, Int> {
    return Triple(brokerNames[i], brokerAreas[i], clientsPerBrokerArea[i])
}

//returns a triple of brokernam, broker area, subscribers and publishers in the area
fun getBrokerTriple(i: Int, brokerNames: List<String>, brokerAreas: List<Geofence>, subsPerBrokerArea: List<Int>,
                    pubsPerBrokerArea: List<Int>): Triple<String, Geofence, Pair<Int, Int>> {
    return Triple(brokerNames[i], brokerAreas[i], Pair(subsPerBrokerArea[i], pubsPerBrokerArea[i]))
}

// count no. of subscription overlaps
fun checkSubscriptionGeofenceBrokerOverlap(geofence: Geofence, brokerAreas: List<Geofence>, stat: Stats) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    stat.addSubscriptionGeofence(intersects)
}

// count no. of message overlaps
fun checkMessageGeofenceBrokerOverlap(geofence: Geofence, brokerAreas: List<Geofence>, stat: Stats) {
    var intersects = -1 // own broker
    brokerAreas.forEach {
        if (geofence.intersects(it)) {
            intersects++
        }
    }
    stat.addMessageGeofence(intersects)
}

fun calculatePingActions(timestamp: Int, location: Location, stat: Stats): String {
    stat.addPingMessages()
    return "$timestamp;${location.lat};${location.lon};ping;;;\n"
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

// checking overlapping brokerareas
fun checkBrokerOverlap(brokerAreas: List<Geofence>): Boolean {
    for (ba in brokerAreas) {
        var numberOfOverlaps = 0
        for (baI in brokerAreas) {
            if (ba.intersects(baI)) {
                numberOfOverlaps++
            }
        }

        if (numberOfOverlaps > 1) {
            return true
        }
    }
    return false
}


fun getOutput(brokerNames: List<String>, subsPerBrokerArea: List<Int>, pubsPerBrokerArea: List<Int>,
              timeToRunPerClient: Int, stat: Stats): String {

    val distancePerClient = stat.getClientDistanceTravelled() / (subsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)
    return """Data set characteristics:
    Number of ping messages: ${stat.getNumberOfPingMessages()} (${stat.getNumberOfPingMessages() / (subsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)} messages/per_subscriber)
    Number of subscribe messages: ${stat.getNumberOfSubscribeMessages()} (${stat.getNumberOfSubscribeMessages() / (subsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)} messages/per_subscriber)
    Number of publish messages: ${stat.getNumberOfPublishedMessages()} (${stat.getNumberOfPublishedMessages() / (pubsPerBrokerArea.stream().mapToInt { it }.sum() + brokerNames.size)} messages/per_publisher)
    Publish payload size: ${stat.getTotalPayloadSize() / 1000.0} KB (${stat.getTotalPayloadSize() / stat.getNumberOfPublishedMessages()}
    byte/message)
    Client distance travelled: ${stat.getClientDistanceTravelled()}km ($distancePerClient km/client)
    Client average speed: ${distancePerClient / timeToRunPerClient * 3600} km/h
    Number of message geofence broker overlaps: ${stat.getNumberOfOverlappingMessageGeofences()}
    Number of subscription geofence broker overlaps: ${stat.getNumberOfOverlappingSubscriptionGeofences()}"""
}

fun getOutput(clientsPerBrokerArea: List<Int>, timeToRunPerClient: Int, stat: Stats): String {
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