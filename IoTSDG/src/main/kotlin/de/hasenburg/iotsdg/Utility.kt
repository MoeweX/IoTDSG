package de.hasenburg.iotsdg

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import org.apache.logging.log4j.LogManager
import units.Distance
import units.Distance.Unit.*
import units.Time
import units.Time.Unit.*
import java.io.File
import kotlin.random.Random

private val logger = LogManager.getLogger()

/*****************************************************************
 * Preparation
 ****************************************************************/

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

fun getSetupString(className: String): String {
    // there should be another solution in the future: https://stackoverflow.com/questions/33907095/kotlin-how-can-i-use-reflection-on-packages
    val c = Class.forName(className)
    val stringBuilder = java.lang.StringBuilder("Setup:\n")

    for (field in c.declaredFields) {
        field.isAccessible = true
        if (field.name.contains("logger")) {
            // do not use variable
        } else {
            stringBuilder.append("\t").append(field.name).append(": ").append(field.get(c)).append("\n")
        }
    }
    return stringBuilder.toString()
}

/**
 * @return a header for a CSV file
 */
fun getHeader(): String {
    return "timestamp(ms);latitude;longitude;action_type;topic;geofence;payload_size\n"
}

/*****************************************************************
 * During generation
 ****************************************************************/

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

fun getCurrentWorkloadMachine(clientIndex: Int, brokerName: String, workloadMachines: Int,
                              clientsAtThisBroker: Int): Int {
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
 * Calculates the next [Location] and adds the corresponding travel distance to [stats].
 *
 * @return the next [Location]
 */
fun calculateNextLocation(brokerGeofence: Geofence, location: Location, clientDirection: Double,
                          minTravelDistance: Distance, maxTravelDistance: Distance, stats: Stats): Location {
    // calculate travelled distance
    val distance = Distance(Random.nextDouble(minTravelDistance.d(M), maxTravelDistance.d(M)), M)
    logger.trace("Travelling for ${distance.d(M)}m.")
    return calculateNextLocation(brokerGeofence, location, clientDirection, distance, stats)
}

/**
 * Calculates the next [Location] and adds the corresponding travel distance to [stats].
 *
 * @return the next [Location]
 */
fun calculateNextLocation(brokerGeofence: Geofence, location: Location, clientDirection: Double, travelTime: Time,
                          minTravelSpeed: Int, maxTravelSpeed: Int, stats: Stats): Location {

    val travelSpeed = Random.nextInt(minTravelSpeed, maxTravelSpeed) // km/h
    val distance = Distance(travelSpeed * travelTime.d(H), KM) // km
    logger.trace("Travelling with $travelSpeed km/h for ${travelTime.d(S)} seconds which leads to ${distance.d(M)}m.")

    return calculateNextLocation(brokerGeofence, location, clientDirection, distance, stats)
}

private fun calculateNextLocation(brokerGeofence: Geofence, location: Location, clientDirection: Double,
                                  travelDistance: Distance, stats: Stats): Location {
    var nextLocation: Location
    var relaxFactor = 1.0

    while (true) {
        // choose a direction (roughly in the direction of the client
        val direction = Random.nextDouble(clientDirection - 10.0, clientDirection + 10.0)

        nextLocation = Location.locationInDistance(location, travelDistance.d(KM), direction)

        // in case we are at the edge of a geofence, we need to relax it a little bit otherwise this will be an
        // infinite loop
        relaxFactor += 1.0

        if (relaxFactor > 30) {
            // let's go back by 180 degree
            nextLocation = Location.locationInDistance(location, travelDistance.d(KM), direction + 180.0)
        } else if (relaxFactor > 32) {
            logger.warn("Location $location cannot be used to find another location.")
            return location
        }

        // only stop when we found the next location
        if (brokerGeofence.contains(nextLocation)) {
            stats.addClientDistanceTravelled(travelDistance)
            return nextLocation
        }
    }

}
