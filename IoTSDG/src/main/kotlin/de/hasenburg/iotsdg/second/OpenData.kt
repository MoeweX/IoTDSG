package de.hasenburg.iotsdg.second

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.iotsdg.*
import org.locationtech.spatial4j.distance.DistanceUtils
import java.io.File
import org.apache.logging.log4j.LogManager
import kotlin.random.Random
import de.hasenburg.iotsdg.Stats
import units.Distance
import units.Distance.Unit.*
import units.Time
import units.Time.Unit.*

private val logger = LogManager.getLogger()

// -------- Brokers --------
private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(-82.999083, 39.961332), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
private val workloadMachinesPerBroker = listOf(3, 3, 3)
private val subsPerBrokerArea = listOf(150, 150, 150)
private val pubsPerBrokerArea = listOf(600, 600, 600)

// -------- Subscribers --------
private val minTravelDistance = Distance(500, M)
private val maxTravelDistance = Distance(100, KM)
private val subscriberMobilityCheckTime = Time(5, S) // TODO add a range here to prevent bulk subscriptions
private const val mobilityProbability = 10 // %

// -------- Subscription Geofences -------- values are in degree
private const val minTemperatureSubscriptionGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxTemperatureSubscriptionGeofenceDiameter = 100.0 * DistanceUtils.KM_TO_DEG
private const val minHumiditySubscriptionGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxHumiditySubscriptionGeofenceDiameter = 100.0 * DistanceUtils.KM_TO_DEG
private const val minBarometricSubscriptionGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxBarometricSubscriptionGeofenceDiameter = 100.0 * DistanceUtils.KM_TO_DEG

// -------- Publishers --------
private val minPubTimeGap = Time(5, S)
private val maxPubTimeGap = Time(15, S)

// -------- Messages --------
private const val temperatureTopic = "temperature"
private const val humidityTopic = "humidity"
private const val barometricPressureTopic = "barometric_pressure"
private const val temperaturePayloadSize = 100
private const val minHumidityPayloadSize = 130
private const val maxHumidityPayloadSize = 180
private const val minBarometerPayloadSize = 210
private const val maxBarometerPayloadSize = 240

// -------- Others  --------
private const val directoryPath = "./environmentaldata"
private val warmupTime = Time(5, S)
private val timeToRunPerClient = Time(30, MIN)


fun main() {
    validateBrokersDoNotOverlap(brokerAreas)
    prepareDir(directoryPath)

    val stats = Stats()
    val setup = getSetupString("de.hasenburg.iotsdg.second.OpenDataKt")
    logger.info(setup)
    File("$directoryPath/01_summary.txt").writeText(setup)

    for (b in 0..2) { // for sensors/publishers

        val broker = getBrokerTriple(b, brokerNames, brokerAreas, subsPerBrokerArea, pubsPerBrokerArea)
        var currentWorkloadMachine: Int

        logger.info("Calculating publisher actions for broker ${broker.first}")
        // loop through publishers for broker
        for (pub in 1..broker.third.second) {
            currentWorkloadMachine =
                    getCurrentWorkloadMachine(pub, broker.first, workloadMachinesPerBroker[b], broker.third.second)
            val clientName = randomName()
            logger.debug("Calculating actions for publisher $clientName")

            // file and writer
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_Pub_$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())

            // vars
            val location = Location.randomInGeofence(broker.second)
            var timestamp = Time(Random.nextInt(0, warmupTime.i(MS)), MS)

            // write fixed location of publisher
            writer.write(calculatePingAction(timestamp, location, stats))
            timestamp = warmupTime

            // generate actions until time reached
            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePublishActions(timestamp, location, stats))
                timestamp += Time(Random.nextInt(minPubTimeGap.i(MS), maxPubTimeGap.i(MS)), MS)
            }

            // add a last ping message at runtime, as "last message"
            writer.write(calculatePingAction(timeToRunPerClient, location, stats))

            writer.flush()
            writer.close()
        }

        logger.info("Calculating subscriber actions for broker ${broker.first}")
        // loop through subscribers for broker
        for (sub in 1..broker.third.first) { // for subscribers
            currentWorkloadMachine =
                    getCurrentWorkloadMachine(sub, broker.first, workloadMachinesPerBroker[b], broker.third.first)
            val clientName = randomName()
            logger.debug("Calculating actions for subscriber $clientName")

            // file and writer
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_Sub_$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())

            // vars
            var location = Location.randomInGeofence(broker.second)
            var timestamp = Time(Random.nextInt(0, warmupTime.i(MS)), MS)

            // send first ping (needed for broker jurisdiction) and create initial subscriptions
            writer.write(calculatePingAction(timestamp, location, stats))
            writer.write(calculateSubscribeActions(timestamp, location, stats))
            timestamp = warmupTime

            // generate actions until time reached
            while (timestamp <= timeToRunPerClient) {
                if (getTrueWithChance(mobilityProbability)) {
                    // we are mobile and travel somewhere else
                    location = calculateNextLocation(broker.second,
                            location,
                            Random.nextDouble(0.0, 360.0),
                            minTravelDistance,
                            maxTravelDistance,
                            stats)
                    // no need to send ping as subscriber location is not important -> no message geofence
                    writer.write(calculateSubscribeActions(timestamp, location, stats))

                }
                timestamp += subscriberMobilityCheckTime
            }

            // add a last ping message at runtime, as "last message"
            writer.write(calculatePingAction(timeToRunPerClient, location, stats))

            writer.flush()
            writer.close()
        }
    }
    val output = stats.getSummary(subsPerBrokerArea, pubsPerBrokerArea, timeToRunPerClient)
    logger.info(output)
    File("$directoryPath/01_summary.txt").appendText(output)
}

private fun calculatePingAction(timestamp: Time, location: Location, stats: Stats): String {
    stats.addPingMessage()
    return "${timestamp.i(MS)};${location.lat};${location.lon};ping;;;\n"
}

private fun calculateSubscribeActions(timestamp: Time, location: Location, stats: Stats): String {

    val actions = StringBuilder()

    // temperature
    val geofenceTB = Geofence.circle(location,
            Random.nextDouble(minTemperatureSubscriptionGeofenceDiameter, maxTemperatureSubscriptionGeofenceDiameter))
    actions.append("${timestamp.i(MS) + 1};${location.lat};${location.lon};subscribe;" + "$temperatureTopic;" + "${geofenceTB.wktString};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceTB, brokerAreas)
    stats.addSubscribeMessage()

    // humidity
    val geofenceHB = Geofence.circle(location,
            Random.nextDouble(minHumiditySubscriptionGeofenceDiameter, maxHumiditySubscriptionGeofenceDiameter))
    actions.append("${timestamp.i(MS) + 2};${location.lat};${location.lon};subscribe;" + "$humidityTopic;" + "${geofenceHB.wktString};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceHB, brokerAreas)
    stats.addSubscribeMessage()

    // barometric pressure
    val geofenceBB = Geofence.circle(location,
            Random.nextDouble(minBarometricSubscriptionGeofenceDiameter, maxBarometricSubscriptionGeofenceDiameter))
    actions.append("${timestamp.i(MS) + 3};${location.lat};${location.lon};subscribe;" + "$barometricPressureTopic;" + "${geofenceBB.wktString};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceBB, brokerAreas)
    stats.addSubscribeMessage()

    return actions.toString()
}

private fun calculatePublishActions(timestamp: Time, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    // temperature condition
    actions.append("${timestamp.i(MS) + 4};${location.lat};${location.lon};publish;" + "$temperatureTopic;;" + "$temperaturePayloadSize\n")
    stats.addPublishMessage()
    stats.addPayloadSize(temperaturePayloadSize)

    // humidity broadcast
    var payloadSize = Random.nextInt(minHumidityPayloadSize, maxHumidityPayloadSize)
    actions.append("${timestamp.i(MS) + 5};${location.lat};${location.lon};publish;" + "$humidityTopic;;" + "$payloadSize\n")
    stats.addPublishMessage()
    stats.addPayloadSize(payloadSize)

    // barometric pressure broadcast
    payloadSize = Random.nextInt(minBarometerPayloadSize, maxBarometerPayloadSize)
    actions.append("${timestamp.i(MS) + 6};${location.lat};${location.lon};publish;" + "$barometricPressureTopic;;$payloadSize\n")
    stats.addPublishMessage()
    stats.addPayloadSize(payloadSize)

    return actions.toString()
}
