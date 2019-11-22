package de.hasenburg.iotdsg

/**
In the OpenData scenario, IoT sensors publish their data to their respective topic, i.e., temperature, humidity,
or barometric pressure.
This data is supposed to be available world wide so no message geofence to restrain access based on regions, exist.
Furthermore, in this scenario subscribers have an interest in data from sensors nearby and thus create subscriptions
for different sensors in proximity. Each subscriber might have a different preference regarding the proximity, so the
subscription geofences have arbitrary sizes, even different for the same subscriber for the available topics.
*/

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.iotdsg.helper.*
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils
import units.Distance
import units.Distance.Unit.KM
import units.Distance.Unit.M
import units.Time
import units.Time.Unit.*
import java.io.File
import kotlin.random.Random

private val logger = LogManager.getLogger()

// -------- Brokers --------
private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(39.961332, -82.999083), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
private val workloadMachinesPerBroker = listOf(3, 3, 3)
private val subsPerBrokerArea = listOf(400, 400, 400)
private val pubsPerBrokerArea = listOf(800, 800, 800)

// -------- Subscribers --------
private val minTravelDistance = Distance(500, M)
private val maxTravelDistance = Distance(100, KM)
private val minMobilityCheck = Time(3, S)
private val maxMobilityCheck = Time(6, S)
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
private const val directoryPath = "./environment"
private val warmupTime = Time(5, S)
private val timeToRunPerClient = Time(15, MIN)


fun main() {
    validateBrokersDoNotOverlap(brokerAreas)
    prepareDir(directoryPath)

    val stats = Stats()
    val setup = getSetupString("de.hasenburg.iotdsg.OpenDataGeneratorKt")
    logger.info(setup)
    File("$directoryPath/00_summary.txt").writeText(setup)

    for (b in 0..2) {

        val broker = getBrokerTriple(b,
                brokerNames,
                brokerAreas,
                subsPerBrokerArea,
                pubsPerBrokerArea)
        var currentWorkloadMachine: Int

        logger.info("Calculating publisher actions for broker ${broker.first}")
        // loop through publishers for broker
        for (pub in 1..broker.third.second) {
            currentWorkloadMachine =
                    getCurrentWorkloadMachine(pub,
                            broker.first,
                            workloadMachinesPerBroker[b],
                            broker.third.second)
            val clientName = randomName()
            logger.debug("Calculating actions for publisher $clientName")

            // file and writer
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())

            // vars
            val location = Location.randomInGeofence(broker.second)
            var timestamp = Time(Random.nextInt(0, warmupTime.i(MS)), MS)

            // write fixed location of publisher
            writer.write(calculatePingAction(timestamp, location, stats))
            timestamp = warmupTime

            // pick device topic
            val rnd = Random.nextInt(0, 3)

            // generate actions until time reached
            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePublishActions(timestamp, location, rnd, stats))
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
                    getCurrentWorkloadMachine(sub,
                            broker.first,
                            workloadMachinesPerBroker[b],
                            broker.third.first)
            val clientName = randomName()
            logger.debug("Calculating actions for subscriber $clientName")

            // file and writer
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_$clientName.csv")
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
                timestamp += Time(Random.nextInt(minMobilityCheck.i(MS), maxMobilityCheck.i(MS)), MS)
            }

            // add a last ping message at runtime, as "last message"
            writer.write(calculatePingAction(timeToRunPerClient, location, stats))

            writer.flush()
            writer.close()
        }
    }
    val output = stats.getSummary(subsPerBrokerArea, pubsPerBrokerArea, timeToRunPerClient)
    logger.info(output)
    File("$directoryPath/00_summary.txt").appendText(output)
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
    actions.append("${timestamp.i(MS) + 1};${location.lat};${location.lon};subscribe;$temperatureTopic;${geofenceTB.wktString};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceTB, brokerAreas)
    stats.addSubscribeMessage()

    // humidity
    val geofenceHB = Geofence.circle(location,
            Random.nextDouble(minHumiditySubscriptionGeofenceDiameter, maxHumiditySubscriptionGeofenceDiameter))
    actions.append("${timestamp.i(MS) + 2};${location.lat};${location.lon};subscribe;$humidityTopic;${geofenceHB.wktString};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceHB, brokerAreas)
    stats.addSubscribeMessage()

    // barometric pressure
    val geofenceBB = Geofence.circle(location,
            Random.nextDouble(minBarometricSubscriptionGeofenceDiameter, maxBarometricSubscriptionGeofenceDiameter))
    actions.append("${timestamp.i(MS) + 3};${location.lat};${location.lon};subscribe;$barometricPressureTopic;${geofenceBB.wktString};\n")
    stats.addSubscriptionGeofenceOverlaps(geofenceBB, brokerAreas)
    stats.addSubscribeMessage()

    return actions.toString()
}

private fun calculatePublishActions(timestamp: Time, location: Location, topicIndex: Int, stats: Stats): String {
    val actions = StringBuilder()
    val geofence = Geofence.world()

    when (topicIndex) {
        0 -> {
            // temperature condition
            actions.append("${timestamp.i(MS) + 4};${location.lat};${location.lon};publish;$temperatureTopic;${geofence.wktString};$temperaturePayloadSize\n")
            stats.addPublishMessage()
            stats.addPayloadSize(temperaturePayloadSize)
        }

        1 -> {
            // humidity broadcast
            val payloadSize = Random.nextInt(minHumidityPayloadSize, maxHumidityPayloadSize)
            actions.append("${timestamp.i(MS) + 5};${location.lat};${location.lon};publish;$humidityTopic;${geofence.wktString};$payloadSize\n")
            stats.addPublishMessage()
            stats.addPayloadSize(payloadSize)
        }

        2 -> {
            // barometric pressure broadcast
            val payloadSize = Random.nextInt(minBarometerPayloadSize, maxBarometerPayloadSize)
            actions.append("${timestamp.i(MS) + 6};${location.lat};${location.lon};publish;$barometricPressureTopic;${geofence.wktString};$payloadSize\n")
            stats.addPublishMessage()
            stats.addPayloadSize(payloadSize)
        }

        else -> {
            logger.warn("Topic index {} out of range, no data will be published", topicIndex)
        }
    }

    return actions.toString()
}
