package de.hasenburg.iotsdg.third

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.iotsdg.*
import org.apache.logging.log4j.LogManager
import org.locationtech.spatial4j.distance.DistanceUtils
import units.Time
import units.Time.Unit.*
import java.io.File
import kotlin.random.Random.Default.nextDouble
import kotlin.random.Random.Default.nextInt


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
private const val minTravelSpeed = 2 // km/h
private const val maxTravelSpeed = 8 // km/h
private val minTravelTime = Time(5, S)
private val maxTravelTime = Time(30, S)
// random choice whether to subscribe to temperature or not
private const val checkTemperatureSubscriptionProbability = 5 // %

// -------- Publishers --------
private val minPubTimeGap = Time(10, S)
private val maxPubTimeGap = Time(60, S)

// -------- Message Geofences -------- values are in degree
private const val minTemperatureMessageGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxTemperatureMessageGeofenceDiameter = 25.0 * DistanceUtils.KM_TO_DEG
private const val minAnnouncementMessageGeofenceDiameter = 10.0 * DistanceUtils.KM_TO_DEG
private const val maxAnnouncementMessageGeofenceDiameter = 100.0 * DistanceUtils.KM_TO_DEG

// -------- Messages --------
private const val temperatureTopic = "temperature"
private const val temperaturePayloadSize = 100
private const val announcementTopic = "announcement"
private const val minAnnouncementPayloadSize = 50
private const val maxAnnouncementPayloadSize = 500

// -------- Others  --------
private const val directoryPath = "./context"
private val warmupTime = Time(5, S)
private val timeToRunPerClient = Time(15, MIN)

fun main() {
    validateBrokersDoNotOverlap(brokerAreas)
    prepareDir(directoryPath)

    val stats = Stats()
    val setup = getSetupString("de.hasenburg.iotsdg.third.DataDisKt")
    logger.info(setup)
    File("$directoryPath/00_summary.txt").writeText(setup)

    for (b in 0..2) { // for sensors

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
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())

            // vars
            val location = Location.randomInGeofence(broker.second)
            var timestamp = Time(nextInt(0, warmupTime.i(MS)), MS)

            // write fixed location of publisher
            writer.write(calculatePingAction(timestamp, location, stats))
            timestamp = warmupTime

            // determine whether announcement or temperature
            val announcement = getTrueWithChance(10)

            while (timestamp <= timeToRunPerClient) {
                if (announcement) {
                    writer.write(calculateAnnouncementPublishAction(timestamp, location, stats))
                } else {
                    writer.write(calculateTemperaturePublishAction(timestamp, location, stats))
                }
                timestamp += Time(nextInt(minPubTimeGap.i(MS), maxPubTimeGap.i(MS)), MS)
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
            val clientDirection = nextDouble(0.0, 360.0)
            logger.debug("Calculating actions for client $clientName which travels in $clientDirection")

            // file and writer
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())

            // vars
            var location = Location.randomInGeofence(broker.second)
            var timestamp = Time(nextInt(0, warmupTime.i(MS)), MS)

            // send first ping and create initial subscriptions
            writer.write(calculatePingAction(timestamp, location, stats))
            writer.write(calculateTemperatureSubscribeAction(timestamp, location, stats))
            writer.write(calculateAnnouncementSubscribeAction(timestamp, location, stats))
            timestamp = warmupTime

            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePingAction(timestamp, location, stats))

                // renew temperature action?
                if (getTrueWithChance(checkTemperatureSubscriptionProbability)) {
                    writer.write(calculateTemperatureSubscribeAction(timestamp, location, stats))
                }

                val travelTime = Time(nextInt(minTravelTime.i(MS), maxTravelTime.i(MS)), MS)
                location = calculateNextLocation(broker.second,
                        location,
                        clientDirection,
                        travelTime,
                        minTravelSpeed,
                        maxTravelSpeed,
                        stats)
                timestamp += travelTime
            }

            // add a last ping message at runtime, as "last message"
            writer.write(calculatePingAction(timeToRunPerClient, location, stats))

            writer.flush()
            writer.close()
        }
    }
    // only consider subscribers for client distance travelled and average speed
    val output = stats.getSummary(subsPerBrokerArea, timeToRunPerClient)
    logger.info(output)
    File("$directoryPath/00_summary.txt").appendText(output)
}

private fun calculatePingAction(timestamp: Time, location: Location, stats: Stats): String {
    stats.addPingMessage()
    return "${timestamp.i(MS)};${location.lat};${location.lon};ping;;;\n"
}

private fun calculateTemperatureSubscribeAction(timestamp: Time, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    val geofence = Geofence.world()
    actions.append("${timestamp.i(MS) + 1};${location.lat};${location.lon};subscribe;$temperatureTopic;${geofence.wktString};\n")
    stats.addSubscribeMessage()

    return actions.toString()
}

private fun calculateAnnouncementSubscribeAction(timestamp: Time, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    val geofence = Geofence.world()
    actions.append("${timestamp.i(MS) + 2};${location.lat};${location.lon};subscribe;$announcementTopic;${geofence.wktString};\n")
    stats.addSubscribeMessage()

    return actions.toString()
}

private fun calculateAnnouncementPublishAction(timestamp: Time, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    val geofence = Geofence.circle(location,
            nextDouble(minAnnouncementMessageGeofenceDiameter, maxAnnouncementMessageGeofenceDiameter))
    val payloadSize = nextInt(minAnnouncementPayloadSize, maxAnnouncementPayloadSize)
    actions.append("${timestamp.i(MS) + 3};${location.lat};${location.lon};publish;$announcementTopic;${geofence.wktString};$payloadSize\n")
    stats.addPayloadSize(payloadSize)
    stats.addMessageGeofenceOverlaps(geofence, brokerAreas)
    stats.addPublishMessage()

    return actions.toString()
}

private fun calculateTemperaturePublishAction(timestamp: Time, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    val geofence = Geofence.circle(location,
            nextDouble(minTemperatureMessageGeofenceDiameter, maxTemperatureMessageGeofenceDiameter))
    val payloadSize = temperaturePayloadSize
    actions.append("${timestamp.i(MS) + 3};${location.lat};${location.lon};publish;$temperatureTopic;${geofence.wktString};$payloadSize\n")
    stats.addPayloadSize(payloadSize)
    stats.addMessageGeofenceOverlaps(geofence, brokerAreas)
    stats.addPublishMessage()

    return actions.toString()
}