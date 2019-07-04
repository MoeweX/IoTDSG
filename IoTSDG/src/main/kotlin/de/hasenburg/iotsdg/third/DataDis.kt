package de.hasenburg.iotsdg.third

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.iotsdg.*
import de.hasenburg.iotsdg.Stats
import org.locationtech.spatial4j.distance.DistanceUtils
import java.io.File
import org.apache.logging.log4j.LogManager
import kotlin.random.Random


private val logger = LogManager.getLogger()

private const val minTravelDistance = 1.0 //km
private const val maxTravelDistance = 20.0 //km
private const val minPubTimeGap = 2000 //ms
private const val maxPubTimeGap = 70000 //ms
private const val minSubTimeGap = 3000 //ms
private const val maxSubTimeGap = 12000 //ms
private const val minSubRenewalTime = 5 * 60 * 1000 //5 min
private const val maxSubRenewalTime = 60 * 60 * 1000 //60 min

private const val minTemperatureBroadcastMessageGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxTemperatureBroadcastMessageGeofenceDiameter = 10.0 * DistanceUtils.KM_TO_DEG
private const val minHumidityBroadcastMessageGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxHumidityBroadcastMessageGeofenceDiameter = 10.0 * DistanceUtils.KM_TO_DEG
private const val minAnnouncementBroadcastMessageGeofenceDiameter = 40.0 * DistanceUtils.KM_TO_DEG
private const val maxAnnouncementBroadcastMessageGeofenceDiameter = 120.0 * DistanceUtils.KM_TO_DEG

private const val temperaturePayloadSize = 100
private const val minHumidityPayloadSize = 50
private const val maxHumidityPayloadSize = 150
private const val minAnnouncementPayloadSize = 10
private const val maxAnnouncementPayloadSize = 75
private const val mobilityProbability = 50

private const val directoryPath = "./dataDistribution"
private const val temperatureTopic = "temperature"
private const val humidityTopic = "humidity"
private const val announcementTopic = "public_announcement"

private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(-82.999083, 39.961332), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
private val workloadMachinesPerBroker = listOf(3, 3, 3)
private val subsPerBrokerArea = listOf(2, 2, 2)
private val pubsPerBrokerArea = listOf(3, 3, 3)

private const val timeToRunPerClient = 1800000

fun main() {
    validateBrokersDoNotOverlap(brokerAreas)
    prepareDir(directoryPath)

    val stats = Stats()
    val setup = getSetupString("de.hasenburg.iotsdg.third.DataDisKt")
    logger.info(setup)
    File("$directoryPath/02_summary.txt").writeText(setup)

    for (b in 0..2) {

        val broker = getBrokerTriple(b, brokerNames, brokerAreas, subsPerBrokerArea, pubsPerBrokerArea)
        var currentWorkloadMachine: Int

        // publisher actions
        for (pub in 1..broker.third.second) {
            currentWorkloadMachine =
                    getCurrentWorkloadMachine(pub, broker.first, workloadMachinesPerBroker[b], broker.third.second)

            val clientName = randomName()
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_Pub_$clientName.csv")
            var timestamp = Random.nextInt(0, 2000)
            val writer = file.bufferedWriter()
            var location = Location.randomInGeofence(broker.second)
            writer.write(getHeader())

            /*writer.write(calculatePingActions(timestamp, location))
            * not important */

            while (timestamp <= timeToRunPerClient) {

                writer.write(calculatePublishActions(timestamp, location, stats))

                if (getTrueWithChance(mobilityProbability)) {
                    location = calculateNextLocation(broker.second,
                            location,
                            Random.nextDouble(0.0, 360.0),
                            minTravelDistance,
                            maxTravelDistance,
                            stats)
                }
                timestamp += Random.nextInt(minPubTimeGap, maxPubTimeGap)
            }
            writer.flush()
            writer.close()
        }

        // subscriber actions
        for (sub in 1..broker.third.first) { // for subscribers
            currentWorkloadMachine =
                    getCurrentWorkloadMachine(sub, broker.first, workloadMachinesPerBroker[b], broker.third.first)

            val clientName = randomName()
            logger.debug("Calculating actions for client $clientName")
            var location = Location.randomInGeofence(broker.second)
            var timestamp = Random.nextInt(0, 3000)
            val file = File("$directoryPath/${broker.first}-${currentWorkloadMachine}_Sub_$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())
            writer.write(calculateSubscribeActions(timestamp, location, stats))
            timestamp += 1000
            var subRenewalTime = Random.nextInt(minSubRenewalTime, maxSubRenewalTime)

            while (timestamp <= timeToRunPerClient) {

                if (timestamp >= subRenewalTime) {
                    writer.write(calculateSubscribeActions(timestamp, location, stats))
                    subRenewalTime += Random.nextInt(minSubRenewalTime, maxSubRenewalTime)
                }
                location = calculateNextLocation(broker.second,
                        location,
                        Random.nextDouble(0.0, 360.0),
                        minTravelDistance,
                        maxTravelDistance,
                        stats)
                writer.write(calculatePingActions(timestamp, location, stats))
                timestamp += Random.nextInt(minSubTimeGap, maxSubTimeGap)
            }
            writer.flush()
            writer.close()
        }
    }
    val output = getSummary(subsPerBrokerArea, pubsPerBrokerArea, timeToRunPerClient / 1000, stats)
    logger.info(output)
    File("$directoryPath/02_summary.txt").appendText(output)
}

private fun calculatePingActions(timestamp: Int, location: Location, stats: Stats): String {
    stats.addPingMessages()
    return "$timestamp;${location.lat};${location.lon};ping;;;\n"
}

private fun calculateSubscribeActions(timestamp: Int, location: Location, stats: Stats): String {

    val actions = StringBuilder()

    // temperature
    actions.append("${timestamp + 1};${location.lat};${location.lon};subscribe;" + "$temperatureTopic;;\n")
    stats.addSubscribeMessages()

    // humidity
    actions.append("${timestamp + 2};${location.lat};${location.lon};subscribe;" + "$humidityTopic;;\n")
    stats.addSubscribeMessages()

    // barometric pressure
    actions.append("${timestamp + 3};${location.lat};${location.lon};subscribe;" + "$announcementTopic;;\n")
    stats.addSubscribeMessages()

    return actions.toString()
}

private fun calculatePublishActions(timestamp: Int, location: Location, stats: Stats): String {
    val actions = StringBuilder()

    // temperature condition
    val geofenceTB = Geofence.circle(location,
            Random.nextDouble(minTemperatureBroadcastMessageGeofenceDiameter,
                    maxTemperatureBroadcastMessageGeofenceDiameter))
    addStat_messageGeofenceOverlaps(geofenceTB, brokerAreas, stats)
    actions.append("${timestamp + 4};${location.lat};${location.lon};publish;" + "$temperatureTopic;" + "${geofenceTB.wktString};" + "$temperaturePayloadSize\n")
    stats.addPublishMessages()
    stats.addPayloadSize(temperaturePayloadSize)

    // humidity broadcast
    val geofenceH = Geofence.circle(location,
            Random.nextDouble(minHumidityBroadcastMessageGeofenceDiameter, maxHumidityBroadcastMessageGeofenceDiameter))
    addStat_messageGeofenceOverlaps(geofenceH, brokerAreas, stats)
    var payloadSize = Random.nextInt(minHumidityPayloadSize, maxHumidityPayloadSize)
    actions.append("${timestamp + 5};${location.lat};${location.lon};publish;" + "$humidityTopic;" + "${geofenceH.wktString};" + "$payloadSize\n")
    stats.addPayloadSize(payloadSize)
    stats.addPublishMessages()

    // public announcement broadcast
    val geofencePA = Geofence.circle(location,
            Random.nextDouble(minAnnouncementBroadcastMessageGeofenceDiameter,
                    maxAnnouncementBroadcastMessageGeofenceDiameter))
    addStat_messageGeofenceOverlaps(geofencePA, brokerAreas, stats)
    payloadSize = Random.nextInt(minAnnouncementPayloadSize, maxAnnouncementPayloadSize)
    actions.append("${timestamp + 6};${location.lat};${location.lon};publish;" + "$announcementTopic;" + "${geofencePA.wktString};" + "$payloadSize\n")
    stats.addPayloadSize(payloadSize)
    stats.addPublishMessages()

    return actions.toString()
}

private fun getSetupString(s: String): String {
    // there should be another solution in the future: https://stackoverflow.com/questions/33907095/kotlin-how-can-i-use-reflection-on-packages
    val c = Class.forName(s)
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