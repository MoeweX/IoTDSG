package de.hasenburg.iotsdg.second

import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomName
import de.hasenburg.iotsdg.utility.*
import org.locationtech.spatial4j.distance.DistanceUtils
import java.io.File
import org.apache.logging.log4j.LogManager
import kotlin.random.Random
import de.hasenburg.iotsdg.stats.Stats


private val logger = LogManager.getLogger()

private const val minTravelDistance = 20.0 //km
private const val maxTravelDistance = 80.0 //km
private const val minPubTimeGap = 2000 //ms
private const val maxPubTimeGap = 15000 //ms
private const val minSubTimeGap = 3000 //ms
private const val maxSubTimeGap = 12000 //ms
private const val minSubRenewalTime = 5 * 60 * 1000 //5 min
private const val maxSubRenewalTime = 15 * 60 * 1000 //12 min

private const val minTemperatureBroadcastSubscriptionGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxTemperatureBroadcastSubscriptionGeofenceDiameter = 500.0 * DistanceUtils.KM_TO_DEG
private const val minHumidityBroadcastSubscriptionGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxHumidityBroadcastSubscriptionGeofenceDiameter = 500.0 * DistanceUtils.KM_TO_DEG
private const val minBarometricBroadcastSubscriptionGeofenceDiameter = 1.0 * DistanceUtils.KM_TO_DEG
private const val maxBarometricBroadcastSubscriptionGeofenceDiameter = 500.0 * DistanceUtils.KM_TO_DEG

private const val mobilityProbability = 10
private const val temperaturePayloadSize = 100
private const val minHumidityPayloadSize = 50
private const val maxHumidityPayloadSize = 150
private const val minBarometerPayloadSize = 10
private const val maxBarometerPayloadSize = 75

private const val directoryPath = "./environmentaldata"
private const val temperatureTopic = "temperature"
private const val humidityTopic = "humidity"
private const val barometricPressureTopic = "barometric_pressure"

private val brokerNames = listOf("Columbus", "Frankfurt", "Paris")
private val brokerAreas = listOf(Geofence.circle(Location(-82.999083, 39.961332), 5.0),
        Geofence.circle(Location(50.106732, 8.663124), 2.1),
        Geofence.circle(Location(48.877366, 2.359708), 2.1))
private val workloadMachinePerBroker = listOf(3, 3, 3)
private val subsPerBrokerArea = listOf(2, 2, 2)
private val pubsPerBrokerArea = listOf(3, 3, 3)
private var timeToRunPerClient = 1800000


fun main() {

    if (checkBrokerOverlap(brokerAreas)) {
        logger.fatal("brokers should not overlap")
        System.exit(1)
    }
    //to maintain indicators for scenario
    val stat = Stats()
    // make sure dir exists, delete old content
    val dir = File(directoryPath)
    if (dir.exists()) {
        logger.info("Deleting old content")
        dir.deleteRecursively()
    }
    dir.mkdirs()

    val setup = getSetupString("de.hasenburg.iotsdg.second.OpenDataKt")
    logger.info(setup)
    File("$directoryPath/01_summary.txt").writeText(setup)

    for (b in 0..2) { // for sensors/publishers

        val broker = getBrokerTriple(b, brokerNames, brokerAreas, subsPerBrokerArea, pubsPerBrokerArea)
        var currentWorkloadMachine: Int

        for (pub in 0..broker.third.second) {

            if (workloadMachinePerBroker.get(b) == 0) {
                logger.info("Skipping actions for broker ${broker.first} as it does not have any workload machines")
                break
            }
            currentWorkloadMachine = pub % workloadMachinePerBroker.get(b)
            val clientName = randomName()
            val file = File("$directoryPath/${broker.first}-$currentWorkloadMachine-Pub-$clientName.csv")
            var timestamp = Random.nextInt(0, 2000)
            val writer = file.bufferedWriter()
            val location = Location.randomInGeofence(broker.second)
            writer.write(getHeader())
            writer.write(calculatePingActions(timestamp, location, stat))
            while (timestamp <= timeToRunPerClient) {
                writer.write(calculatePublishActions(timestamp, location, stat))
                timestamp += Random.nextInt(minPubTimeGap, maxPubTimeGap)
            }
            writer.flush()
            writer.close()
        }

        for (c in 0..broker.third.first) { // for subscribers

            if (workloadMachinePerBroker.get(b) == 0) {
                logger.info("Skipping actions for broker ${broker.first} as it does not have any workload machines")
                break
            }
            currentWorkloadMachine = c % workloadMachinePerBroker.get(b)
            val clientName = randomName()
            logger.debug("Calculating actions for client $clientName")
            var location = Location.randomInGeofence(broker.second)
            var timestamp = Random.nextInt(0, 3000)
            val file = File("$directoryPath/${broker.first}-$currentWorkloadMachine-Sub-$clientName.csv")
            val writer = file.bufferedWriter()
            writer.write(getHeader())
            writer.write(calculateSubscribeActions(timestamp, location, stat))
            timestamp += 1000
            var subRenewalTime = Random.nextInt(minSubRenewalTime, maxSubRenewalTime)

            while (timestamp <= timeToRunPerClient) {

                if (timestamp >= subRenewalTime) {
                    writer.write(calculateSubscribeActions(timestamp, location, stat))
                    subRenewalTime += Random.nextInt(minSubRenewalTime, maxSubRenewalTime)
                }
                if (getTrueWithChance(mobilityProbability)) {
                    writer.write(calculatePingActions(timestamp, location, stat))
                    location = calculateNextLocation(broker.second,
                            location,
                            Random.nextDouble(0.0, 360.0),
                            minTravelDistance,
                            maxTravelDistance,
                            stat)
                }
                timestamp += Random.nextInt(minSubTimeGap, maxSubTimeGap)
            }
            writer.flush()
            writer.close()
        }
    }
    val output = getOutput(brokerNames, subsPerBrokerArea, pubsPerBrokerArea, timeToRunPerClient / 1000, stat)

    logger.info(output)
    File("$directoryPath/01_summary.txt").appendText(output)
}

private fun calculateSubscribeActions(timestamp: Int, location: Location, stat: Stats): String {

    val actions = StringBuilder()

    // temperature
    val geofenceTB = Geofence.circle(location,
            Random.nextDouble(minTemperatureBroadcastSubscriptionGeofenceDiameter,
                    maxTemperatureBroadcastSubscriptionGeofenceDiameter))
    checkSubscriptionGeofenceBrokerOverlap(geofenceTB, brokerAreas, stat)
    actions.append("${timestamp + 1};${location.lat};${location.lon};subscribe;" + "$temperatureTopic;" + "${geofenceTB.wktString};\n")
    stat.addSubscribeMessages()

    // humidity
    val geofenceHB = Geofence.circle(location,
            Random.nextDouble(minHumidityBroadcastSubscriptionGeofenceDiameter,
                    maxHumidityBroadcastSubscriptionGeofenceDiameter))
    actions.append("${timestamp + 2};${location.lat};${location.lon};subscribe;" + "$humidityTopic;" + "${geofenceHB.wktString};\n")
    checkSubscriptionGeofenceBrokerOverlap(geofenceHB, brokerAreas, stat)
    stat.addSubscribeMessages()

    // barometric pressure
    val geofenceBB = Geofence.circle(location,
            Random.nextDouble(minBarometricBroadcastSubscriptionGeofenceDiameter,
                    maxBarometricBroadcastSubscriptionGeofenceDiameter))
    actions.append("${timestamp + 3};${location.lat};${location.lon};subscribe;" + "$barometricPressureTopic;" + "${geofenceBB.wktString};\n")
    checkSubscriptionGeofenceBrokerOverlap(geofenceBB, brokerAreas, stat)
    stat.addSubscribeMessages()

    return actions.toString()
}

private fun calculatePublishActions(timestamp: Int, location: Location, stat: Stats): String {
    val actions = StringBuilder()

    // temperature condition
    actions.append("${timestamp + 4};${location.lat};${location.lon};publish;" + "$temperatureTopic;;" + "$temperaturePayloadSize\n")
    stat.addPublishMessages()
    stat.addPayloadSize(temperaturePayloadSize)

    // humidity broadcast
    var payloadSize = Random.nextInt(minHumidityPayloadSize, maxHumidityPayloadSize)
    actions.append("${timestamp + 5};${location.lat};${location.lon};publish;" + "$humidityTopic;;" + "$payloadSize\n")
    stat.addPayloadSize(payloadSize)
    stat.addPublishMessages()

    // barometric pressure broadcast
    payloadSize = Random.nextInt(minBarometerPayloadSize, maxBarometerPayloadSize)
    actions.append("${timestamp + 6};${location.lat};${location.lon};publish;" + "$barometricPressureTopic;;$payloadSize\n")
    stat.addPayloadSize(payloadSize)
    stat.addPublishMessages()
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