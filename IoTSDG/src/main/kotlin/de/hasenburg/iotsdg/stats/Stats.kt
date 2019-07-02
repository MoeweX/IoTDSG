package de.hasenburg.iotsdg.stats

class Stats {
    private var numberOfPingMessages = 0
    private var clientDistanceTravelled = 0.0
    private var numberOfPublishedMessages = 0
    private var totalPayloadSize = 0
    private var numberOfOverlappingSubscriptionGeofences = 0
    private var numberOfSubscribeMessages = 0
    private var numberOfOverlappingMessageGeofences = 0

    fun getNumberOfPingMessages(): Int {
        return numberOfPingMessages
    }

    fun getNumberOfPublishedMessages(): Int {
        return numberOfPublishedMessages
    }

    fun getNumberOfSubscribeMessages(): Int {
        return numberOfSubscribeMessages
    }

    fun getClientDistanceTravelled(): Double {
        return clientDistanceTravelled
    }

    fun getNumberOfOverlappingSubscriptionGeofences(): Int {
        return numberOfOverlappingSubscriptionGeofences
    }

    fun getNumberOfOverlappingMessageGeofences(): Int {
        return numberOfOverlappingMessageGeofences
    }

    fun getTotalPayloadSize(): Int {
        return totalPayloadSize
    }

    fun addSubscribeMessages() {
        numberOfSubscribeMessages++
    }

    fun addPublishMessages() {
        numberOfPublishedMessages++
    }

    fun addPayloadSize(size: Int) {
        totalPayloadSize += size
    }

    fun addClientDistanceTravelled(distance: Double) {
        clientDistanceTravelled += distance
    }

    fun addSubscriptionGeofence(intersect: Int) {
        numberOfOverlappingSubscriptionGeofences += intersect
    }

    fun addMessageGeofence(intersect: Int) {
        numberOfOverlappingMessageGeofences += intersect
    }

    fun addPingMessages() {
        numberOfPingMessages++
    }
}