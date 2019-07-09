# Context-based Data Distribution Scenario

The main idea of this scenario is that subscribers know what kind of data they want to receive, while publishers know in
what geo-context a data delivery makes sense. For example, a subscriber might want to continuously receive air
temperature readings. But what temperature readings should he receive? While he could receive all readings of sensors in
close proximity, this is not necessarily the most intelligent solution. A more advanced solution can be created when the
sensors define the geo-context in which their data has relevance, as they have additional knowledge about the
environment they operate in.
Thus, in this scenario subscribers create subscriptions for a set of topics; these subscriptions are only updated 
rarely and do not consider any Geofence. However, as subscribers are moving, their location is updated often.
On the other hand, publishers do not travel at all and publish to their topic messages that have a geofence.
