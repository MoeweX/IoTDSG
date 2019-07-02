# Hiking Scenario

In the hiking scenario, clients travel on pre-defined routes and publish data to all other clients in close proximity
on a regular basis.
Our clients are hikers that use a messaging service to share information concerning their surroundings (e.g., the
condition of the path they are taking), as well as to send text messages to other hikers nearby.
Each published message has a message geofence that ensures that data is not sent to clients too far away to keep
information local and prevent data being mined by third parties.
Furthermore, each client creates a subscription with a geofence comprising the nearby area; note, that each message and
subscription geofence can have a different shape and size as clients can define these based on their personal needs and
preferences.