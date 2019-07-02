# Open Environmental Data Scenario

IoT sensor data is often not available directly to users, instead, it is common to create data dumps that are released
once per day. Such a procedure renders all IoT applications that require real time data impossible. Therefore, in this
scenario, DisGB is used to make IoT sensor data directly available to users so that they can process information in real
time without relying on a third party that collects the information and releases it once per day.
The publishers are IoT sensors, which publish to topics such as temperature, humidity, barometric pressure, etc. While
it is unlikely that any message geofence exists as data is supposed to be open and available to evryone, message
geofences can be used to restrain access based on regions; for instance, people present inside a country should only get
to access data from sensors there. However, as this scenario is about open data, we assume that message geofences do not
exist. On the other hand, subscribers only have an interest in sensor data originating in a certain region. For example,
a tourist might want to receive data only from the city he's visiting or a smart home application might only have an
interest in barometric pressure values of sensors that are at most 20km away in order to identify approaching storms so
that windows can be closed.

