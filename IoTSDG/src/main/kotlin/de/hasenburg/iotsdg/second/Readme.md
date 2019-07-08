# Open Environmental Data Scenario

In the OpenData scenario, IoT sensors publish their data to their respective topic, i.e., temperature, humidity, 
or barometric pressure.
This data is supposed to be available world wide so no message geofence to restrain access based on regions, exist.
Furthermore, in this scenario subscribers have an interest in data from sensors nearby and thus create subscriptions 
for different sensors in proximity. Each subscriber might have a different preference regarding the proximity, so the
subscription geofences have arbitrary sizes, even different for the same subscriber for the available topics.

