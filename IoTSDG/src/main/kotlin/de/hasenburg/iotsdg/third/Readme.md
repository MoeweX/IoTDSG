# Context-based Data Distribution Scenario

The main idea of this scenario is that subscribers know what kind of data they want to receive, while publishers know in
what geo-context a data delivery makes sense. For example, a subscriber might want to continuously receive air
temperature readings. But what temperature readings should he receive? While he could receive all readings of sensors in
close proximity, this is not necessarily the most intelligent solution. A more advanced solution can be created when the
sensors define the geo-context in which their data has relevance, as they have additional knowledge about the
environment they operate in.
In practice, this can be used by temperature sensors to control that only pedestrians walking down the street they are
installed at to receive their readings. Then, when entering a store, the user would not ne inside the street's geofences
anymore, but would suddenly be inside a message geofence that comprises the store's exhibition area. Another example is
distribution of public announcements to citizens. As these announcements are only relevant to citizens currently in
affected areas, it is convenient for a user to only subscribe once to respective topic (eg. (public) announcements) and
then travel between districts authorities aszs these authorities have an additional knowledge about the information they
are going to publish. To implement such applications with DisGB, users only have to subscribe once to topics of their
interest and can then receive relevant data of publishers based on message geofences which are created with additional
domain knowledge.
