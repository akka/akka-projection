# Example Use Cases

## Energy management

Energy management systems, including those that manage energy at a residential property, have a particular trait 
where they should strive to function without being connected to the internet. All local processing decisions should
occur while treating the internet opportunistically. When the internet is there, it can be used to update data
for improved decision making. When it is not though, the energy system must be able to continue to function using the
data it has.

Take residential Electric Vehicle charging as an example. It is important that a car is able to receive a charge even when
the internet is not present. When the internet is present then energy market forecast data can be downloaded for the
next 24 hours, and a charging schedule is set up in consideration of grid demands. Failing connectivity, off-peak
scheduling and historical trends on usage can be relied on.

In the case where access to the edge-based system is not possible due to an internet outage, Bluetooth or WiFi can be
used to connect locally and access the user interface of the system.

## Vehicle communications

There are many use cases for having vehicles communicate with each other, or with local devices such
as traffic lights, information signs, distribution points, either in a single geographic area or a wide area:

* Traffic coordination and status reporting
* Support self-driving vehicles with AI decisions
* Fleet tracking, coordination and communication

The communication between the vehicles and local devices would not be point-to-point, but via a local PoP (point of presence). Akka Edge
would be great for implementing this PoP service. It can not only manage the communication between the devices in the
area, but also aggregate or filter information of importance and send that to more central cloud services. It can
receive information from cloud services and propagate relevant information to the devices.

The benefits of having vehicles in an area communicate locally with each other and devices instead of through the cloud include:

* Isolation from wider area network faults
* Communication can be far more chatty without overwhelming ingress/egress in the cloud, allowing much higher scales
* Much lower latencies

## High availability retail

Retail systems need to be highly available. One of the biggest problems that retail systems have in achieving this is
that the times that they are most important (that the company stands to lose the most revenue if they go down) are the
times when they are most likely to go down, because the surge in load due to the surge in shoppers often makes the
system unstable.

Isolation is an effective way to address this, if one part of the system serving one geographic area is overloaded,
and it fails, while not ideal, the damage is limited if that doesn’t affect other geographic areas. This can be
achieved by using PoPs close to, or even in, the retail outlets themselves.

One challenge with this is data that moves around. A shopper might browse online first, then go to one store, and
maybe go to another store if they find the stock they want isn’t there. Having this users profile follow them,
so that a “virtual shop assistant” can guide them to get the right help they need in store, without relying
on the availability of a central system, would enable seamless experiences even under load.

## Sporting events

Real-time, interactive applications for a sports event can offer a much more enriching experience. Augmented reality
of a game require low latency for real-time updates. Quick access to rules of the game and facts about the players 
may also be of importance for a nice interactive experience. Many people gathered at the same place is demanding
for the communication infrastructure and the backend systems. Local services running at the sports arena would
be a good use case for Akka Edge.

## Agriculture

Edge based systems can be very useful on farms to collect data from sensors monitoring water tank levels, water pipe
pressure, electric fence voltages, soil moisture and temperature and more. Most of the data here is about being able
to view trends in data over time. The function of the edge in this situation is to store and then forward these observations. 
If there is no internet connection then these observations should continue to be collected and stored; even if a short 
power outage causes the edge-device to restart, noting that sensors often transmit at most once per hour and can 
re-transmit their previous observation along with a new one.

When internet connectivity is re-established, non-synchronized observations can be forwarded to a cloud-based service.

For more remote farms, having internet connectivity is not strictly necessary either. An edge-device can forward its 
observations on to another computer located at a local building using low-powered directional WiFi. 
This other computer is then available only on the farm's LAN.

## Factories

Local compute power to analyze images and videos to quickly detect abnormalities in the manufacturing process. Machine
learning can be shared between factories. Telemetry data can be gathered, aggregated and analyzed by cloud services
to find how the process can be improved.


