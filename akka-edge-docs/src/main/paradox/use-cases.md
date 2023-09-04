# Example Use Cases

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

## Factories

Local compute power to analyze images and videos to quickly detect abnormalities in the manufacturing process. Machine
learning can be shared between factories. Telemetry data can be gathered, aggregated and analyzed by cloud services
to find how the process can be improved.


