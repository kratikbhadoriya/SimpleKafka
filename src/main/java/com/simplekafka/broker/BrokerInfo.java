package com.simplekafka.broker;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/*
* Holds information about a broker in the SimpleKafka cluster
*/
@Getter
@AllArgsConstructor
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class BrokerInfo {
    @EqualsAndHashCode.Include
    private final int id;
    private final String host;
    private final int port;
}
