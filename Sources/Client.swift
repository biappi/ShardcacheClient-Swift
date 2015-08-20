//
//  Client.swift
//  ShardcacheClient
//
//  Created by Antonio Malara on 8/20/15.
//  Copyright © 2015 Antonio Malara. All rights reserved.
//

import Foundation

public typealias Hostname        = String
public typealias Nodename        = String
public typealias Port            = Int

public typealias ShardcacheNodes = [Hostname: (Nodename, Port)]

public class ShardcacheClient {
    let chash       : CHash
    var nodes       : ShardcacheNodes
    var connections : [String:Stream] = [:]
    
    public init(nodes: ShardcacheNodes) {
        chash = CHash(nodes: [String](nodes.keys), replicas: 200)
        self.nodes = nodes
    }

    func get(key:[UInt8]) {
        var message = Message(message:.GET)
        message.addRecord(key)
        message.end()
        
        let nodeName   = chash.lookup(key)
        let connection = connections[nodeName]
        
        if let connection = connection {
            connection.send(message.packet)
        }
        else {
            let (host, port) = nodes[nodeName]!
            let connection = Stream(host: host, port: port)
            connection.connect()
            connections[nodeName] = connection
        }
    }
}

public extension ShardcacheClient {
    func get(key: String) { get([UInt8](key.utf8)) }
}