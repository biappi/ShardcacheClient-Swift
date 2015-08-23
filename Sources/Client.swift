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

    func connectionForNode(nodeName: String) -> Stream {
        let connection = connections[nodeName]
        
        if let connection = connection {
            return connection
        }
        else {
            let (host, port) = nodes[nodeName]!
            let connection   = Stream(host: host, port: port)
            
            connection.connect()
            connections[nodeName] = connection
            
            return connection
        }
    }
    
    func get(key:[UInt8]) {
        var message = Message(message:.GET)
        message.addRecord(key)
        message.end()
        
        let nodeName   = chash.lookup(key)
        let connection = connectionForNode(nodeName)
        
        connection.send(message.packet)
    }
    
    func set(key:[UInt8], value:[UInt8]) {
        var message = Message(message:.SET)
        message.addRecord(key)
        message.addRecord(value)
        message.end()
        
        let nodeName   = chash.lookup(key)
        let connection = connectionForNode(nodeName)
        
        connection.send(message.packet)
    }
}

public extension ShardcacheClient {
    func get(key: String) { get([UInt8](key.utf8)) }
    func set(key: String, value: String) { set([UInt8](key.utf8), value:[UInt8](value.utf8)) }
}