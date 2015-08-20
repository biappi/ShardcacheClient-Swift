//
//  Tests.swift
//  Tests
//
//  Created by Antonio Malara on 8/20/15.
//  Copyright © 2015 Antonio Malara. All rights reserved.
//

import XCTest

@testable
import ShardcacheClient

class Tests: XCTestCase {
    let testBloomHashData = [
        ("a",         678337968),
        ("ab",        967615280),
        ("abc",      2237464879),
        ("abcd",     3116905299),
        ("abcda",     658514887),
        ("abcdab",   3832400320),
        ("abcdabc",  1116613426),
        ("abcdabcd",  458327958),
    ]
    
    let testCHashInit = [
        ("server2",  292611),
        ("server4", 8425965),
        ("server3", 9348127),
    ]

    let testCHashData = [
        "server1": 19236,
        "server2": 21802,
        "server3": 21468,
        "server4": 17602,
        "server5": 19892,
    ]

    override func setUp() {
        super.setUp()
    }
    
    override func tearDown() {
        super.tearDown()
    }
    
    func testHash() {
        for (key, expected) in testBloomHashData {
            let the_key    = [UInt8](key.utf8)
            let the_expect = UInt32(expected)
            let hash       = leveldb_bloom_hash(the_key)
            
            XCTAssert(hash == the_expect, "wrong hash for key \(key), was: \(hash) expected: \(expected)")
        }

    }
    
    func testCHashInitialization() {
        let nodes = testCHashData.keys.sort()

        let chash = CHash(
            nodes:    nodes,
            replicas: 160
        )
        
        for (i, (node, point)) in testCHashInit.enumerate() {
            let data = chash.buckets[i]
            let test = node == data.name && point == Int(data.point)
            
            XCTAssert(test, "bucket at index \(i) KO, should've been \(node), \(point) -- was -- \(data)")
        }
    }
    
    func testCHash() {
        let nodes = testCHashData.keys.sort()
        
        let chash = CHash(
            nodes:    nodes,
            replicas: 160
        )
        
        for (i, (node, point)) in testCHashInit.enumerate() {
            let data = chash.buckets[i]
            let test = node == data.name && point == Int(data.point)
            
            XCTAssert(test, "bucket at index \(i) KO, should've been \(node), \(point) -- was -- \(data)")
        }

        var results = [String: Int]()
        
        for node in nodes {
            results[node] = 0
        }
        
        for i in 0..<100000 {
            let key = [UInt8]("foo\(i)\n".utf8)
            let node = chash.lookup(key)
            
            results[node]?++
        }
        
        
        for node in nodes {
            XCTAssert(results[node] == testCHashData[node], "wrong for node \(node), was \(results[node]) expected\(testCHashData[node])")
        }
    }
    
    func testGetMessage() {
        let test : [UInt8] = [
            0x73,
            0x68,
            0x63,
            0x01,
            0x01,
            0x00,
            0x03,
            0x61,
            0x62,
            0x63,
            0x00,
            0x00,
            0x00,
        ]
        
        var m = Message(message:.GET)
        m.addRecord([UInt8]("abc".utf8))
        m.end()
        
        XCTAssert(test == m.packet)
    }
    
    func testSetMessage() {
        let test : [UInt8] = [
            0x73,
            0x68,
            0x63,
            0x01,
            0x02,
            0x00,
            0x03,
            0x61,
            0x62,
            0x63,
            0x00,
            0x00,
            0x80,
            0x00,
            0x03,
            0x64,
            0x65,
            0x66,
            0x00,
            0x00,
            0x00,
        ]
        
        var m = Message(message:.SET)
        m.addRecord([UInt8]("abc".utf8))
        m.addRecord([UInt8]("def".utf8))
        m.end()
        
        XCTAssert(test == m.packet)
    }
    
    func testStatsMessage() {
        let test : [UInt8] = [
            0x73,
            0x68,
            0x63,
            0x01,
            0x32,
            0x00,
            0x00,
            0x00,
        ]
        
        var m = Message(message:.STS)
        m.end()
        
        XCTAssert(test == m.packet)
    }
    
    func testStocaz() {
        let shc = ShardcacheClient(nodes: ["suca": ("localhost", 6969)])
        shc.get("suca")
        
    }
}
