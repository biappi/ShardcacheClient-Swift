//
//  CHash.swift
//  ShardcacheClient
//
//  Created by Antonio Malara on 8/20/15.
//  Copyright © 2015 Antonio Malara. All rights reserved.
//

import Foundation

func leveldb_bloom_hash(key: [UInt8]) -> UInt32 {
    let seed : UInt32 = 0xbc9f1d34
    let m    : UInt32 = 0xc6a4a793
    
    var pos = 0
    var len = key.count
    
    var h : UInt32 = seed ^ UInt32(len) &* m
    
    while len >= 4 {
        h = h &+ (UInt32(key[pos + 0]) <<  0)
        h = h &+ (UInt32(key[pos + 1]) <<  8)
        h = h &+ (UInt32(key[pos + 2]) << 16)
        h = h &+ (UInt32(key[pos + 3]) << 24)
        
        h = h &* m
        h ^= (h >> 16)
        
        pos += 4
        len -= 4
    }
    
    switch len {
    case 3:
        h += UInt32(key[pos + 2]) << 16
        fallthrough
    case 2:
        h += UInt32(key[pos + 1]) << 8
        fallthrough
    case 1:
        h += UInt32(key[pos])
        h = h &* m
        h ^= h >> 24
    default:
        break
    }
    
    return h
}

public class CHash {
    struct CHashBucket {
        let name  : String
        let point : UInt32
    }
    
    var buckets : [CHashBucket]
    
    public init(nodes: [String], replicas: Int) {
        var buckets : [CHashBucket] = []
        for node in nodes {
            for i in 0..<replicas {
                let key = "\(i)\(node)"
                let point = leveldb_bloom_hash([UInt8](key.utf8))
                buckets.append(CHashBucket(name: node, point: point))
            }
        }
        
        self.buckets = buckets.sort { $0.point < $1.point }
    }
    
    public func lookup(key: [UInt8]) -> String {
        let point = leveldb_bloom_hash(key)
        
        var low   = 0
        var high  = self.buckets.count
        
        while low < high {
            let mid = low + ((high - low) / 2)
            if self.buckets[mid].point > point {
                high = mid
            }
            else {
                low = mid + 1
            }
        }
        
        if low >= self.buckets.count {
            low = 0
        }
        
        return self.buckets[low].name
    }
}
