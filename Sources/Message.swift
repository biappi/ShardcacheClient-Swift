//
//  Message.swift
//  ShardcacheClient
//
//  Created by Antonio Malara on 8/22/15.
//  Copyright © 2015 Antonio Malara. All rights reserved.
//

import Foundation

let PROTOCOL_1_HEADER : [UInt8] = [
    0x73,
    0x68,
    0x63,
    0x01
]

let RECORD_SEPARATOR  :  UInt8  =  0x80
let RECORD_TERMINATOR : [UInt8] = [0x00, 0x00]

let CHUNK_SIZE = 0xFFFF

enum MessageType : UInt8 {
    case GET  = 0x01
    case SET  = 0x02
    case DEL  = 0x03
    case EVI  = 0x04
    case OFX  = 0x06
    case ADD  = 0x07
    case EXI  = 0x08
    
    case CHK  = 0x31
    case STS  = 0x32
    
    case SIG  = 0xF0
    case CSIG = 0xF1
    
    case NOOP = 0x90
}

func toUInt8(n : Int) -> [UInt8] {
    return [
        UInt8(n >> 8) & 0xFF,
        UInt8(n     ) & 0xFF
    ]
}

struct Message {
    var packet : [UInt8]
    
    var recordAdded = 0
    var ended       = false
    
    init(message: MessageType) {
        packet = PROTOCOL_1_HEADER
        packet.append(message.rawValue)
    }
    
    mutating func addRecord(data: [UInt8]) {
        precondition(ended == false)
        
        defer { recordAdded++ }
        
        if recordAdded != 0 {
            packet.append(RECORD_SEPARATOR)
        }
        
        let fullChunksNumber = data.count / CHUNK_SIZE
        let lastChunkSize    = data.count % CHUNK_SIZE
        
        for i in 0..<fullChunksNumber {
            let toCopyStart = CHUNK_SIZE * i
            let toCopyEnd   = CHUNK_SIZE * (i + 1)
            
            packet.extend(toUInt8(CHUNK_SIZE))
            packet.extend(data[toCopyStart..<toCopyEnd])
        }
        
        let lastCopyStart = CHUNK_SIZE * fullChunksNumber
        let lastCopyEnd   = data.count
        
        packet.extend(toUInt8(lastChunkSize))
        packet.extend(data[lastCopyStart..<lastCopyEnd])
        
        packet.extend(RECORD_TERMINATOR)
    }
    
    mutating func end() {
        precondition(ended == false)
        ended = true
        
        if recordAdded == 0 {
            packet.extend(RECORD_TERMINATOR)
        }
        
        packet.append(0)
    }
}