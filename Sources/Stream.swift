//
//  Stream.swift
//  ShardcacheClient
//
//  Created by Antonio Malara on 8/21/15.
//  Copyright © 2015 Antonio Malara. All rights reserved.
//

import Foundation

class Stream : NSObject, NSStreamDelegate {
    var input  : NSInputStream
    var output : NSOutputStream
    var toSend : [UInt8]?
    
    init(host: String, port: Int) {
        var input  : NSInputStream?
        var output : NSOutputStream?
        
        NSStream.getStreamsToHostWithName(
            host,
            port:         port,
            inputStream:  &input,
            outputStream: &output
        )
        
        self.input  = input!
        self.output = output!
    }
    
    func connect() {
        input.delegate  = self;
        output.delegate = self;
        
        input.scheduleInRunLoop(
            NSRunLoop.currentRunLoop(),
            forMode:NSRunLoopCommonModes
        )
        
        output.scheduleInRunLoop(
            NSRunLoop.currentRunLoop(),
            forMode: NSRunLoopCommonModes
        )
        
        input.open()
        output.open()
    }
    
    func stream(aStream: NSStream, handleEvent eventCode: NSStreamEvent) {
        print("stream: \(aStream), eventcode: \(eventCode)")

        if aStream == input {
            handleInputEvent(eventCode)
        }
        
        if aStream == output {
            handleOutputEvent(eventCode)
        }
    }
    
    func handleInputEvent(eventCode: NSStreamEvent) {
        switch eventCode {
        case NSStreamEvent.None:
            print("None")
            
        case NSStreamEvent.OpenCompleted:
            print("Open Completed")
            
        case NSStreamEvent.HasBytesAvailable:
            print("HasBytesAvailable")
            
        case NSStreamEvent.HasSpaceAvailable:
            print("HasSpaceAvailable")
            
        case NSStreamEvent.EndEncountered:
            print("EndEncountered")
            
        case NSStreamEvent.ErrorOccurred:
            print("ErrorOccurred")
            print (input.streamStatus.rawValue)
            print (input.streamError)
            
        default:
            assertionFailure("nada")
        }
    }
    
    func handleOutputEvent(eventCode: NSStreamEvent) {
        switch eventCode {
        case NSStreamEvent.None:
            print("None")
            
        case NSStreamEvent.OpenCompleted:
            print("Open Completed")
            
        case NSStreamEvent.HasBytesAvailable:
            print("HasBytesAvailable")
            
        case NSStreamEvent.HasSpaceAvailable:
            if let toSend = toSend {
                let sent = output.write(toSend, maxLength: toSend.count)
                self.toSend = [UInt8](toSend[sent..<toSend.count])
            }
            
        case NSStreamEvent.EndEncountered:
            print("EndEncountered")
            
        case NSStreamEvent.ErrorOccurred:
            print("ErrorOccurred")
            print (output.streamStatus.rawValue)
            print (output.streamError)
            
        default:
            assertionFailure("nada")
        }
    }
    
    func send(data:[UInt8]) {
        if var toSend = toSend {
            toSend.extend(data)
        }
        else {
            let sent = output.write(data, maxLength: data.count)
            
            if sent != data.count {
                toSend = [UInt8](data[sent..<data.count])
            }
            
            if sent == -1 {
                //TODO: error
                assertionFailure("nada")
            }
        }
    }
}
