//
//  main.swift
//  shardcacheclient-example
//
//  Created by Antonio Malara on 8/22/15.
//  Copyright © 2015 Antonio Malara. All rights reserved.
//

import ShardcacheClient
import Foundation

let x = ShardcacheClient(nodes: ["me": ("localhost", 6969)])

//x.set("suca", value: "milla")
x.get("suca")

NSRunLoop.currentRunLoop().run()