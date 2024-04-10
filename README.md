# M5: Distributed Execution Engine

> Full name: `Leishu Qiu`
> Email: `leishu_qiu@brown.edu`
> Username: `lqiu13`

## Summary

> Summarize your implementation, including key challenges you encountered

My implementation comprises `3` new software components, totaling `200` added lines of code over the previous implementation. Key challenges included `shuffling the key value pairs, coordinating communications between master and worker nodes, dealing with data structures and restructuring dataset into required type`.

## Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation

_Correctness_: was verified through unit testing. Challenges included simulating distributed environments in tests and ensuring data consistency. I developed additional tests to cover edge cases.

_Performance_: was evaluated by benchmarking task execution times. measured throughput and latency under varying loads.

## Key Feature

> Which extra features did you implement and how?

The memory optimization feature enables optional in-memory storage for intermediate results. It's implemented by checking the `memory` flag in the `mr.exec` function, choosing between `mem` for in-memory and `store` for persistent storage. This feature speeds up data processing, particularly for large datasets.

## Time to Complete

> Roughly, how many hours did this milestone take you to complete?

Hours: `<15 hour>`
