# Flink 基本概念

## 分布式流处理模型
[数据流模型:在大规模、无界、乱序数据处理中平衡正确性、延迟和成本的实用方法](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/43864.pdf)，<wbr>
分为三大块步骤 Data Source (数据输入)、DataStream Transformations (数据计算)、Data Sinks (数据输出)，source、transformation、sink
都是 Operation，具有以下特点：
1. 数据从上一个 Operation 节点直接 Push 到下一个 Operation 节点
2. 拥有多个节点，可以并行执行，数据在 Operation 之间流转
3. 有 shuffle 操作，主动将数据从上游 push 到下游（MapReduce于此相反）
4. Flink 是基于这种理念的实现框架   

**思考：** 为什么同样是 shuffle 操作，Dataflow Model 与 MapReduce 数据分发逻辑相反？

## 批处理与流处理
批处理：数据处理任务执行前，所要输入的数据集大小就是已知且固定不变了  
流处理：数据处理任务执行前，不知道输入数据集大小，任务执行中数据在持续输入，持续增加，理论是像流水一样无穷无尽

## 什么是有状态处理？
在流式任务中，会记录一条数据在当前算子和 Task 中与环境的关系，举个例子，一个扣费事件数据，只能消费一次，不能不消费或多次消费，这样就必须记录这条<wbr>
数据在流式任务中的状态。

## 有状态处理有什么好处？
就像动物的记忆一样，状态好处多了去了。错误避免、信息复用、信息挖掘（实时模型）。

## Flink 如何设置状态和保存状态的？
Flink 的 checkpointing 是一种快照机制。
通过恢复算子的状态和从 checkpointing 的某点重放数据(任务失败重启后，从未消费的偏移量继续消费待消费的数据)，实时数据流任务能从失败处恢复并保持一致性（精确一次语义）。<wbr>
checkpointing 的过程是周期性不间断的输出 snapshot 到外部存储系统，保存实时流任务的状态。

## checkpointing 的对齐
屏障：Flink 会在 source 算子处插入屏障，假设插入的屏障从早到晚分别是 0、1、2、...、n、n+1，其中屏障 n 是 snapshot 的 ID，表示从 n-1 到 n <wbr>
的的数据已经录其状态。<br>
屏障对齐过程：下游算子对来自上游算子的并行输入，当并行输入流的屏障 n 都到达了下游算子，下游算子才会处理来自屏障 n-1 到 n 的数据。<br>
写 snapshot：当屏障对齐后，下游算子回消费这些数据，消费完后，异步写 snapshot 到外部存储系统。<br>
确认写 snapshot n 的完成：当 sink 算子的并行任务都接受到屏障 n 并报告给 JobManger，可以认为写 snapshot n 完成。<br>
写 snapshot n 的完成的意义：表示屏障 n 之前的数据都被处理了，不需要再做处理。

对齐发生的条件：下游算子是 shuffle 读来自上游的数据。

## exactly once 与 at least once
精确一次是有且只有一次，不多也不少；最少一次是会出现一次或多次的情况。