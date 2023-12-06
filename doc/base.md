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
数据在流式任务中的状态。<br>
## 有状态处理有什么好处？
就像动物的记忆一样，状态好处多了去了。错误避免、信息复用、信息挖掘（实时模型）。
## Flink 如何设置状态和保存状态的？
通过恢复算子的状态和从 checkpointing 的某点重放数据，实时数据流任务能从失败处恢复并保持一致性（精确一次语义）。