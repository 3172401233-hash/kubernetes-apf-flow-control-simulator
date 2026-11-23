# kubernetes-apf-flow-control-simulator

模拟 Kubernetes API Priority and Fairness (APF) 的队列与公平调度。

**这是一个纯内存模拟器：不连接任何真实的 API Server，不发起网络请求，
仅用于离线研究 APF 的调度行为。**

## 模型

- `FlowSchema`：按 user / verb / resource / namespace 匹配请求，把请求分类到
  某个 `PriorityLevelConfiguration`，并用 flow distinguisher（user 或
  user+namespace）划分流。
- `PriorityLevelConfiguration`：`assured_concurrency_shares`（最小保障并发）、
  `lendable_percent`（可借出比例）、`borrowing_limit_percent`（可借入上限）、
  `queue_capacity`（队列容量，超出即拒绝）。
- 每个请求带 `work`（work estimate，占用一个 seat 的时间片数）、`arrive`
  （到达时间片）和 `timeout`（排队超时，超时即过期丢弃）。

## 调度（每个时间片）

1. 接纳到达请求：分类后入队，队列满则拒绝；
2. 清理排队超过 timeout 的过期请求；
3. 重算各优先级并发上限：先保证 assured shares（最小保障），空闲级别的
   lendable 份额汇入借用池，有未满足需求的级别按需求比例借入，受
   borrowing limit 约束；
4. 级别内用虚拟时间公平队列（start-time fair queuing）调度：按流的虚拟
   完成时间选队，请求的虚拟代价 = work estimate / 当前并发宽度——既不是
   按到达时间也不是按优先级简单排序；
5. 运行中的请求推进一个时间片，完成的释放 seat。运行中的请求不被抢占。

**防突发**：空闲后恢复的流，其虚拟时间被提升到队列集合的当前虚拟时间，
不能利用空闲期积累信用一次性清空积压，只能与其他流交替逐步排空。

## 运行

```bash
python3 demo.py          # 打印每轮调度的请求、队列占用、借用量、被拒绝/过期请求
python3 -m unittest      # 运行测试
```

测试覆盖：双流竞争（防队头阻塞）、容量借用与回收、突发流量渐进排空、
work estimate 偏斜（按 seat-time 公平），以及队列容量拒绝与排队过期。

## 文件

- `apf/models.py` — FlowSchema / PriorityLevelConfiguration / Request 等数据模型
- `apf/fairqueue.py` — 虚拟时间公平队列
- `apf/scheduler.py` — 时间片调度器（借用、过期、调度循环）
- `tests/test_apf.py` — 单元测试
- `demo.py` — 示例场景

兼容 Python 3.8+。
