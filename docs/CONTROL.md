# XTF 高级重试与频控策略

> 源码位置：[`core/control.py`](../core/control.py)

---

## 目录

- [1. 概述](#1-概述)
- [2. 重试策略](#2-重试策略)
  - [2.1 指数退避](#21-指数退避-exponential_backoff)
  - [2.2 线性增长](#22-线性增长-linear_growth)
  - [2.3 固定等待](#23-固定等待-fixed_wait)
- [3. 频控策略](#3-频控策略)
  - [3.1 固定等待](#31-固定等待-fixed_wait-1)
  - [3.2 滑动窗口](#32-滑动窗口-sliding_window)
  - [3.3 固定窗口](#33-固定窗口-fixed_window)
- [4. 策略组合](#4-策略组合)
- [5. 预置配置方案](#5-预置配置方案)
- [6. 参数参考](#6-参数参考)
- [7. 监控与调优](#7-监控与调优)

---

## 1. 概述

XTF 默认使用简单的固定延迟和固定重试机制。当需要更精细的控制时，可通过 `enable_advanced_control: true` 启用高级策略系统。

**架构**：

```
AdvancedController (线程安全单例)
  │
  ├─→ RetryStrategy (重试策略)
  │     选择一种：指数退避 / 线性增长 / 固定等待
  │
  └─→ RateLimitStrategy (频控策略)
        选择一种：固定等待 / 滑动窗口 / 固定窗口
```

**启用方式**：

```yaml
enable_advanced_control: true  # 开启高级控制
```

> 未启用时，系统使用 `rate_limit_delay` + `max_retries` 的简单模式。

---

## 2. 重试策略

重试策略决定 API 请求失败后的等待时间计算方式。

### 2.1 指数退避 (exponential_backoff)

每次重试的等待时间按乘数倍增：

```
等待时间 = initial_delay × multiplier^(attempt-1)

示例 (initial_delay=0.5, multiplier=2.0):
  第1次重试: 0.5s
  第2次重试: 1.0s
  第3次重试: 2.0s
  第4次重试: 4.0s
  第5次重试: 8.0s
  ...
  上限: retry_max_wait_time (如设置)
```

**特点**：
- 初始恢复快，后期等待长
- 适合应对突发性限流
- ⭐ 推荐作为默认重试策略

**配置**：

```yaml
retry_strategy_type: "exponential_backoff"
retry_initial_delay: 0.5    # 初始等待（秒）
retry_multiplier: 2.0       # 倍增系数
retry_max_wait_time: 60     # 最大等待时间（秒，可选）
```

### 2.2 线性增长 (linear_growth)

每次重试的等待时间增加固定步长：

```
等待时间 = initial_delay + increment × (attempt-1)

示例 (initial_delay=0.5, increment=0.5):
  第1次重试: 0.5s
  第2次重试: 1.0s
  第3次重试: 1.5s
  第4次重试: 2.0s
  第5次重试: 2.5s
```

**特点**：
- 等待时间增长平稳可预测
- 适合稳定的限流场景

**配置**：

```yaml
retry_strategy_type: "linear_growth"
retry_initial_delay: 0.5    # 初始等待（秒）
retry_increment: 0.5        # 每次增量（秒）
```

### 2.3 固定等待 (fixed_wait)

每次重试等待相同的时间：

```
等待时间 = initial_delay (固定)

示例 (initial_delay=1.0):
  第1次重试: 1.0s
  第2次重试: 1.0s
  第3次重试: 1.0s
```

**特点**：
- 行为最简单、最可预测
- 适合已知固定限流间隔的场景

**配置**：

```yaml
retry_strategy_type: "fixed_wait"
retry_initial_delay: 1.0    # 固定等待时间（秒）
```

---

## 3. 频控策略

频控策略控制 API 请求的发送频率，在请求之前主动限制速率。

### 3.1 固定等待 (fixed_wait)

每次请求之间等待固定时间：

```
请求1 → 等待 → 请求2 → 等待 → 请求3 → ...
       delay       delay
```

**配置**：

```yaml
rate_limit_strategy_type: "fixed_wait"
rate_limit_delay: 0.5  # 使用通用 rate_limit_delay 参数
```

### 3.2 滑动窗口 (sliding_window)

在滚动时间窗口内限制请求数量：

```
时间轴: ──────[─────window_size─────]──────→
              ↑ 窗口内请求数 ≤ max_requests

窗口随时间滑动，过期的请求自动移出统计。
```

**特点**：
- 精确控制请求速率
- 允许短时间内的突发请求
- ⭐ 推荐用于需要精确频控的场景

**配置**：

```yaml
rate_limit_strategy_type: "sliding_window"
rate_limit_window_size: 1.0    # 时间窗口（秒）
rate_limit_max_requests: 10    # 窗口内最大请求数
```

### 3.3 固定窗口 (fixed_window)

在固定时间段内限制请求数量，到期后重置计数：

```
时间轴: ──[──window_1──]──[──window_2──]──[──window_3──]──→
           max_requests   max_requests    max_requests
           ↑ 到期重置     ↑ 到期重置      ↑ 到期重置
```

**特点**：
- 实现简单，开销最小
- 窗口边界可能出现突发（两个窗口交界处）

**配置**：

```yaml
rate_limit_strategy_type: "fixed_window"
rate_limit_window_size: 1.0    # 窗口大小（秒）
rate_limit_max_requests: 10    # 每窗口最大请求数
```

---

## 4. 策略组合

3 种重试策略 × 3 种频控策略 = **9 种组合**。推荐的组合：

| 组合 | 重试策略 | 频控策略 | 适用场景 |
|------|----------|----------|----------|
| ⭐ **推荐** | 指数退避 | 滑动窗口 | 大多数生产场景 |
| 稳定型 | 线性增长 | 固定窗口 | 稳定的限流环境 |
| 简单型 | 固定等待 | 固定等待 | 简单场景、调试 |
| 激进型 | 指数退避 | 固定等待 | 需要最大吞吐量 |

---

## 5. 预置配置方案

### 保守方案 — 稳定优先

适合生产环境、大数据集、网络不稳定场景。

```yaml
enable_advanced_control: true
retry_strategy_type: "exponential_backoff"
retry_initial_delay: 1.0
retry_multiplier: 2.0
retry_max_wait_time: 60
rate_limit_strategy_type: "sliding_window"
rate_limit_window_size: 2.0
rate_limit_max_requests: 5
max_retries: 5
```

### 渐进方案 — 平衡模式

适合日常使用、中等数据量。

```yaml
enable_advanced_control: true
retry_strategy_type: "exponential_backoff"
retry_initial_delay: 0.5
retry_multiplier: 2.0
rate_limit_strategy_type: "sliding_window"
rate_limit_window_size: 1.0
rate_limit_max_requests: 10
max_retries: 3
```

### 激进方案 — 性能优先

适合网络良好、小数据集、限流宽松场景。

```yaml
enable_advanced_control: true
retry_strategy_type: "linear_growth"
retry_initial_delay: 0.2
retry_increment: 0.3
rate_limit_strategy_type: "fixed_wait"
rate_limit_delay: 0.1
max_retries: 3
```

### 调试方案 — 详细观察

适合排查问题、观察 API 行为。

```yaml
enable_advanced_control: true
retry_strategy_type: "fixed_wait"
retry_initial_delay: 2.0
rate_limit_strategy_type: "fixed_wait"
rate_limit_delay: 1.0
max_retries: 1
log_level: DEBUG
```

---

## 6. 参数参考

### 重试参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `retry_strategy_type` | `str` | `exponential_backoff` | 策略类型 |
| `retry_initial_delay` | `float` | `0.5` | 初始等待时间（秒） |
| `retry_multiplier` | `float` | `2.0` | 指数退避乘数 |
| `retry_increment` | `float` | `0.5` | 线性增长步长 |
| `retry_max_wait_time` | `float` | `null` | 最大等待时间（秒） |
| `max_retries` | `int` | `3` | 最大重试次数 |

### 频控参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `rate_limit_strategy_type` | `str` | `fixed_wait` | 策略类型 |
| `rate_limit_window_size` | `float` | `1.0` | 时间窗口大小（秒） |
| `rate_limit_max_requests` | `int` | `10` | 窗口内最大请求数 |
| `rate_limit_delay` | `float` | `0.5` | 固定等待延迟（秒） |

---

## 7. 监控与调优

### 日志观察

启用 `DEBUG` 日志级别可以观察重试和频控行为：

```
DEBUG - 重试策略: exponential_backoff, 当前延迟: 2.0s, 重试次数: 3/5
DEBUG - 频控策略: sliding_window, 窗口内请求: 8/10, 等待: 0.2s
INFO  - 批量操作完成: 500/500 条记录, 重试 2 次
```

### 调优建议

| 问题 | 调优方向 |
|------|----------|
| 重试次数耗尽仍失败 | 增大 `max_retries`，增大 `retry_initial_delay` |
| 等待时间过长 | 减小 `retry_multiplier`，减小 `retry_max_wait_time` |
| 限流频繁触发 | 减小 `rate_limit_max_requests`，增大 `rate_limit_window_size` |
| 吞吐量不足 | 增大 `rate_limit_max_requests`，减小延迟 |
| 突发限流 | 从固定窗口切换到滑动窗口 |

### 飞书 API 限制参考

| 接口 | 限制 | 建议 |
|------|------|------|
| 多维表格批量操作 | ~100 次/秒 | `rate_limit_max_requests: 10-20` |
| 电子表格读写 | ~100 次/秒 | `rate_limit_max_requests: 10-20` |
| 单次请求体积 | ≤ 10MB | 使用分块 + 二分重试 |
