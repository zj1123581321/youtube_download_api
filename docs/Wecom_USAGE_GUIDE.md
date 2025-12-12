# 企业微信通知器使用指南

## 📌 重要提示

> **⚠️ 使用前必读**
>
> - ✅ **推荐**：全局使用单个 `WeComNotifier` 实例
> - ❌ **避免**：频繁创建多个实例（会导致频控失效、资源浪费）
> - 📖 详见下方"最佳实践"章节

## 🎯 快速开始

### 安装

```bash
pip install -U wecom-notifier
```

### 最简单的例子

```python
from wecom_notifier import WeComNotifier

# 1. 初始化
notifier = WeComNotifier()

# 2. 发送消息
result = notifier.send_text(
    webhook_url="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR-KEY",
    content="Hello 企业微信！"
)

# 3. 检查结果
if result.is_success():
    print("发送成功！")
else:
    print(f"发送失败: {result.error}")
```

## 📚 功能详解

### 1. 文本消息

#### 基础文本
```python
notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="这是一条普通消息"
)
```

#### 带@all
```python
notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="紧急通知！",
    mentioned_list=["@all"]  # @所有人
)
```

#### @特定用户
```python
notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="任务分配通知",
    mentioned_list=["user1", "user2"],  # 按用户ID
    mentioned_mobile_list=["13800138000"]  # 按手机号
)
```

### 2. Markdown消息

#### 基础Markdown
```python
markdown_content = """
# 项目上线通知

## 更新内容
- **新功能**: 用户导出
- **优化**: 性能提升50%

## 测试结果
| 测试项 | 结果 |
|--------|------|
| 单元测试 | 通过 |
| 集成测试 | 通过 |

[查看详情](https://example.com)
"""

notifier.send_markdown(
    webhook_url=WEBHOOK_URL,
    content=markdown_content
)
```

#### Markdown + @all
```python
notifier.send_markdown(
    webhook_url=WEBHOOK_URL,
    content="# 重要通知\n\n服务器将在30分钟后维护",
    mention_all=True  # 会额外发送一条@all的text消息
)
```

### 3. 图片消息

#### 通过文件路径
```python
notifier.send_image(
    webhook_url=WEBHOOK_URL,
    image_path="report.png"
)
```

#### 通过Base64
```python
notifier.send_image(
    webhook_url=WEBHOOK_URL,
    image_base64="iVBORw0KGgoAAAANS...",  # base64字符串
    mention_all=True
)
```

### 4. 同步vs异步

#### 异步发送（默认，推荐）
```python
# 立即返回，不等待发送完成
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="异步消息",
    async_send=True  # 默认值
)

# 可以选择等待
result.wait(timeout=30)  # 最多等30秒
if result.is_success():
    print("发送成功")
```

#### 同步发送
```python
# 阻塞等待发送完成
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="同步消息",
    async_send=False
)

# 立即知道结果
if result.is_success():
    print("确认发送成功")
else:
    print(f"发送失败: {result.error}")
```

### 5. 长文本自动分段

```python
# 超过4096字节会自动分段
long_text = "\n".join([f"第{i}行" for i in range(1000)])

result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content=long_text
)

# 会自动分成多条消息发送
# 每条消息会带有"（续上页）"或"（未完待续）"标记
```

### 6. 表格智能分段

```python
# 超长表格会保留表头分段
table = """
| ID | 名称 | 数据 |
|----|------|------|
""" + "\n".join([f"| {i} | Item{i} | Data{i} |" for i in range(200)])

notifier.send_markdown(
    webhook_url=WEBHOOK_URL,
    content=table
)

# 每个分段都会保留表头
# 自动添加续页提示
```

### 7. 并发发送

```python
# 异步发送多条消息
results = []

for i in range(10):
    result = notifier.send_text(
        webhook_url=WEBHOOK_URL,
        content=f"消息 {i}",
        async_send=True
    )
    results.append(result)

# 等待所有完成
for result in results:
    result.wait()
    print(f"状态: {result.is_success()}")
```

### 8. 多Webhook管理

#### 方式1：向不同webhook发送不同消息

```python
# 同一个notifier实例可以管理多个webhook
webhooks = {
    "开发群": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=DEV-KEY",
    "测试群": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=TEST-KEY",
    "生产群": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=PROD-KEY",
}

# 每个webhook自动独立管理频率限制
for name, url in webhooks.items():
    notifier.send_text(
        webhook_url=url,
        content=f"发送到{name}"
    )
```

#### 方式2：Webhook池 - 突破单webhook频率限制（新功能）

**适用场景：批量数据推送、高频通知**

当你需要每分钟发送超过20条消息时，可以使用webhook池来突破单webhook的频率限制。

**原理**：
- 单个webhook：20条/分钟
- 3个webhook池：60条/分钟
- 10个webhook池：200条/分钟
- **理论无上限**（添加更多webhook即可）

**使用方法**：

```python
from wecom_notifier import WeComNotifier

notifier = WeComNotifier()

# 在同一个群聊中添加多个机器人，获取多个webhook地址
webhook_pool = [
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=KEY1",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=KEY2",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=KEY3"
]

# 传入webhook列表，系统会自动负载均衡
result = notifier.send_text(
    webhook_url=webhook_pool,  # 传入列表而非字符串
    content="很长的消息内容..." * 100,
    async_send=False
)

# 检查结果
if result.is_success():
    print(f"发送成功！")
    print(f"使用的webhooks数量: {len(result.used_webhooks)}")
    print(f"消息分段数: {result.segment_count}")
```

**核心特性**：

1. **智能负载均衡**（最空闲优先策略）
   - 系统自动选择配额最多的webhook发送
   - 确保负载均匀分布在所有webhook上

2. **消息顺序保证**
   - 单线程串行处理，严格保证消息顺序
   - 同一消息的分段可以跨webhook发送
   - 在群里阅读时顺序完全正确

3. **自动容错恢复**
   - webhook失败自动切换到其他可用webhook
   - 失败的webhook进入冷却期（10秒、20秒、40秒递增）
   - 冷却期过后自动恢复使用

4. **全局频控共享**
   - 同一webhook在单webhook和池模式下共享频率限制
   - 避免冲突和重复计数

**高频批量发送示例**：

```python
# 每分钟发送60条消息（3个webhook池）
notifier = WeComNotifier()

webhook_pool = [
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=KEY1",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=KEY2",
    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=KEY3"
]

# 批量发送60条消息
results = []
for i in range(60):
    result = notifier.send_text(
        webhook_url=webhook_pool,
        content=f"批量消息 {i+1}/60",
        async_send=True
    )
    results.append(result)

# 等待所有消息完成
for result in results:
    result.wait()

# 统计
success_count = sum(1 for r in results if r.is_success())
print(f"成功: {success_count}/{len(results)}")
```

**性能对比**：

| 模式 | Webhook数量 | 理论吞吐量 | 实际测试 |
|------|------------|-----------|---------|
| 单webhook | 1个 | 20条/分钟 | 20条/60秒 |
| Webhook池 | 3个 | 60条/分钟 | 60条/92秒 |
| Webhook池 | 10个 | 200条/分钟 | 未测试 |

**注意事项**：

1. **必须在同一个群聊中添加多个机器人**
   - 确保消息发送到同一个聊天窗口
   - 这样消息才能按顺序显示

2. **向后兼容**
   - 传入字符串：单webhook模式（原有行为）
   - 传入列表：webhook池模式（新功能）

3. **返回值扩展**
   - `result.used_webhooks`: 实际使用的webhook URL列表
   - `result.segment_count`: 分段数量

**错误处理**：

```python
# 空列表会抛出异常
try:
    notifier.send_text(webhook_url=[], content="消息")
except Exception as e:
    print(f"错误: {e}")  # InvalidParameterError: webhook_url list cannot be empty

# 无效类型会抛出异常
try:
    notifier.send_text(webhook_url=123, content="消息")
except Exception as e:
    print(f"错误: {e}")  # InvalidParameterError: webhook_url must be str or list
```

### 9. 自定义配置

```python
notifier = WeComNotifier(
    max_retries=5,         # HTTP请求最大重试次数（默认3）
    retry_delay=3.0        # 重试延迟秒数（默认2.0）
)
```

**注意**：v0.2.0+ 已移除 `log_level` 参数，日志配置请参考下方"日志配置"章节。

### 10. 日志配置

**重要变更（v0.2.0+）**：本库不再自动配置日志，由用户完全控制。

#### 方式1：使用库提供的快速配置（推荐新手）

```python
from wecom_notifier import WeComNotifier, setup_logger

# 在创建 notifier 之前配置日志
setup_logger(log_level="INFO")  # 输出到控制台

notifier = WeComNotifier()
```

#### 方式2：在应用层统一配置（推荐生产环境）

```python
from loguru import logger
from wecom_notifier import WeComNotifier

# 配置应用的全局日志（包括本库）
logger.add(
    "app.log",
    level="INFO",
    rotation="10 MB",
    retention="7 days"
)

notifier = WeComNotifier()
```

#### 方式3：完全静默（不输出日志）

```python
from wecom_notifier import WeComNotifier, disable_logger

disable_logger()  # 完全禁用本库日志
notifier = WeComNotifier()
```

#### 高级配置选项

```python
from wecom_notifier import setup_logger

setup_logger(
    log_level="DEBUG",           # 日志级别：DEBUG/INFO/WARNING/ERROR
    add_console=True,            # 是否输出到控制台
    add_file=True,               # 是否输出到文件
    log_file="wecom.log",        # 日志文件路径
    colorize=True                # 控制台是否启用颜色
)
```

**更多详细信息**，请参考：
- [日志配置指南](doc/logging_configuration_guide.md) - 完整的日志配置文档
- [README.md - 日志配置](README.md#日志配置) - 快速参考

### 11. 内容审核（可选功能）

**适用场景**：需要过滤敏感词的消息发送

内容审核功能可以在消息发送前自动检测和处理敏感内容，支持三种策略：
- **Block（拒绝）**：检测到敏感词时拒绝发送，发送告警消息
- **Replace（替换）**：将敏感词替换为 `[敏感词]`
- **PinyinReverse（拼音混淆）**：将敏感词转换为拼音混淆形式

#### 快速开始

```python
from wecom_notifier import WeComNotifier

# 启用内容审核（替换策略）
notifier = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": [
            "https://example.com/sensitive_words.txt"  # 敏感词库URL
        ],
        "strategy": "replace",  # 策略：block, replace, pinyin_reverse
    }
)

# 正常发送，敏感词会自动处理
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="这是一条可能包含敏感词的消息"
)
```

#### 策略1：Block（拒绝发送）

检测到敏感词时拒绝发送，并发送告警消息（仅包含前50个字符）

```python
notifier = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": [
            "https://raw.githubusercontent.com/konsheng/Sensitive-lexicon/refs/heads/main/Vocabulary/%E6%96%B0%E6%80%9D%E6%83%B3%E5%90%AF%E8%92%99.txt"
        ],
        "strategy": "block",
    }
)

# 发送包含敏感词的消息
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="这是一条包含梭哈买房的讨论消息",
    async_send=False
)

# 结果：消息被拒绝
if not result.is_success():
    print(f"消息被拒绝: {result.error}")
    # 输出: "消息被拒绝: Content blocked by moderator"

# 群里会收到告警消息：
# "[内容审核告警] 检测到敏感词: 梭哈买房
#  原始内容前50字符: 这是一条包含梭哈买房的讨论消息"
```

#### 策略2：Replace（替换敏感词）

将敏感词替换为 `[敏感词]` 后正常发送

```python
notifier = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": [
            "https://raw.githubusercontent.com/konsheng/Sensitive-lexicon/refs/heads/main/Vocabulary/%E6%96%B0%E6%80%9D%E6%83%B3%E5%90%AF%E8%92%99.txt"
        ],
        "strategy": "replace",
    }
)

# 发送包含敏感词的消息
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="讨论梭哈买房和供养者思维的话题",
    async_send=False
)

# 结果：消息成功发送
if result.is_success():
    print("消息已发送（敏感词已替换）")

# 群里会收到:
# "讨论[敏感词]和[敏感词]的话题"
```

#### 策略3：PinyinReverse（拼音混淆）

将敏感词转换为拼音混淆形式（中文转拼音首字母倒序，英文字母倒序）

```python
notifier = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": [
            "https://raw.githubusercontent.com/konsheng/Sensitive-lexicon/refs/heads/main/Vocabulary/%E6%96%B0%E6%80%9D%E6%83%B3%E5%90%AF%E8%92%99.txt"
        ],
        "strategy": "pinyin_reverse",
    }
)

# 发送包含敏感词的消息
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="讨论梭哈买房的问题",
    async_send=False
)

# 结果：消息成功发送
if result.is_success():
    print("消息已发送（敏感词已混淆）")

# 群里会收到:
# "讨论fmshs的问题"
# 说明：梭哈买房 → shāhǎmǎifáng → shsmf → fmshs（倒序）
```

#### 敏感词文件格式

支持从URL加载TXT格式的敏感词库，格式要求：

```text
# 这是注释，会被忽略
# 每行一个敏感词，支持中英文

梭哈买房
梭哈结婚
供养者思维

# 空行会被忽略

# 会自动去除首尾空格
  空格敏感词
```

**加载机制**：
1. 启动时从URL加载敏感词
2. 加载成功：更新本地缓存（`.wecom_cache/sensitive_words_xxx.txt`）
3. 加载失败：使用本地缓存（如果存在）
4. 检测特性：
   - 部分匹配（子串匹配）
   - 不区分大小写
   - 高性能：使用AC自动机算法，支持1000+敏感词，检测时间 < 1ms

#### 敏感消息日志

自动记录检测到敏感词的消息（不记录普通消息），方便审计和分析

**启用日志**：

```python
notifier = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": ["https://example.com/sensitive_words.txt"],
        "strategy": "replace",
        "log_sensitive_messages": True,  # 启用日志（默认True）
        "log_file": ".wecom_cache/moderation.log",  # 日志文件路径
        "log_max_bytes": 10 * 1024 * 1024,  # 单文件最大10MB
        "log_backup_count": 5,  # 保留5个备份文件
    }
)
```

**日志格式**（JSON Lines，每行一条记录）：

```json
{"timestamp": "2025-10-29 17:53:39.140", "message_id": "2b81f971-xxx", "strategy": "replace", "msg_type": "text", "detected_words": ["梭哈结婚"], "original_content": "这是第一条关于梭哈结婚的测试消息"}
{"timestamp": "2025-10-29 17:53:41.962", "message_id": "074e5895-xxx", "strategy": "replace", "msg_type": "text", "detected_words": ["供养者思维"], "original_content": "这是第二条关于供养者思维的测试消息"}
```

**字段说明**：
- `timestamp`: 记录时间
- `message_id`: 消息唯一ID
- `strategy`: 使用的审核策略
- `msg_type`: 消息类型（text/markdown/image）
- `detected_words`: 检测到的敏感词列表（去重）
- `original_content`: 原始消息内容（仅记录原始内容，不记录审核后的）

**查询日志示例**：

```bash
# 查看所有敏感消息记录
cat .wecom_cache/moderation.log

# 查看最近10条
tail -n 10 .wecom_cache/moderation.log

# 查询包含特定敏感词的记录
grep "梭哈买房" .wecom_cache/moderation.log

# 统计检测到的敏感词频率
cat .wecom_cache/moderation.log | jq -r '.detected_words[]' | sort | uniq -c | sort -rn

# 按日期过滤（需要 jq）
cat .wecom_cache/moderation.log | jq 'select(.timestamp | startswith("2025-10-29"))'
```

**日志轮换**：
- 当日志文件达到 `log_max_bytes` 时自动轮换
- 轮换后的文件：`moderation.log.1`, `moderation.log.2`, ...
- 保留最近 `log_backup_count` 个备份文件
- 超出的旧文件自动删除

#### 禁用日志

```python
notifier = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": ["https://example.com/sensitive_words.txt"],
        "strategy": "replace",
        "log_sensitive_messages": False,  # 禁用日志
    }
)
```

#### 完整配置示例

```python
from wecom_notifier import WeComNotifier, setup_logger

# 配置日志（可选）
setup_logger(log_level="INFO")

notifier = WeComNotifier(
    # 基础配置
    max_retries=3,
    retry_delay=2.0,

    # 内容审核配置
    enable_content_moderation=True,
    moderation_config={
        # 敏感词库（支持多个URL）
        "sensitive_word_urls": [
            "https://raw.githubusercontent.com/konsheng/Sensitive-lexicon/refs/heads/main/Vocabulary/%E6%96%B0%E6%80%9D%E6%83%B3%E5%90%AF%E8%92%99.txt",
            "https://example.com/custom_words.txt",
        ],

        # 审核策略
        "strategy": "replace",  # block | replace | pinyin_reverse

        # 缓存配置
        "cache_dir": ".wecom_cache",  # 敏感词缓存目录
        "url_timeout": 10,  # URL请求超时（秒）

        # 日志配置
        "log_sensitive_messages": True,  # 是否记录敏感消息
        "log_file": ".wecom_cache/moderation.log",  # 日志文件路径
        "log_max_bytes": 10 * 1024 * 1024,  # 单文件最大10MB
        "log_backup_count": 5,  # 保留5个备份
    }
)

# 正常使用
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="任何内容都会自动审核"
)
```

#### 注意事项

1. **性能影响**：
   - 启用审核会增加每条消息约 1ms 的处理时间（1000个敏感词）
   - 使用AC自动机算法，时间复杂度 O(n)，与敏感词数量无关

2. **敏感词更新**：
   - 只在启动时加载一次敏感词
   - 如需更新敏感词，需要重启应用
   - 可以手动删除缓存文件强制重新加载

3. **缓存位置**：
   - 默认缓存目录：`.wecom_cache/`
   - 敏感词文件：`sensitive_words_<hash>.txt`（hash基于URL生成）
   - 日志文件：`moderation.log`（及轮换文件）

4. **审核时机**：
   - 在消息分段之后进行审核
   - 每个分段独立审核
   - 分段中检测到敏感词会单独处理

5. **Markdown消息**：
   - 审核的是原始Markdown文本，不解析格式
   - 敏感词可能出现在标题、列表、代码块等任何位置

## 🔍 常见场景

### 场景1：定时任务通知

```python
def send_task_notification(task_name, status, details):
    """发送任务通知"""
    notifier = WeComNotifier()

    content = f"""# 定时任务通知

**任务名称**: {task_name}
**执行状态**: {status}

## 详细信息
{details}
"""

    result = notifier.send_markdown(
        webhook_url=WEBHOOK_URL,
        content=content,
        mention_all=(status == "失败")  # 失败时@all
    )

    return result.is_success()

# 使用
send_task_notification("数据同步", "成功", "同步了1000条记录")
```

### 场景2：异常告警

```python
def send_error_alert(error_msg, traceback_str):
    """发送错误告警"""
    notifier = WeComNotifier()

    # 第一条：简要告警（@all）
    notifier.send_text(
        webhook_url=WEBHOOK_URL,
        content=f"❌ 系统异常：{error_msg}",
        mentioned_list=["@all"]
    )

    # 第二条：详细堆栈
    notifier.send_text(
        webhook_url=WEBHOOK_URL,
        content=f"详细堆栈:\n{traceback_str}"
    )

# 使用
try:
    # 你的代码
    risky_operation()
except Exception as e:
    import traceback
    send_error_alert(str(e), traceback.format_exc())
```

### 场景3：数据报表

```python
def send_daily_report(data):
    """发送每日数据报表"""
    notifier = WeComNotifier()

    # 生成表格
    table = f"""# 每日数据报表

| 指标 | 今日 | 昨日 | 增长率 |
|------|------|------|--------|
| 用户数 | {data['users_today']} | {data['users_yesterday']} | {data['user_growth']}% |
| 订单数 | {data['orders_today']} | {data['orders_yesterday']} | {data['order_growth']}% |
| 销售额 | ¥{data['revenue_today']} | ¥{data['revenue_yesterday']} | {data['revenue_growth']}% |

生成时间: {data['timestamp']}
"""

    notifier.send_markdown(
        webhook_url=WEBHOOK_URL,
        content=table
    )
```

### 场景4：批量通知（带频率控制）

```python
def send_batch_notifications(user_list):
    """批量发送通知（自动频率控制）"""
    notifier = WeComNotifier()

    for user in user_list:
        # 不用担心超频，会自动限速
        notifier.send_text(
            webhook_url=WEBHOOK_URL,
            content=f"Hi {user['name']}，你的任务已分配",
            mentioned_list=[user['userid']],
            async_send=True  # 异步，不阻塞
        )

    print(f"已提交{len(user_list)}条通知到队列")
```

### 场景5：用户反馈收集（带内容审核）

```python
def send_user_feedback_with_moderation(feedback_data):
    """收集并转发用户反馈（自动过滤敏感内容）"""

    # 启用内容审核，替换策略
    notifier = WeComNotifier(
        enable_content_moderation=True,
        moderation_config={
            "sensitive_word_urls": [
                "https://raw.githubusercontent.com/konsheng/Sensitive-lexicon/refs/heads/main/Vocabulary/%E6%96%B0%E6%80%9D%E6%83%B3%E5%90%AF%E8%92%99.txt"
            ],
            "strategy": "replace",
            "log_sensitive_messages": True,  # 记录敏感反馈用于分析
        }
    )

    # 构建反馈消息
    content = f"""# 用户反馈

**用户ID**: {feedback_data['user_id']}
**反馈时间**: {feedback_data['timestamp']}
**反馈类型**: {feedback_data['type']}

## 反馈内容
{feedback_data['content']}

---
评分: {feedback_data['rating']}/5
"""

    # 发送（敏感词会自动替换）
    result = notifier.send_markdown(
        webhook_url=WEBHOOK_URL,
        content=content,
        async_send=False
    )

    if result.is_success():
        print(f"反馈已转发到企业微信（消息ID: {result.message_id}）")

        # 如果包含敏感词，会记录到 .wecom_cache/moderation.log
        # 可以定期分析日志了解用户反馈中的敏感话题
    else:
        print(f"转发失败: {result.error}")

# 使用示例
feedback = {
    'user_id': 'user_12345',
    'timestamp': '2025-10-29 18:30:00',
    'type': '产品建议',
    'content': '我觉得这个功能设计有问题，建议改进...',
    'rating': 4
}

send_user_feedback_with_moderation(feedback)
```

### 场景6：社区内容审核（Block策略）

```python
def moderate_community_post(post_data):
    """社区帖子发布前审核（拒绝包含敏感词的帖子）"""

    # 启用内容审核，拒绝策略
    notifier = WeComNotifier(
        enable_content_moderation=True,
        moderation_config={
            "sensitive_word_urls": [
                "https://example.com/community_rules.txt"
            ],
            "strategy": "block",  # 检测到敏感词直接拒绝
            "log_sensitive_messages": True,
        }
    )

    # 尝试发送帖子内容到审核群
    result = notifier.send_text(
        webhook_url=WEBHOOK_URL,
        content=f"[新帖子审核]\n\n标题: {post_data['title']}\n\n{post_data['content']}",
        async_send=False
    )

    if result.is_success():
        # 审核通过，可以发布
        print("✓ 内容审核通过，可以发布")
        return True
    else:
        # 审核未通过，帖子被拒绝
        print(f"✗ 内容包含敏感词，拒绝发布: {result.error}")

        # 群里会收到告警消息，提醒运营人员关注
        # 日志文件会记录完整内容用于进一步分析

        return False

# 使用示例
post = {
    'title': '关于产品的讨论',
    'content': '我想讨论一下...'
}

if moderate_community_post(post):
    # 发布到社区
    publish_to_community(post)
else:
    # 通知用户修改
    notify_user_to_modify(post)
```

## 💡 最佳实践

### ✅ 推荐：使用单例模式

**为什么需要单例？**

每个 `WeComNotifier` 实例会为每个 webhook 创建独立的：
- 工作线程（处理消息队列）
- 频率控制器（20条/分钟）

如果创建多个实例，它们无法协调频率限制，容易触发服务端频控。

**正确做法：全局单例**

```python
# config.py 或应用初始化文件
from wecom_notifier import WeComNotifier, setup_logger

# 配置日志（可选）
setup_logger(log_level="INFO")

# 创建全局实例
NOTIFIER = WeComNotifier(
    max_retries=5
)

# 如果有多个 webhook，也只需一个实例
WEBHOOKS = {
    "dev": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=DEV-KEY",
    "prod": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=PROD-KEY"
}
```

```python
# 在其他模块中使用
from config import NOTIFIER, WEBHOOKS

def send_notification(message):
    """发送通知到开发群"""
    NOTIFIER.send_text(
        webhook_url=WEBHOOKS["dev"],
        content=message
    )

def send_alert(message):
    """发送告警到生产群"""
    NOTIFIER.send_text(
        webhook_url=WEBHOOKS["prod"],
        content=message,
        mentioned_list=["@all"]
    )
```

**优点**：
- ✅ 单个实例管理所有 webhook，资源高效
- ✅ 每个 webhook 独立的队列和频控，互不影响
- ✅ 避免多实例竞争导致的频控问题

### ❌ 错误：频繁创建实例

**错误示例1：每次调用都创建**
```python
# ❌ 不要这样做
def send_message(msg):
    notifier = WeComNotifier()  # 每次都创建新实例！
    notifier.send_text(WEBHOOK_URL, msg)
    # 实例销毁，线程也会停止
```

**问题**：
- 每次调用创建新线程，浪费资源
- 实例销毁时线程也停止，可能丢失未发送的消息
- 频控器无法累积，无法有效限速

**错误示例2：多个实例发送同一个 webhook**
```python
# ❌ 不要这样做
notifier1 = WeComNotifier()
notifier2 = WeComNotifier()

# 两个实例发送到同一个 webhook
notifier1.send_text(WEBHOOK_URL, "消息1")  # 线程1处理
notifier2.send_text(WEBHOOK_URL, "消息2")  # 线程2处理
```

**问题**：
- 两个独立的工作线程并发发送，无法保证顺序
- 两个独立的频控器，可能同时发送超过20条/分钟
- 触发服务端频控（45009错误）

### 🔄 使用上下文管理器（可选）

如果只是临时使用，可以添加上下文管理器：

```python
class WeComNotifierContext:
    """上下文管理器包装"""
    def __init__(self, **kwargs):
        self.notifier = WeComNotifier(**kwargs)

    def __enter__(self):
        return self.notifier

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.notifier.stop_all()
        return False

# 使用
with WeComNotifierContext() as notifier:
    notifier.send_text(WEBHOOK_URL, "消息1")
    notifier.send_text(WEBHOOK_URL, "消息2")
# 退出时自动清理资源
```

### 🧵 线程生命周期说明

**工作线程何时启动？**
```python
notifier = WeComNotifier()  # 此时还没有线程

# 第一次发送到某个 webhook 时，创建并启动工作线程
notifier.send_text(WEBHOOK_URL_A, "消息")  # 为 WEBHOOK_URL_A 创建线程

# 第一次发送到另一个 webhook 时，创建另一个工作线程
notifier.send_text(WEBHOOK_URL_B, "消息")  # 为 WEBHOOK_URL_B 创建线程

# 同一个 webhook 的后续消息，复用已有线程
notifier.send_text(WEBHOOK_URL_A, "消息2")  # 复用 WEBHOOK_URL_A 的线程
```

**工作线程何时停止？**
- 显式调用 `notifier.stop_all()`
- `WeComNotifier` 实例被垃圾回收（`__del__`）
- 主程序退出（daemon 线程自动终止）

**关键点**：
- 每个 webhook 只创建一次工作线程
- 线程会持续运行，处理消息队列
- 标记为 daemon，不会阻止程序退出

### 📊 多实例问题示例

**问题演示**：
```python
import threading

# 创建3个实例
notifier1 = WeComNotifier()
notifier2 = WeComNotifier()
notifier3 = WeComNotifier()

# 查看线程数
print(f"初始线程数: {threading.active_count()}")

# 都向同一个 webhook 发送
notifier1.send_text(WEBHOOK_URL, "消息1")  # 创建线程1
notifier2.send_text(WEBHOOK_URL, "消息2")  # 创建线程2
notifier3.send_text(WEBHOOK_URL, "消息3")  # 创建线程3

print(f"当前线程数: {threading.active_count()}")
# 输出：当前线程数: 4（主线程 + 3个工作线程）
```

**正确做法**：
```python
# 只创建一个实例
notifier = WeComNotifier()

# 所有消息共享同一个队列和线程
notifier.send_text(WEBHOOK_URL, "消息1")
notifier.send_text(WEBHOOK_URL, "消息2")
notifier.send_text(WEBHOOK_URL, "消息3")

print(f"当前线程数: {threading.active_count()}")
# 输出：当前线程数: 2（主线程 + 1个工作线程）
```

### 🎯 实际项目集成示例

**Flask 应用**：
```python
# app/__init__.py
from flask import Flask
from wecom_notifier import WeComNotifier

# 全局实例
notifier = WeComNotifier()

def create_app():
    app = Flask(__name__)
    # ... 其他配置
    return app

# app/tasks.py
from app import notifier
from config import WEBHOOK_URL

def send_task_notification(task_id, status):
    notifier.send_text(
        webhook_url=WEBHOOK_URL,
        content=f"任务 {task_id} {status}"
    )
```

**Django 应用**：
```python
# myproject/settings.py
from wecom_notifier import WeComNotifier

WECOM_NOTIFIER = WeComNotifier()
WECOM_WEBHOOK = os.getenv("WECOM_WEBHOOK_URL")

# myapp/tasks.py (Celery任务)
from django.conf import settings

def send_notification(message):
    settings.WECOM_NOTIFIER.send_text(
        webhook_url=settings.WECOM_WEBHOOK,
        content=message
    )
```

**通用脚本**：
```python
# utils/notifier.py
from wecom_notifier import WeComNotifier
import os

# 模块级单例
_notifier = None

def get_notifier():
    """获取全局 notifier 实例"""
    global _notifier
    if _notifier is None:
        _notifier = WeComNotifier()
    return _notifier

# 使用
from utils.notifier import get_notifier

notifier = get_notifier()
notifier.send_text(WEBHOOK_URL, "消息")
```

### 📝 内容审核最佳实践

#### 选择合适的审核策略

根据不同场景选择不同的策略：

**Block（拒绝）** - 适用于：
- ✅ 严格的内容管理场景（社区发帖、用户评论）
- ✅ 需要人工介入的敏感内容
- ✅ 合规性要求高的场景

**Replace（替换）** - 适用于：
- ✅ 自动化通知（日志、报表、监控告警）
- ✅ 需要保留上下文但过滤敏感词
- ✅ 用户反馈转发

**PinyinReverse（混淆）** - 适用于：
- ✅ 内部沟通（团队协作、技术讨论）
- ✅ 需要传达完整信息但避免触发关键词检测
- ⚠️ 不推荐用于正式通知

#### 管理敏感词库

```python
# ✅ 推荐：使用多个词库，分类管理
notifier = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": [
            "https://example.com/base_words.txt",      # 基础敏感词
            "https://example.com/industry_words.txt",  # 行业相关
            "https://example.com/custom_words.txt",    # 自定义词库
        ],
        "strategy": "replace",
    }
)

# 📝 词库维护建议：
# 1. 定期更新词库，重启应用生效
# 2. 使用 GitHub/GitLab 托管词库，方便版本控制
# 3. 分类管理：基础词库 + 业务词库
# 4. 测试环境先验证，再部署到生产环境
```

#### 日志分析与监控

```python
# 定期分析敏感消息日志
import json

def analyze_sensitive_logs(log_file=".wecom_cache/moderation.log"):
    """分析敏感消息日志，生成统计报告"""

    if not os.path.exists(log_file):
        print("日志文件不存在")
        return

    # 统计数据
    total_count = 0
    word_counter = {}
    strategy_counter = {}

    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                log = json.loads(line.strip())
                total_count += 1

                # 统计策略
                strategy = log.get('strategy', 'unknown')
                strategy_counter[strategy] = strategy_counter.get(strategy, 0) + 1

                # 统计敏感词频率
                for word in log.get('detected_words', []):
                    word_counter[word] = word_counter.get(word, 0) + 1
            except:
                pass

    # 生成报告
    print(f"\n=== 敏感消息日志分析报告 ===")
    print(f"总记录数: {total_count}")
    print(f"\n策略分布:")
    for strategy, count in strategy_counter.items():
        print(f"  {strategy}: {count}")

    print(f"\n高频敏感词 TOP 10:")
    sorted_words = sorted(word_counter.items(), key=lambda x: x[1], reverse=True)
    for word, count in sorted_words[:10]:
        print(f"  {word}: {count}次")

# 定期执行分析
analyze_sensitive_logs()
```

#### 缓存管理

```bash
# 清理缓存，强制重新加载敏感词
rm -rf .wecom_cache/sensitive_words_*.txt

# 查看缓存文件
ls -lh .wecom_cache/

# 输出示例:
# sensitive_words_a1b2c3d4.txt  # 敏感词缓存（基于URL hash命名）
# moderation.log                 # 敏感消息日志
# moderation.log.1               # 日志备份1
# moderation.log.2               # 日志备份2
```

#### 性能优化建议

```python
# ✅ 推荐：在应用启动时初始化（只加载一次）
from wecom_notifier import WeComNotifier

# 全局初始化
NOTIFIER = WeComNotifier(
    enable_content_moderation=True,
    moderation_config={
        "sensitive_word_urls": ["https://example.com/words.txt"],
        "strategy": "replace",
    }
)

# 后续使用不需要重新加载
def send_message(content):
    NOTIFIER.send_text(WEBHOOK_URL, content)  # 快速，无需重新加载词库

# ❌ 避免：每次调用都初始化（每次都重新加载词库）
def send_message(content):
    notifier = WeComNotifier(
        enable_content_moderation=True,
        moderation_config={...}  # 每次都下载和解析词库！
    )
    notifier.send_text(WEBHOOK_URL, content)
```

#### 测试内容审核

```python
def test_moderation():
    """测试内容审核功能"""

    notifier = WeComNotifier(
        enable_content_moderation=True,
        moderation_config={
            "sensitive_word_urls": ["https://example.com/test_words.txt"],
            "strategy": "replace",
            "log_sensitive_messages": True,
            "log_file": ".wecom_cache/test_moderation.log",
        }
    )

    # 测试用例
    test_cases = [
        ("正常消息", "这是一条正常的消息，不包含任何问题"),
        ("包含敏感词", "这条消息包含敏感词"),
        ("多个敏感词", "这条消息包含多个敏感词和问题词"),
    ]

    for name, content in test_cases:
        print(f"\n测试: {name}")
        result = notifier.send_text(
            webhook_url=WEBHOOK_URL,
            content=content,
            async_send=False
        )

        if result.is_success():
            print(f"  ✓ 发送成功")
        else:
            print(f"  ✗ 发送失败: {result.error}")

    # 检查日志
    print(f"\n查看日志: cat .wecom_cache/test_moderation.log")

# 运行测试
test_moderation()
```

## ⚠️ 注意事项

### 1. Webhook安全
- ❌ 不要将webhook地址提交到公开仓库
- ✅ 使用环境变量存储
- ✅ 使用配置文件（加入.gitignore）

```python
import os
WEBHOOK_URL = os.getenv("WECOM_WEBHOOK_URL")
```

### 2. 实例管理（重要！）
- ✅ **推荐**：全局使用单个 `WeComNotifier` 实例
- ❌ **避免**：频繁创建新实例或多实例并发
- ❌ **避免**：在函数内部创建实例后立即销毁
- 📖 详见上方"最佳实践"章节

### 3. 频率限制
- 企业微信限制：20条/分钟/webhook
- 本项目自动处理：
  - **本地预防**：滑动窗口算法限速
  - **服务端频控智能重试**：等待65秒后重试，最多5次
- 即使 webhook 被其他程序触发频控，消息也会等待后成功发送
- 详见 README.md 的"频率控制（双层保护）"章节

### 4. 消息长度
- 限制：4096字节/条
- 本项目自动分段，无需手动处理
- 分段间隔默认1000ms

### 5. @all功能
- `text`格式原生支持
- `markdown_v2`和`image`需额外发送text消息
- 本项目自动处理

### 6. 错误处理
```python
result = notifier.send_text(...)

if not result.is_success():
    # 发送失败，查看错误
    print(f"错误: {result.error}")

    # 可以实现备用通知方式
    send_email_alert(result.error)
```

## 🐛 故障排查

### 问题1：发送失败
```python
# 检查webhook是否有效
result = notifier.send_text(
    webhook_url=WEBHOOK_URL,
    content="测试消息",
    async_send=False  # 同步模式便于调试
)

if not result.is_success():
    print(result.error)  # 查看具体错误
```

### 问题2：消息顺序混乱
- 确认：同一消息的分段是连续的
- 不同消息可能交错（这是正常的）
- 如需严格顺序，使用同步模式

### 问题3：超过频率限制
- 检查是否有其他程序也在使用同一webhook
- 本项目会自动等待，但外部调用会绕过限制

### 问题4：日志太多

**v0.2.0+ 日志控制方式**：

```python
# 方式1：调整日志级别（只显示警告和错误）
from wecom_notifier import setup_logger
setup_logger(log_level="WARNING")

# 方式2：完全禁用日志
from wecom_notifier import disable_logger
disable_logger()

# 方式3：通过环境变量控制
import os
os.environ["LOGURU_LEVEL"] = "WARNING"
```

**旧版本（v0.1.x）**：
```python
# ❌ v0.2.0+ 不再支持
notifier = WeComNotifier(log_level="WARNING")
```

## 📖 更多信息

- [README.md](README.md) - 项目介绍和快速开始
- [日志配置指南](doc/logging_configuration_guide.md) - 完整的日志配置文档
- [日志最佳实践](doc/wecom_notifier_logging_best_practices.md) - 日志系统设计原则
- [tests/](tests/) - 测试示例
- [examples/basic_usage.py](examples/basic_usage.py) - 完整示例

---

有问题？欢迎提issue：https://github.com/yourusername/wecom-notifier/issues
