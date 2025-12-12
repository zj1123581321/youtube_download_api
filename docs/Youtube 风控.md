1. 威胁图谱：YouTube 现代风控机制深度解析
在设计防御体系之前，必须首先对“攻击面”——即 YouTube 的风控检测逻辑——进行解构。当前的检测系统不再依赖单一维度的阈值（如简单的请求频率），而是演变为多维度、基于行为启发式的综合评分系统。

1.1 网络层指纹与 IP 信誉评分体系
YouTube 的第一道防线位于网络传输层。

IP 信誉数据库 (IP Reputation): Google 维护着庞大的 IP 数据库，将 IP 地址分类为“住宅 (Residential)”、“数据中心 (Datacenter/Hosting)”和“移动网络 (Mobile)”。

风险传递机制： 在使用 Clash 代理的场景下，用户通常使用的是机场提供的共享节点。这些节点多属于数据中心 IP（如 AWS, DigitalOcean, HK BN 等）。如果同一节点上的“邻居”用户进行了滥用操作（如大规模爬虫），该 IP 会被标记为高风险，导致所有通过该 IP 的流量受到更严格的验证 。   

ASN 封锁： 在极端情况下，Google 会对整个自治系统号 (ASN) 进行降权处理，导致该 IP 段下的所有请求直接触发 403 错误或验证码 。   

TCP/IP 协议栈指纹 (p0f): 虽然主要用于识别操作系统，但在代理环境中，Clash 的流量特征（如 TTL 值、TCP 窗口大小）如果与 HTTP 头部声明的 User-Agent 不符（例如 User-Agent 声明是 Windows Chrome，但 TCP 特征显示是 Linux 服务器），会增加被标记为机器人的概率 。   

1.2 传输层安全 (TLS) 指纹识别：JA3/JA4 签名
这是目前自动化工具最容易暴露的特征之一。

原理： 在 HTTPS 握手阶段（ClientHello），客户端会发送其支持的加密套件 (Cipher Suites)、TLS 版本、扩展字段 (Extensions) 及其排列顺序。这些参数组合形成了一个独特的指纹，被称为 JA3 或 JA4 签名 。   

识别逻辑： 标准的 Python requests 库或旧版 yt-dlp 使用的 OpenSSL 库生成的 TLS 指纹，与真实的 Chrome 或 Safari 浏览器截然不同。Google 的 BotGuard 会实时比对 User-Agent 声明的浏览器类型与实际的 TLS 指纹。如果不匹配（例如声明是 Chrome 120 但指纹是 Python 3.10），请求将被视为自动化流量并被拦截 。   

后果： 这是导致“Sign in to confirm you're not a bot”以及 HTTP 403 错误的主要技术原因之一，即使你使用了高质量的住宅 IP，如果 TLS 指纹暴露，依然会被拦截 。   

1.3 应用层验证：BotGuard 与 PO Token
自 2024 年底起，YouTube 逐步强制要求客户端提供 Proof of Origin (PO) Token。

BotGuard/DroidGuard 机制： 这是一个运行在客户端（浏览器 JS 或移动端 App）的虚拟机环境，负责收集设备环境信息（如 Canvas 指纹、屏幕分辨率、鼠标移动轨迹等）并生成加密的 Attestation Token 。   

PO Token 的作用： 该 Token 证明了请求是由一个“经过验证的真实环境”发出的。

IP 绑定特性 (The Binding Constraint): 这是一个关键的工程约束。PO Token 的生成过程与生成时的公网 IP 地址强绑定。如果 PO Token 是在本地（IP A）生成的，但随后的视频数据请求是通过代理节点（IP B）发出的，Google 会检测到 IP 不一致，从而判定 Token 无效并拒绝服务 。这直接否定了简单的“本地生成 Token，代理下载视频”的架构，要求 Token 生成服务必须与下载服务共享完全相同的出口 IP。   

1.4 行为启发式分析 (Behavioral Heuristics)
请求速率与模式： 机器人的下载模式通常是线性的、高并发的、无休眠的。人类用户的行为包含随机的暂停、页面滚动（加载评论、相关视频）、以及非线性的点击流。

Cookie 一致性： YouTube 的 Cookie（特别是 VISITOR_INFO1_LIVE 和 LOGIN_INFO）包含复杂的会话状态。如果检测到 Cookie 频繁变更 IP 位置（例如几秒钟内从日本跳到美国），或者 Cookie 在没有完整浏览器上下文的情况下被长期使用，会导致账号会话失效或软封禁 。   

