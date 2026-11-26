# 软件设计规格说明书（SDD）

**项目名称：** 闲贝校园——校园二手闲置物品交易平台
**版本：** V1.0
**编制人：** {张三}
**日期：** 2026-11-24
**关联文档：** 《闲贝校园需求规格说明书（SRS）V1.0》

---

## 1. 引言

### 1.1 编写目的
本文档依据《闲贝校园需求规格说明书》编制，描述系统"如何实现"，作为后续编码、测试与验收的依据。预期读者：课程设计评审教师、项目开发人员、测试人员。

### 1.2 项目背景
高校学生每学期产生大量闲置物品（教材、数码产品、生活用品），处理渠道分散且效率低。本项目面向**单一高校（先试点后推广）**的在校师生，提供发布闲置、浏览搜索、在线沟通、当面交易确认的闭环平台，解决信息分散、信任成本高的问题。

### 1.3 术语与缩略语

| 术语 | 说明 |
|---|---|
| SRS | 软件需求规格说明书 |
| SDD | 软件设计规格说明书 |
| JWT | JSON Web Token，登录态凭证 |
| RBAC | 基于角色的访问控制 |

### 1.4 参考文档
- 《闲贝校园需求规格说明书》
- 阿里巴巴 Java 开发手册（编码规范）
- RESTful API 设计规范

---

## 2. 系统总体设计

### 2.1 系统目标与范围
**功能目标：**
- F1 用户注册/登录（学号邮箱验证，限校内用户）
- F2 闲置物品发布（最多 6 张图、分类、定价、成色描述）
- F3 浏览与搜索（分类筛选、关键词、价格区间、排序）
- F4 站内私信沟通（买卖双方一对一对话）
- F5 交易管理（"我想要"标记 → 约定交易 → 当面扫码/确认 → 评价）
- F6 举报与审核（违规下架、用户禁言）

**非功能目标：**
- 核心接口 P95 响应 < 500ms；支持 500 并发在线浏览；
- 系统可用性 ≥ 99.5%（课程设计环境）；
- 密码加盐哈希存储，全站 HTTPS。

**范围外（明确不做）：** 在线支付与担保交易、物流发货、跨校数据互通。

### 2.2 设计约束
- 运行环境：服务端 Linux（CentOS 7+）；客户端 Chrome 90+ / 微信内置浏览器；
- 技术栈：JDK 17、Spring Boot 3.x、MyBatis-Plus、MySQL 8.0、Redis 7、Vue 3 + Element Plus、Nginx；
- 存储约束：图片走本地 OSS 模拟目录（生产可平替为对象存储）；
- 必须可单机 Docker Compose 一键启动，便于课程验收演示。

### 2.3 总体架构
采用前后端分离 + 经典三层架构：

```
┌─────────────────────────────────────────────┐
│  表现层（Vue3 SPA，Nginx 托管/反向代理）       │
├─────────────────────────────────────────────┤
│  接口层：Controller（REST API + JWT 拦截器）  │
├─────────────────────────────────────────────┤
│  业务层：Service（交易、审核、权限等核心业务）  │
├─────────────────────────────────────────────┤
│  数据访问层：Mapper（MyBatis-Plus）           │
├───────────────┬───────────────┬─────────────┤
│  MySQL 8.0    │   Redis 7     │  本地图片存储 │
└───────────────┴───────────────┴─────────────┘
```

**层间职责：**
- 表现层：页面渲染、表单校验、路由守卫（未登录跳登录页）；
- 接口层：参数校验（JSR-303）、统一返回体 `Result<T>`、鉴权（JWT + 拦截器）；
- 业务层：所有业务规则（如"一人同时最多挂单 20 件"、"举报达 3 次自动下架"）；
- 数据访问层：CRUD，禁止写跨表复杂业务逻辑。

---

## 3. 模块设计

### 3.1 模块划分

| 模块 | 职责 | 依赖 |
|---|---|---|
| M1 用户模块 | 注册、登录、JWT 签发/校验、个人资料、信用分 | M8 |
| M2 物品模块 | 发布、编辑、下架、图片上传、分类管理 | M1, M8 |
| M3 搜索模块 | 关键词/分类/价格筛选、热门搜索 | M2, M8 |
| M4 消息模块 | 站内私信会话、未读数、WebSocket 推送 | M1, M8 |
| M5 交易模块 | 我想要、生成交易单、当面确认、取消、评价 | M1, M2, M8 |
| M6 举报审核模块 | 举报提交、管理员审核、违规处理 | M1, M2, M5, M8 |
| M7 后台管理模块 | 用户/物品/分类/举报的管理界面与接口 | M1~M6 |
| M8 基础支撑 | 统一异常处理、错误码、日志、文件存储、缓存 | — |

### 3.2 关键模块详细设计

#### M1 用户模块
- **核心类：** `AuthService.login(username, password) → TokenVO`
  - 校验用户名+密码（bcrypt 比对）→ 失败计数（5 次锁 30 分钟）→ 签发 JWT（2h，Refresh Token 7d）→ 写登录日志。
- **核心类：** `JwtInterceptor`：解析 Header `Authorization: Bearer <token>`，校验签名与过期，将 `userId` 注入 ThreadLocal。
- **异常路径：** 1001 用户不存在 / 1002 密码错误 / 1003 账号锁定 / 1004 Token 过期 / 1005 Token 无效。

#### M2 物品模块
- **发布流程：** 参数校验（价格 0–99999、图片 ≤6 张、分类必须存在）→ 图片落盘并生成缩略图 → 写 `item` 表（状态=在售）→ 清该用户缓存的"我的列表"。
- **软删除设计：** 所有删除均为 `is_deleted` 标记，保留审计追溯。

#### M5 交易模块
- **状态机（核心，必须配图）：**
  `待确认(created) → 已约定(agreed) → 已完成(completed)`；任意非终态可 → `已取消(canceled)`；超时 24h 未确认自动取消（定时任务）。
- **并发控制：** 同一物品同一时刻仅允许一个"进行中"交易单，数据库唯一索引 `uk(item_id, status)`（进行中状态）防止重复下单（超卖）。
- **事务边界：** `createTrade()` 方法级 `@Transactional`：校验物品在售 → 插入交易单 → 物品状态改为"锁定"，两步任一失败整体回滚。

#### M6 举报审核模块
- 举报类型：虚假信息/诈骗嫌疑/违禁品/人身攻击；
- 规则引擎（简化）：同一物品被 ≥3 个不同用户举报 → 自动下架并通知卖家；卖家信用分 -5，低于 60 分限制发布。

---

## 4. 数据设计

### 4.1 实体关系（ER 概述）
核心实体：用户(User)、物品(Item)、分类(Category)、交易单(Trade)、评价(Review)、消息会话(Conversation)、消息(Message)、举报(Report)。
关系：用户 1—n 物品；物品 n—1 分类；用户 1—n 交易单（买方）；物品 1—n 交易单；交易单 1—1 评价；用户 1—n 会话；会话 1—n 消息；用户 1—n 举报。

### 4.2 表结构（节选核心表）

**user 用户表**

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | BIGINT | PK, AUTO_INCREMENT | 用户 ID |
| student_no | VARCHAR(20) | UNIQUE, NOT NULL | 学号/工号 |
| username | VARCHAR(32) | UNIQUE, NOT NULL | 登录名 |
| password_hash | VARCHAR(64) | NOT NULL | bcrypt 哈希 |
| nickname | VARCHAR(32) | | 昵称 |
| avatar | VARCHAR(255) | | 头像路径 |
| credit_score | INT | DEFAULT 100 | 信用分 |
| status | TINYINT | DEFAULT 0 | 0正常 1禁言 2封禁 |
| role | TINYINT | DEFAULT 0 | 0普通 1管理员 |
| created_at | DATETIME | DEFAULT NOW() | |
| is_deleted | TINYINT | DEFAULT 0 | 逻辑删除 |

**item 物品表**

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | BIGINT | PK | 物品 ID |
| seller_id | BIGINT | FK→user.id, NOT NULL | 卖家 |
| category_id | INT | FK→category.id | 分类 |
| title | VARCHAR(64) | NOT NULL, INDEX | 标题 |
| description | TEXT | | 描述 |
| price | DECIMAL(10,2) | NOT NULL | 定价 |
| condition_level | TINYINT | | 成色：1-5 |
| images | JSON | | 图片路径数组 |
| status | TINYINT | DEFAULT 0, INDEX | 0在售 1锁定 2已售 3下架 |
| view_count | INT | DEFAULT 0 | 浏览数 |
| created_at / updated_at | DATETIME | | |

**trade 交易单表**

| 字段 | 类型 | 约束 | 说明 |
|---|---|---|---|
| id | BIGINT | PK | 交易单号 |
| item_id | BIGINT | FK, NOT NULL | 物品 |
| buyer_id | BIGINT | FK, NOT NULL | 买方 |
| seller_id | BIGINT | FK, NOT NULL | 卖方（冗余，便于查询） |
| status | TINYINT | NOT NULL, INDEX | 状态机值 |
| meet_place | VARCHAR(128) | | 约定地点 |
| expire_at | DATETIME | | 超时时间 |
| UNIQUE KEY uk_active | (item_id, status) | | 进行中唯一（防超卖，DB 层兜底） |
| created_at / completed_at | DATETIME | | |

**message / report / review / conversation** 等表字段略，详见数据库脚本 `schema.sql`（见交付物清单）。

### 4.3 一致性与并发
- 交易创建、物品状态变更走本地事务；跨表只读用读写分离（课程环境主库即可）；
- 热点数据缓存：首页最新物品列表（Redis，TTL 5min，发布/下架时主动失效）；
- 浏览数用 Redis `INCR` 异步落库，避免行锁竞争。

---

## 5. 接口设计

### 5.1 用户界面
主要页面：登录/注册、首页、分类浏览、搜索结果、物品详情、发布页、私信页、我的（在售/交易/评价）、管理后台。给出页面流转图（见附录 A 图 2）。

### 5.2 内部 API 规范（示例）

**登录**
```
POST /api/v1/auth/login
Req:  { "username": "20230001", "password": "******" }
Resp: { "code":0, "data": { "accessToken":"eyJ...", "refreshToken":"eyJ...", "expire":7200 } }
```

**发布物品**
```
POST /api/v1/items            （需登录，Content-Type: multipart/form-data）
Req:  title, description, price, conditionLevel, categoryId, images[] ≤6
Resp: { "code":0, "data": { "itemId": 1024 } }
错误：2001 参数校验失败 / 2002 图片超限 / 2003 分类不存在
```

**创建交易单（防超卖）**
```
POST /api/v1/trades
Req:  { "itemId": 1024, "meetPlace": "图书馆门口" }
Resp: 成功 → { "code":0, "data": { "tradeId": 90001 } }
冲突 → { "code":3001, "msg":"该物品已有进行中的交易" }（依赖 uk_active 索引捕获）
```

**统一错误格式：** `{ "code": <业务码>, "msg": "<说明>", "data": null }`
错误码段：1xxx 用户/认证，2xxx 物品，3xxx 交易，4xxx 消息，5xxx 审核，9xxx 系统。

### 5.3 外部接口
- 对象存储：抽象 `FileStorageService` 接口，当前实现 `LocalFileStorage`，生产可替换 `OssFileStorage`，对业务层透明。

---

## 6. 非功能性设计

| 类别 | 设计措施 |
|---|---|
| 性能 | 列表分页（pageSize≤20）；item 表对 (category_id,status) 建联合索引；慢查询阈值 200ms 记录日志 |
| 安全 | bcrypt(10) 哈希密码；JSR-303 入参校验 + 服务端二次校验；MyBatis 参数化杜绝 SQL 注入；XSS：前端 v-text 输出 + 后端富文本白名单；上传文件校验 MIME/大小/重命名存储；JWT 过期 + 接口鉴权注解 `@RequireRole` |
| 可靠性 | 全局异常处理器 `@RestControllerAdvice`；关键操作（登录、交易、审核）写操作日志表；每日 mysqldump 备份 |
| 可维护性 | 配置外置 `application.yml` + 环境变量；统一 Result/错误码/日志格式；接口版本前缀 `/api/v1` |

---

## 7. 出错处理与异常设计
- **分类：** 参数错误（4xx 语义）/ 业务冲突（3xxx 码）/ 未认证未授权（1004/1006）/ 系统异常（9xxx，日志告警）；
- **重试与降级：** 消息推送失败重试 3 次后落离线表；Redis 不可用时降级直查数据库并告警；
- **幂等设计：** 创建交易单前端按钮防连点（禁用态）+ 服务端唯一索引兜底。

---

## 8. 部署设计

```
Docker Compose（单机验收环境）
├─ nginx        :80  → 静态资源 + /api 反代
├─ app          :8080（Spring Boot，可 scale=2）
├─ mysql        :3306（数据卷持久化）
└─ redis        :6379
```
- 端口规划：80（Nginx）、8080（应用，仅内网）、3306/6379 不对外暴露；
- 环境区分：`dev`（H2+内存缓存，单测）/ `prod`（MySQL+Redis）。

---

## 9. 测试考虑
- **单元测试：** 状态机流转、信用分规则、bcrypt 校验等纯逻辑（JUnit5 + Mockito，覆盖率 ≥60%）；
- **集成测试：** 登录→发布→搜索→下单→确认→评价全链路；并发创建交易单验证唯一索引；
- **需求追踪表（节选）：**

| SRS 需求 | 设计章节 | 测试用例 |
|---|---|---|
| F2 发布物品 | §3.2 M2、§5.2 | TC-F2-01 正常发布；TC-F2-02 超 6 张图拒绝 |
| F5 交易 | §3.2 M5、§4.2 | TC-F5-01 超卖拦截；TC-F5-02 24h 自动取消 |

---

## 10. 附录
- 图 1：系统架构图（见 §2.3）；图 2：页面流转图；图 3：交易状态机图；图 4：ER 图
- 交付物清单：`schema.sql`、接口文档（Swagger）、Docker Compose 文件、测试报告
- 变更记录：V1.0 初稿，2026-11-24
- TBD：管理后台数据统计报表（V1.1）；WebSocket 消息已读回执优化
