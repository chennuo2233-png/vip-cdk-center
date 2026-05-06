# VIP CDK 兑换中台 V2

这是一个“老板发卡 + 外国员工人工核销”的 CDK 兑换中台。

核心规则：

- 用户提交 CDK + Token 后，只创建一条人工核销任务，不会立即核销 CDK。
- 同一个 CDK 同一时间只能有一条未完成任务。
- 员工点击 **Success** 后，CDK 才会变成已核销。
- 员工点击 **Failed** 后，CDK 不核销，用户可以检查 Token 后重新提交。
- 失败原因和员工备注只在后台可见；用户侧只提示“请检查Token”。
- 老板掌握发卡、导出、财务统计、日志和全部任务。
- 组长可以创建组员、管理组员、分配任务、处理任务。
- 组员只能看到分配给自己的任务。

## 目录结构

```text
vip-cdk-center
├─ app.py
├─ requirements.txt
├─ cdk_center.db              # 本地运行后自动创建；不要提交到 Git
├─ static
│  ├─ style.css
│  └─ app.js
└─ templates
   ├─ base.html
   ├─ redeem.html
   ├─ admin_login.html
   ├─ admin.html
   ├─ owner_tasks.html
   ├─ generate.html
   ├─ codes.html
   ├─ stats.html
   ├─ team.html
   ├─ ops_login.html
   ├─ ops_tasks.html
   ├─ ops_team.html
   ├─ logs.html
   └─ task_cards.html
```

## 本地运行

```bash
cd vip-cdk-center
python -m venv .venv

# Windows PowerShell
.venv\Scripts\Activate.ps1

# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
python app.py
```

浏览器打开：

- 用户兑换页：http://127.0.0.1:5000/redeem
- 老板后台：http://127.0.0.1:5000/FenYi/login
- 员工工作台：http://127.0.0.1:5000/ops/login


## 用户页跳转按钮配置

用户兑换页的“步骤二”包含两个固定跳转按钮。默认是占位地址，上线前请改成真实地址。推荐使用环境变量配置：

```text
STEP2_LOGIN_URL=网页1登录地址
STEP2_COPY_URL=网页2复制内容地址
STEP2_LOGIN_LABEL=打开网页 1
STEP2_COPY_LABEL=打开网页 2
```

如果不使用环境变量，也可以直接在 `app.py` 顶部修改 `STEP2_LOGIN_URL` 和 `STEP2_COPY_URL`。

公开页面不会显示老板后台或 Ops 登录入口；你仍然可以直接访问 `/FenYi/login` 和 `/ops/login` 登录。

## 首次登录

首次运行会自动创建一个老板账号。

默认：

```text
username: owner
password: admin123
```

上线前必须设置环境变量：

```text
OWNER_USERNAME=你的老板用户名
OWNER_PASSWORD=你的老板强密码
SECRET_KEY=随机长密钥
```

如果没有设置 `OWNER_PASSWORD`，系统会兼容旧的 `ADMIN_PASSWORD`；如果两个都没有，才会使用本地默认密码 `admin123`。

## 角色权限

### owner / 老板

- 生成 CDK
- 导出 CDK
- 查看卡密库
- 查看全部任务
- 处理全部任务
- 创建组长和组员
- 禁用 / 启用员工账号
- 删除员工账号
- 重置员工密码
- 查看财务统计
- 查看操作日志

### lead / 组长

- 查看全部人工核销任务
- 分配任务给组员
- 自己处理任务
- 创建组员
- 禁用 / 启用组员账号
- 删除组员账号
- 重置组员密码

不能：

- 生成 CDK
- 导出 CDK
- 查看收入、成本、利润
- 创建组长或老板

### staff / 组员

- 只查看分配给自己的任务
- Start / Success / Failed
- 复制 Token
- 填写失败原因和备注

不能：

- 查看未分配任务池
- 查看其他员工任务
- 创建员工
- 生成或导出 CDK
- 查看财务

## 状态含义

### CDK 状态

- `created`：刚生成，还没上架
- `distributed`：可兑换
- `redeemed`：已核销，只有任务 Success 后才会进入该状态
- `disabled`：老板手动禁用

### 任务状态

- `pending`：用户刚提交，等待分配或处理
- `assigned`：已分配给员工
- `processing`：处理中
- `success`：充值成功，CDK 已核销
- `failed`：充值失败，CDK 不核销，用户可以重新提交

## 从 V1 升级

V2 会自动迁移旧数据库：

- 旧的 `codes.status = success` 会变成 `codes.status = redeemed`。
- 旧的 `submitted / processing / failed` 会变回 `distributed`，并尽量生成对应的历史任务。
- 旧的 `student_id` 字段会作为旧任务的 Token 迁移到 `redemption_tasks.token`。

升级前建议先备份：

```powershell
copy E:\vip-cdk-center\vip-cdk-center\cdk_center.db E:\vip-cdk-center\vip-cdk-center\cdk_center.backup.db
```

## 上线前检查

必须完成：

1. 设置 `SECRET_KEY`。
2. 设置 `OWNER_PASSWORD`。
3. 备份旧的 `cdk_center.db`。
4. 确认部署平台会持久化数据库文件；否则重启后数据可能丢失。
5. 生产环境不要开启 Flask debug 模式。

## 安全说明

本项目仍然是轻量 Flask + SQLite 版本，适合小规模人工核销。多人协作已经有独立账号、角色权限、CSRF 防护和操作日志，但如果订单量明显增长，建议下一阶段迁移到 PostgreSQL，并增加登录限速、IP 风控和更完整的审计报表。

## 批量查询订单状态

用户兑换页 `/redeem` 的“订单状态查询”支持一次输入多个 CDK：

- 支持英文逗号 `,`、中文逗号 `，`、空格、Tab、换行分隔。
- 一次最多输入 1000 个 CDK。
- 重复 CDK 会自动去重并按首次出现顺序展示。
- 批量查询只展示用户可见状态；后台失败原因和员工备注不会出现在用户页面。


## V2.3 队列优先工作台

- `/ops/tasks` 默认只显示未完成任务：`pending / assigned / processing`。
- Success / Failed 任务会自动收纳到 `/ops/tasks/archive`，不再干扰当前队列。
- 老板后台的任务页 `/FenYi/tasks` 同样默认只显示当前队列；完成任务在 `/FenYi/tasks/archive`。
- 每个任务默认折叠，只露出任务 ID、CDK、排队时长、分配人和状态；展开后才显示 Token、Copy、Assign、Success、Failed。
- 队列里会显示 `Queued X minutes` / `已排队 X 分钟`，不做每秒刷新，页面更轻。
- `/ops/team` 会以卡片形式展示每个组员的当前队列、等待中、处理中、今日成功、今日失败。
- 点击组员卡片进入 `/ops/team/<user_id>`，可查看该组员当前任务和今日完成任务，方便组长按工作量结算。


## V2.4 员工账号删除

- 组长在 `/ops/team` 的 Team Workload / Staff Accounts 里可以删除组员账号。
- 老板在 `/FenYi/team` 里也可以删除 lead / staff 账号。
- 删除采用安全删除：账号会从界面消失、无法再登录、无法再被分配任务，但历史任务和操作日志会保留，方便日后核对结算。
- 如果被删除账号还有 `pending / assigned / processing` 未完成任务，这些任务会自动回到未分配队列，避免任务卡死在已删除员工名下。
