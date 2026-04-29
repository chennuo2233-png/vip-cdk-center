# VIP CDK 兑换中台 V1

这是一个只处理“你的代理兑换码”的中台：自动生成 CDK、用户提交学号、后台查看待处理订单、手动更新充值状态。

它不会连接、访问、模拟、调用任何第三方网站。

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
- 后台：http://127.0.0.1:5000/admin

本地默认后台密码：`admin123`

上线前请设置环境变量：

- `ADMIN_PASSWORD`：后台密码
- `SECRET_KEY`：随机密钥

## 状态含义

- `created`：刚生成，还没发给合作方
- `distributed`：已发给合作方，用户可以兑换
- `submitted`：用户已提交，排队中
- `processing`：你正在处理
- `success`：充值成功
- `failed`：充值失败
- `refunded`：已退款

## 使用流程

1. 登录后台 `/admin/login`
2. 到 `/admin/codes` 生成 20 个码，状态选 `distributed`
3. 导出 CSV，把兑换码和兑换网址发给合作方上架
4. 用户到 `/redeem` 输入兑换码、学号、联系方式
5. 后台 `/admin` 会出现 `用户已提交/排队中`
6. 你手动去供应方网站完成充值
7. 完成后在后台把状态改为 `充值成功` 或 `充值失败`

## 部署建议

第一版可部署到 Render / PythonAnywhere 等支持 Flask 的平台。
