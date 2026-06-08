# 纳斯达克100相对强弱排名网页

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/hellozip/nasdaq100-relative-strength)

这是一个可部署的 Python 网页应用，用 Yahoo Finance 免费接口读取纳斯达克100成分股和价格数据，并按以下规则排序：

1. 最新价格高于 MA5 得 1 分。
2. 最新价格高于 MA10 得 1 分。
3. 最新价格高于 MA20 得 1 分。
4. 先按评分从高到低排序。
5. 同评分股票再按相对纳斯达克100指数的 7 个交易日涨跌幅比例排序。

网页里有一个“更新排名”按钮，点击后会在服务器端重新运行 Python 数据读取和排名代码。

## 本地运行

```powershell
python -m pip install -r requirements.txt
python app.py
```

然后打开：

```text
http://127.0.0.1:8000
```

## 部署说明

GitHub Pages 只能托管静态网页，不能让访客点击按钮运行 Python。

要让“更新排名”按钮真正运行代码，需要部署到支持 Python 后端的平台，例如：

- Render
- Railway
- Fly.io
- Replit
- 自己的 VPS

以 Render 为例：

1. 把本文件夹上传到一个 GitHub 仓库。
2. 在 Render 新建 Web Service。
3. 连接这个 GitHub 仓库。
4. Build Command 填：

```text
pip install -r requirements.txt
```

5. Start Command 填：

```text
python app.py
```

6. 部署完成后，Render 会给你一个公开网址，其他人访问后可以点击“更新排名”按钮。

## 数据源

- 纳斯达克100成分股：Yahoo Finance screener `most_actives_ndx`
- 价格数据：Yahoo Finance spark 日线接口
- 基准指数：`^NDX`

数据可能存在延迟，仅供研究参考，不构成投资建议。
