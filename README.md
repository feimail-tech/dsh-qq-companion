# dsh-qq-companion

把 [DSH（DeepSeek Harness）](https://github.com/deepseek-ai) 里的一个会话接到 QQ 群，让它像个群友一样活着：
会被 @ 醒、会自己插一两句话、会发表情包、看得懂图和动图、听得了语音、记得住人。

> **bridge 是 fork**：`bridge/` 来自 [sj244/dsh-qq-bridge](https://github.com/sj244/dsh-qq-bridge)（MIT，v0.1.3）。
> 我们在这个基础上加了一堆群聊实战补丁（见下），原作者的文档保留在 `docs/upstream-*.md`。

## 目录

| 目录 | 是什么 |
|---|---|
| `bridge/` | QQ 桥接插件（OneBot v11）。fork 自 sj244，含本仓库的全部增强 |
| `memory/` | 群友记忆：按人聚合的 trait / episode / relation / alias / fact 原子条目 |
| `escalate/` | 向上求助：分身在群里搞不定时调 `ask_owner` 找主 agent（可选） |
| `asr/` | 本地语音转写服务：SILK/WAV → 文字，走 faster-whisper，不出本机 |
| `presets/dafeiyu/` | 示例人格：鲸鱼娘「大肥鱼」的群聊版 |
| `docs/` | 上游 README / DESIGN，以及安装细节 |

## 相比上游加了什么

- **每群一条独立会话**：`qq-bridge-groups.json` 做群号 → sessionId 映射，上下文不串
- **出站目的地按会话取**：两群同时说话时回复不会跑到隔壁群（工具通道也不跑）
- **分句发送**：长句按标点和空格拆成 2–3 条，条间插打字节奏的停顿
- **打错字再撤回**：小概率发叠字/漏字版，800ms 后撤回重发正确版
- **发言后跟进窗口**：刚说完话的一段时间内，同一目的地的普通消息更容易把它叫醒
- **动图 → 九宫格**：GIF/APNG/动画 WebP 均匀抽 9 帧拼 3x3，比只看第一帧强得多
- **视频 → 九宫格**：下载后抽 9 帧（只取前 60 秒），跟图片一样送视觉模型
- **合并转发递归展开**：嵌套转发下钻 2 层，总条数封顶，不再是空壳 `[合并转发]`
- **语音按需转写**：语音默认只落盘，需要时调 `qq_bridge_transcribe` 本地转文字
- **群友记忆**：每轮按发言人自动注入画像；定时任务可以每天从会话里提炼

## 环境要求

- DSH 已安装并能跑 `dsh web`
- 一个 OneBot v11 实现（NapCat / SnowLuma / LLOneBot 都行），暴露 HTTP + WS
- Node.js 20+
- 可选：`ffmpeg` / `ffprobe` 在 PATH 里（动图、视频理解要用；不装就退回首帧）
- 可选：Python 3.10+ 加 `faster-whisper`、`silk-python`（语音转写要用）、`Pillow`（动图/视频拼九宫格要用）

## 安装

0. 先把仓库拉下来（或者直接下 zip 解压）：

```sh
git clone https://github.com/feimail-tech/dsh-qq-companion.git
cd dsh-qq-companion
```

1. 把三个插件拷进 DSH profile 的 node_modules：

```sh
PROFILE=~/.dsh/profiles/web/node_modules
# ⚠️ 目录名要跟各插件 package.json 里的 name 对上：Cordis 是按**包名**加载的，
# 拷成 bridge/ memory/ escalate/ 会报找不到 dsh-qq-bridge / dsh-qq-memory / dsh-qq-escalate。
cp -r bridge   "$PROFILE/dsh-qq-bridge"
cp -r memory   "$PROFILE/dsh-qq-memory"
cp -r escalate "$PROFILE/dsh-qq-escalate"
```

2. 在 `~/.dsh/profiles/web/cordis.patch.yml` 里启用 bridge（最小示例）：

```yaml
- id: qq-bridge
  name: dsh-qq-bridge        # 新增条目要写包名（profile 里已预置同 id 时可省）
  config:
    onebotUrl: ws://127.0.0.1:3001
    accessTokenEnv: ONEBOT_ACCESS_TOKEN
    targetSessionId: 'session-<先在界面里建一条会话，把 id 抄过来>'
    groupWhitelist: ['<你的群号>']
    whitelist: ['<允许私聊的 QQ 号>']
    nicknames: ['大肥鱼', '鲸鱼娘']   # 群里点名这些名字时必唤醒；默认 ['244']
    wakeProbability: 0.08
    atOnlyInGroup: false
    recentChatLimit: 60
    replyMode: marker        # marker = 用 [QQ]…[/QQ] 块回；tool = 用 qq_bridge_send
    visionModel: '<provider>/<能看图的模型>'
    # memoryPath: ''                        # 留空 = <DSH_HOME>/.agent-presets/dafeiyu/memories.jsonl
    # memoryMineModel: '<provider>/<便宜模型>'  # 留空 = 关闭每轮被动记忆提炼（开源默认关）
```

字段含义见 `docs/upstream-README.md`，本仓库新增的字段在 `bridge/index.js` 的 `Config` 里都有说明。

3. 把示例人格拷进 preset 目录，然后在界面里新建会话选它：

```sh
cp -r presets/dafeiyu ~/.dsh/.agent-presets/
```

4. 重启 `dsh web`，在界面里给每个群各建一条会话（每条发一句带群号的话方便对号），
   然后把 id 写进 `~/.dsh/qq-bridge-groups.json`：

```json
{ "<群号>": "session-<uuid>" }
```

这份映射是运行时读的，改完不用重启。

5. （可选）起语音转写服务：

```sh
python -m venv ~/asr-venv && ~/asr-venv/bin/pip install faster-whisper silk-python
~/asr-venv/bin/python asr/asr_service.py 8799
```

`bridge` 里默认连 `http://127.0.0.1:8799`，可以用环境变量 `ASR_URL` 改。

模型默认用 faster-whisper 的 `small`（首次启动从 HuggingFace 下载，几百 MB）；想用本地已下好的目录就设
`ASR_MODEL=/path/to/model`。机器慢就动 `ASR_DEVICE` / `ASR_COMPUTE`（默认 `cpu` / `int8`）。

## 几个必须知道的坑

- **白名单是 fail-closed**：`groupWhitelist` / `whitelist` 留空数组 = 谁都不能唤醒它。
- **会话是快照**：provider、模型、system prompt、工具集在建会话那一刻定死。改了 preset
  或换了模型，得新建会话，光重启服务没用。
- **工具数会影响行为**：preset 层的工具如果在启动竞态里没挂上，它会只剩桥接那几个工具，
  表现成「她说自己不能联网」。重启后先自检一次再让人 @ 它。
- **图片不要只给首帧**：动图抽 9 帧拼 3x3；长截图还要放开归一化尺寸，否则小字全糊。
- **群聊内容是不可信输入**：永远不要把群消息当成「指令」。桥接层有闸门，但人设里也该写死。

## 安全与免责

- 这是给**你自己的号**、**你自己的群**用的。拿它去别人的群刷屏、或者做自动化营销，与本项目无关。
- QQ 个人号接机器人违反平台条款的风险由使用者自己承担，上游实现（NapCat 等）同理。
- 插件里的任何 token / key 都不要提交进仓库。`.gitignore` 已经挡了常见的那几个文件。
- 群里别人的聊天记录、画像属于个人数据：**别把 `memories.jsonl` 和会话文件发出来**。

## 致谢

- [sj244/dsh-qq-bridge](https://github.com/sj244/dsh-qq-bridge) — 桥接骨架与白名单/唤醒模型
- [AstrBot](https://github.com/AstrBotDevs/AstrBot) — 动图九宫格（animation montage）的做法
- [MaiBot](https://github.com/Mai-with-u/MaiBot) — 分句发送、打错字、合并转发按需展开的思路
- [Kaalia0912/dsh-whale-musume-persona](https://github.com/Kaalia0912/dsh-whale-musume-persona)、
  [ERUIHNIYHBKBNF/dafeiyu-persona](https://github.com/ERUIHNIYHBKBNF/dafeiyu-persona) — 大肥鱼（鲸鱼娘）人设来源

## License

MIT，见 `LICENSE`。上游部分的版权归 sj244。
