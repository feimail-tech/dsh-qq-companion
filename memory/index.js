/**
 * qq-memory — 群里机器人的群友记忆。
 *
 * 参考 AstrBot 的 user-memory 插件思路：记忆不是流水账，而是按人聚合的
 * 原子条目，分四类：
 *   trait    —— 这个人什么脾气、说话什么调子
 *   episode  —— 最近在干什么、发生过什么事
 *   relation —— 他是谁的谁（跟群里谁熟、谁的对象）
 *   alias    —— 大家怎么叫他（外号、缩写、错字）
 *   fact     —— 其他稳定事实（住哪、养猫、专业）
 *
 * 存储：一个 jsonl，一行一条，按 who（QQ 号）聚合。
 * 写入时做近似去重——同一人同一类、文本高度重合就更新不追加。
 */
import { readFileSync, writeFileSync, appendFileSync, existsSync, mkdirSync } from 'node:fs'
import { homedir } from 'node:os'
import { dirname } from 'node:path'
import Schema from '@deepseek-ai/schemastery'
import { defineTool } from '@deepseek-ai/dsh-tools'

export const name = 'qq-memory'
export const inject = ['tools']

export const Config = Schema.object({
  path: Schema.string().default(`${process.env.DSH_HOME || homedir() + '/.dsh'}/.agent-presets/dafeiyu/memories.jsonl`)
    .description('记忆文件路径（jsonl）'),
  maxRecall: Schema.number().default(20).description('单次回忆最多返回多少条'),
})

const TYPES = ['trait', 'episode', 'relation', 'alias', 'fact']

function loadAll(file) {
  if (!existsSync(file)) return []
  const out = []
  for (const line of readFileSync(file, 'utf8').split('\n')) {
    const s = line.trim()
    if (!s) continue
    try { out.push(JSON.parse(s)) } catch { /* 跳过坏行 */ }
  }
  return out
}

function saveAll(file, rows) {
  mkdirSync(dirname(file), { recursive: true })
  writeFileSync(file, rows.map(r => JSON.stringify(r)).join('\n') + '\n', 'utf8')
}

/** 字二元组集合（去掉空白）。similar 与 containment 共用。 */
function bigrams(s) {
  const g = new Set()
  const t = String(s).replace(/\s+/g, '')
  for (let i = 0; i < t.length - 1; i++) g.add(t.slice(i, i + 2))
  return g
}

/**
 * 整体相似度：字二元组 Jaccard（交集 / 并集）。
 * 分母用**并集**而不是较短那一串：短句被长句包住时不会算出 1.0，
 * 否则「先记了短句、后来补上细节的长句」会被当成完全重复直接跳过，新信息就丢了。
 */
function similar(a, b) {
  const A = bigrams(a), B = bigrams(b)
  if (!A.size || !B.size) return 0
  let hit = 0
  for (const x of A) if (B.has(x)) hit++
  return hit / (A.size + B.size - hit)
}

/**
 * 重合率：交集 / 较短那一串。判断「一条是否基本被另一条整条包住」。
 * Jaccard 在长短悬殊时会掉到很低（5 字 vs 16 字 ≈ 0.27），光靠它认不出「短句是长句的一部分」，
 * 所以撞车判定是两道一起看：够像（Jaccard > 0.75）**或者**整条被包住（containment > 0.95）。
 */
function containment(a, b) {
  const A = bigrams(a), B = bigrams(b)
  if (!A.size || !B.size) return 0
  let hit = 0
  for (const x of A) if (B.has(x)) hit++
  return hit / Math.min(A.size, B.size)
}

export function apply(ctx, config) {
  ctx.tools.register(defineTool({
    name: 'memory_remember',
    description: '记下关于某个群友的一件事。只记值得长期记住的：他的脾气、最近在忙什么、跟群里谁有关系、大家怎么叫他。别记流水账、别记一次性的闲聊内容。同一个人同一类里已经有很接近的条目时会更新它，不会堆重复。',
    parameters: {
      who: { type: 'string', required: true, description: '那个人的 QQ 号；不知道就填他的群昵称。' },
      name: { type: 'string', description: '他现在用的昵称。' },
      type: { type: 'string', required: true, enum: TYPES, description: 'trait 脾气｜episode 最近的事｜relation 关系｜alias 外号｜fact 其他事实' },
      text: { type: 'string', required: true, description: '一句话说完，第三人称。写「最近在找工作」而不是「你说你在找工作」。' },
    },
    output: {
      schema: {
        type: 'object',
        additionalProperties: false,
        properties: {
          stored: { type: 'string', required: true, enum: ['added', 'updated', 'skipped'] },
          total: { type: 'integer', required: true },
        },
      },
      render: (_a, v) => [{
        type: 'text',
        text: v.stored === 'added'
          ? `记下了（这个人现在有 ${v.total} 条）。`
          : v.stored === 'updated'
            ? `更新了原来那条（这个人现在有 ${v.total} 条）。`
            : '这条跟已记的差不多，就不重复记了。',
      }],
    },
    async execute(args) {
      const file = config.path
      const rows = loadAll(file)
      const who = String(args.who).trim()
      const text = String(args.text).trim()
      const type = TYPES.includes(args.type) ? args.type : 'fact'
      const now = Date.now()

      const same = rows.filter(r => r.who === who && r.type === type)
      // 撞车判定：整体够像（Jaccard > 0.75），或者一条被另一条整条包住（短句 ↔ 补了细节的长句）。
      const dup = same.find(r => similar(r.text, text) > 0.75 || containment(r.text, text) > 0.95)
      if (dup) {
        const oldText = String(dup.text ?? '')
        const newText = String(text)
        // 几乎一模一样：不写。
        if (similar(oldText, newText) > 0.95) return { stored: 'skipped', total: same.length }
        // 旧的整条被新的包住、且新的更长 → 用新的覆盖，把补上的细节留住。
        const newerCovers = containment(oldText, newText) > 0.95 && newText.length > oldText.length
        // 反过来（新的更短或等长、被旧的整条包住）→ 保留原来那条更全的，别拿短的盖长的。
        const olderCovers = containment(newText, oldText) > 0.95
        if (!newerCovers && olderCovers) return { stored: 'skipped', total: same.length }
        dup.text = text
        dup.at = now
        if (args.name) dup.name = String(args.name)
        saveAll(file, rows)
        return { stored: 'updated', total: same.length }
      }

      const row = { who, type, text, at: now }
      if (args.name) row.name = String(args.name)
      appendFileSync(file, JSON.stringify(row) + '\n', 'utf8')
      return { stored: 'added', total: same.length + 1 }
    },
  }))

  ctx.tools.register(defineTool({
    name: 'memory_recall',
    description: '翻自己记的东西。聊到某个群友之前先查一下，免得问重复的问题或者说错话。可以按人查，也可以按关键词查。',
    parameters: {
      who: { type: 'string', description: '只看这个人的（QQ 号或昵称）。' },
      query: { type: 'string', description: '关键词，匹配记忆内容。' },
    },
    output: {
      schema: {
        type: 'object',
        additionalProperties: false,
        properties: {
          text: { type: 'string', required: true },
          count: { type: 'integer', required: true },
        },
      },
      render: (_a, v) => [{ type: 'text', text: v.text }],
    },
    async execute(args) {
      const rows = loadAll(config.path)
      let hit = rows
      if (args.who) {
        const w = String(args.who).trim().toLowerCase()
        hit = hit.filter(r => String(r.who).toLowerCase().includes(w) || String(r.name || '').toLowerCase().includes(w))
      }
      if (args.query) {
        const q = String(args.query).trim().toLowerCase()
        hit = hit.filter(r => String(r.text).toLowerCase().includes(q))
      }
      hit = hit.sort((a, b) => b.at - a.at).slice(0, config.maxRecall)
      if (!hit.length) return { text: '这个还没记过什么。', count: 0 }
      const text = hit.map(r => {
        const when = new Date(r.at).toLocaleDateString('zh-CN')
        return `- ${r.name || r.who}｜${r.type}｜${r.text}（${when}）`
      }).join('\n')
      return { text, count: hit.length }
    },
  }))
}
