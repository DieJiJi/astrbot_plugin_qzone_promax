"""
astrbot_plugin_qzone_promax —— QQ动态ProMax

在原版 astrbot_plugin_qzone (GPL-3.0, 作者 Zhalslar) 的核心层之上重做，
把「评论」和「发动态」两件事合并进同一个插件，并提供细粒度的频次/额度控制。

【评论侧】
1. 指定内容评论：主人说什么就评论什么（原版无法由对话指定评论内容）
2. 智能评论：带上说说正文 / 图片数 / 已有评论一起生成，说人话
3. 楼中楼自主回复：定时巡检"回复了我评论"的人，自动生成并发出回复
   - 一轮回几个人、每人每天回几条、同一主楼回几条、同一条评论接几轮，均可配

【发动态侧】
4. 手动发说说：命令「发说说 <主题>」或让 AI 用工具发
5. 定时发说说：可配置每天发几条、在哪些时间点发（默认关闭）
   - 素材来源可选：聊天记录 / 自主选题 / 科技AI新闻
   - 可选先发给主人审核再发布

本项目为原项目的衍生作品，遵循 GPL-3.0 协议，署名原作者并保留原仓库链接。
原项目：https://github.com/Zhalslar/astrbot_plugin_qzone
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import re
import time

from astrbot.api import logger
from astrbot.api.event import filter
from astrbot.api.star import Context, Star
from astrbot.core import AstrBotConfig
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

from .core.config import (  # noqa: F401  这些名字必须在本模块可见，供 ConfigNode 反射类型注解使用
    ConfigNode,
    LLMConfig,
    PluginConfig,
    PublishConfig,
    SourceConfig,
    TriggerConfig,
)
from .core.db import PostDB
from .core.llm_action import LLMAction
from .core.model import Post
from .core.qzone import QzoneAPI, QzoneParser, QzoneSession
from .core.sender import Sender
from .core.service import PostService

# 巡检记录保留天数：不再写死，改由配置 pro.watch_ttl_days 决定（见 _watch_ttl_sec）
WATCH_TTL_DAYS_FALLBACK = 3
# 最多同时盯着多少条说说
WATCH_MAX = 30
# 轻量嗅探时每人一次最多看几条说说（不带详情，代价很低）
SNIPE_NUM = 3


class ProConfig(ConfigNode):
    """本插件新增的配置"""

    reply_policy: str
    owner_qq: str
    notify_owner: bool
    poll_interval_min: int
    reply_cooldown_min: int
    max_replies_per_round: int
    daily_reply_limit: int
    per_user_daily_limit: int
    max_replies_per_thread: int
    reply_own_post_comments: bool
    scan_post_count: int
    reply_gap_sec: int
    smart_style: str
    max_comment_len: int
    per_comment_round_limit: int
    # 事件驱动窗口巡检：谁的空间值得盯 + 盯多久
    watch_ttl_days: int
    watch_qqs: str


class QzonePromaxConfig(PluginConfig):
    pro: ProConfig
    publish: PublishConfig

    _plugin_name = "astrbot_plugin_qzone_promax"

    # ConfigNode 对缺失字段只打 warning，不填默认值；
    # 而嵌套的配置组整个缺失时会直接抛 TypeError。
    # 这里兜底补齐，保证从旧版配置迁移过来也能正常加载（已改过的项保持原样）。
    _PRO_DEFAULTS: dict = {
        "reply_policy": "friends",
        "owner_qq": "",
        "notify_owner": True,
        "poll_interval_min": 10,
        "reply_cooldown_min": 30,
        "max_replies_per_round": 3,
        "daily_reply_limit": 20,
        "per_user_daily_limit": 3,
        "max_replies_per_thread": 2,
        "reply_own_post_comments": True,
        "scan_post_count": 3,
        "reply_gap_sec": 8,
        "smart_style": "像真人朋友一样随口接话，不要客套，不要总结，不要写成朋友圈文案。",
        "max_comment_len": 40,
        "per_comment_round_limit": 1,
    }

    _PUBLISH_DEFAULTS: dict = {
        "auto_enabled": False,
        "times_per_day": 1,
        "at_hours": "21",
        "jitter_min": 20,
        "max_per_day": 2,
        "gap_min": 180,
        "need_confirm": False,
        "topic_source": "chat",
        "style_prompt": "像真人随手发的一条说说，别写成朋友圈文案，别加话题标签，别自我解释。",
        "max_len": 80,
    }

    def __init__(self, cfg, context):
        super().__init__(cfg, context)
        self._ensure_group(cfg, "pro", self._PRO_DEFAULTS)
        self._ensure_group(cfg, "publish", self._PUBLISH_DEFAULTS)

    @staticmethod
    def _ensure_group(cfg, name: str, defaults: dict):
        """配置组缺失或字段不全时补默认值，用户改过的项保持不动"""
        cur = cfg.get(name)
        if not isinstance(cur, dict):
            cfg[name] = dict(defaults)
            return
        for k, v in defaults.items():
            cur.setdefault(k, v)


class QzonePromaxPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.context = context
        self.cfg = QzonePromaxConfig(config, context)
        self.session = QzoneSession(self.cfg)
        self.qzone = QzoneAPI(self.session, self.cfg)
        self.db = PostDB(self.cfg)
        self.llm = LLMAction(self.cfg)
        self.sender = Sender(self.cfg)
        self.service = PostService(self.qzone, self.session, self.db, self.llm)

        self._task: asyncio.Task | None = None
        self._last_reply_ts = 0.0
        self._replied: set[str] = set()  # 已处理过的 "post_tid:comment_tid"
        self._watched: list[dict] = []  # 巡检窗口：正在盯的说说
        self._watch_cursor = 0
        self._snipe_cursor = 0  # 轻量嗅探轮转游标（每轮只查一个人，省调用）

        # 每日预算（防烧钱）
        self._daily_date = ""
        self._daily_total = 0
        self._daily_users: dict[str, int] = {}
        self._quota_warned = ""  # 当天额度耗尽的提示只打一次，别刷日志
        self._primed = False  # 冷启动保护：首次巡检只标记不回复
        self._throttle_until = 0.0  # 撞上频率限制后的退避截止时间

        # 发动态的每日额度（和楼中楼回复额度分开算）
        self._pub_date = ""
        self._pub_count = 0
        self._pub_last_ts = 0.0
        self._pub_scheduler = None  # 定时发动态的调度器（仅开启时创建）
        self._pending_publish = ""  # 待主人审核的动态正文

        self._state_path = self.cfg.data_dir / "pro_state.json"
        self._load_state()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    async def initialize(self):
        """插件加载时触发"""
        await self.db.initialize()
        self._capture_client()
        if self.cfg.pro.reply_policy != "off" and self._task is None:
            self._task = asyncio.create_task(self._reply_loop())
            logger.info(
                f"[qzone-promax] 楼中楼巡检已启动：策略={self.cfg.pro.reply_policy}，"
                f"间隔={self.cfg.pro.poll_interval_min} 分钟"
            )
        self._start_auto_publish()

    async def terminate(self):
        if self._task and not self._task.done():
            self._task.cancel()
            self._task = None
        self._stop_auto_publish()
        self._save_state()
        logger.info("[qzone-promax] 已停止")

    # ------------------------------------------------------------------
    # 定时发动态（默认关闭，配置里把 auto_enabled 打开才会生效）
    # ------------------------------------------------------------------
    def _start_auto_publish(self):
        p = self.cfg.publish
        if not p.auto_enabled:
            logger.info("[qzone-promax] 定时发动态：关闭（需要可在插件配置里开启）")
            return
        hours = p.hours()
        if not hours:
            logger.warning("[qzone-promax] 定时发动态已开启，但「时间点」为空，已跳过")
            return
        try:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler
            from apscheduler.triggers.cron import CronTrigger

            sched = AsyncIOScheduler(timezone=self.cfg.timezone)
            sched.start()
            for h in hours:
                minute = self._pick_minute(h)
                sched.add_job(
                    func=self._auto_publish_job,
                    trigger=CronTrigger(
                        hour=h, minute=minute, timezone=self.cfg.timezone
                    ),
                    name=f"qzone_promax_publish_{h:02d}{minute:02d}",
                    max_instances=1,
                )
            self._pub_scheduler = sched
            logger.info(
                f"[qzone-promax] 定时发动态已启动：每天 {hours} 点，"
                f"每天 {p.times_per_day} 条，抖动 ±{p.jitter_min} 分钟"
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 定时发动态启动失败: {e}")

    def _pick_minute(self, hour: int) -> int:
        """给某个整点挑一个随机分钟，避免看起来像定点打卡"""
        import random as _random

        j = max(0, min(59, int(self.cfg.publish.jitter_min or 0)))
        if not j:
            return 0
        return max(0, min(59, 30 + _random.randint(-j, j)))

    def _stop_auto_publish(self):
        sched = getattr(self, "_pub_scheduler", None)
        if not sched:
            return
        try:
            sched.shutdown(wait=False)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[qzone-promax] 关闭定时发动态时忽略异常: {e}")
        self._pub_scheduler = None

    def _publish_topic(self, topic: str) -> str:
        """按素材来源，把用户的主题提示加工成最终的任务主题"""
        source = (self.cfg.publish.topic_source or "chat").strip()
        topic = (topic or "").strip()
        if source == "topic":
            return topic or "随便挑一件你此刻想说的日常小事"
        if source == "news":
            return (
                topic
                or "从今天刷到的科技/AI新闻里挑一件你觉得最有趣的，说一句你的看法"
            )
        return topic

    async def _auto_publish_job(self):
        """定时发动态：生成 → 按需审核 → 发布"""
        logger.info("[qzone-promax] 定时发动态任务触发")
        if self._pub_left() <= 0:
            logger.info("[qzone-promax] 今日发动态额度已用完，跳过")
            return
        try:
            text = await self.service.llm.generate_free_post(
                topic=self._publish_topic(""),
                style_prompt=(self.cfg.publish.style_prompt or "").strip(),
                event=None,
            )
        except Exception as e:  # noqa: BLE001
            logger.error(f"[qzone-promax] 自动生成动态内容失败：{e}")
            return
        text = self._clean_publish_text(text)
        if not text:
            logger.error("[qzone-promax] 自动生成的动态内容为空，跳过")
            return

        if self.cfg.publish.need_confirm:
            self._pending_publish = text
            await self._notify_owner(
                "【待审核动态】\n" + text + "\n\n回复「发」我就发出去"
            )
            return

        result = await self._do_publish(None, text=text)
        logger.info(f"[qzone-promax] 定时发动态结果：{result}")
        await self._notify_owner("【自动发动态】" + result)

    # ------------------------------------------------------------------
    # 状态持久化：已回复记录 + 我评论过的说说
    # ------------------------------------------------------------------
    def _load_state(self):
        self._primed = self._state_path.exists()
        try:
            if self._state_path.exists():
                data = json.loads(self._state_path.read_text(encoding="utf-8"))
                self._replied = set(data.get("replied", []))
                self._watched = list(data.get("watched", []))
                daily = data.get("daily") or {}
                self._daily_date = str(daily.get("date") or "")
                self._daily_total = int(daily.get("total") or 0)
                self._daily_users = dict(daily.get("users") or {})
                pub = data.get("publish") or {}
                self._pub_date = str(pub.get("date") or "")
                self._pub_count = int(pub.get("count") or 0)
                self._pub_last_ts = float(pub.get("last_ts") or 0.0)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 读取历史失败（已忽略）: {e}")
            self._replied, self._watched = set(), []
        self._roll_daily()

    def _save_state(self):
        try:
            payload = {
                "primed": self._primed,
                "replied": sorted(self._replied)[-2000:],
                "watched": self._watched[-WATCH_MAX:],
                "daily": {
                    "date": self._daily_date,
                    "total": self._daily_total,
                    "users": self._daily_users,
                },
                "publish": {
                    "date": self._pub_date,
                    "count": self._pub_count,
                    "last_ts": self._pub_last_ts,
                },
            }
            self._state_path.write_text(
                json.dumps(payload, ensure_ascii=False), encoding="utf-8"
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 保存历史失败: {e}")

    # ------------------------------------------------------------------
    # 每日预算：到点自动清零，用满就停手
    # ------------------------------------------------------------------
    def _today(self) -> str:
        try:
            return dt.datetime.now(self.cfg.timezone).strftime("%Y-%m-%d")
        except Exception:  # noqa: BLE001
            return time.strftime("%Y-%m-%d")

    def _roll_daily(self):
        """跨天则清零每日额度"""
        today = self._today()
        if self._daily_date != today:
            if self._daily_date:
                logger.info(
                    f"[qzone-promax] 新的一天，昨日共自动回复 {self._daily_total} 条，额度已重置"
                )
            self._daily_date = today
            self._daily_total = 0
            self._daily_users = {}

    def _daily_left(self) -> int:
        """今天还能回几条"""
        return max(0, self.cfg.pro.daily_reply_limit - self._daily_total)

    def _user_left(self, uid: str) -> int:
        """对这个人今天还能回几条"""
        used = self._daily_users.get(uid, 0)
        return max(0, self.cfg.pro.per_user_daily_limit - used)

    def _consume_quota(self, uid: str):
        self._daily_total += 1
        self._daily_users[uid] = self._daily_users.get(uid, 0) + 1
        self._save_state()

    # ------------------------------------------------------------------
    # 发动态的每日额度与间隔
    # ------------------------------------------------------------------
    def _roll_publish(self):
        """跨天则清零发动态额度"""
        today = self._today()
        if self._pub_date != today:
            if self._pub_date:
                logger.info(
                    f"[qzone-promax] 新的一天，昨日共发 {self._pub_count} 条动态，额度已重置"
                )
            self._pub_date = today
            self._pub_count = 0

    def _pub_left(self) -> int:
        """今天还能发几条动态"""
        self._roll_publish()
        return max(0, self.cfg.publish.max_per_day - self._pub_count)

    def _pub_gap_left_sec(self) -> float:
        """距离下次可发动态还剩多少秒（0 表示随时可发）"""
        gap = max(0, int(self.cfg.publish.gap_min)) * 60
        if not gap or not self._pub_last_ts:
            return 0.0
        return max(0.0, gap - (time.time() - self._pub_last_ts))

    def _consume_pub_quota(self):
        self._pub_count += 1
        self._pub_last_ts = time.time()
        self._save_state()

    # ------------------------------------------------------------------
    # 事件驱动巡检窗口
    # ------------------------------------------------------------------
    def _watch_ttl_sec(self) -> int:
        """窗口存活时长（秒）：一条说说从我开始盯它算起，超过就不再碰它的接口"""
        days = 0
        try:
            days = int(getattr(self.cfg.pro, "watch_ttl_days", 0) or 0)
        except (TypeError, ValueError):
            days = 0
        # 没配（或配了非法值）就退回默认天数，宁短勿长——短了顶多少回几句，长了白烧请求
        if days <= 0:
            days = WATCH_TTL_DAYS_FALLBACK
        return max(1, days) * 86400

    def _watch_targets(self) -> list[str]:
        """要嗅探谁的新动态（主人默认算一个，可再加其他人）"""
        raw = str(getattr(self.cfg.pro, "watch_qqs", "") or "").strip()
        out: list[str] = []
        for token in re.split(r"[,\s;，、]+", raw):
            token = token.strip()
            if token.isdigit() and token not in out:
                out.append(token)
        owner = self._owner_qq()
        if owner.isdigit() and owner not in out:
            out.insert(0, owner)
        return out

    def _open_watch(self, post: Post) -> bool:
        """开一个巡检窗口

        触发时机：机器人刚发动态 / 机器人评论了某条说说 / 嗅探到名单里的人发了新动态。
        返回 True 表示这是新开的窗口。
        """
        tid = str(post.tid or "")
        if not tid:
            return False
        now = int(time.time())
        # 太老的说说不开窗：帖子本身都超过窗口期限了，再去盯没有意义
        created = int(getattr(post, "create_time", 0) or 0)
        if created and now - created > self._watch_ttl_sec():
            return False

        key = f"{post.uin}:{tid}"
        for item in self._watched:
            if f"{item.get('uin')}:{item.get('tid')}" == key:
                item["ts"] = now
                return False
        self._watched.append(
            {
                "uin": post.uin,
                "tid": tid,
                "ts": now,  # 开窗时刻，TTL 从这里算
                "created": created or now,  # 帖子发布时间，仅用于日志判龄
            }
        )
        self._prune_watch()
        self._save_state()
        logger.info(
            f"[qzone-promax] 巡检窗口已开启：{key}（盯 "
            f"{self._watch_ttl_sec() // 86400} 天）"
        )
        return True

    async def _bootstrap_watch(self, uin: int) -> None:
        """冷启动：把自己窗口期内最新的动态纳入巡检（只拉列表不带详情）"""
        try:
            own = await self.service.query_feeds(
                target_id=str(uin), pos=0, num=SNIPE_NUM, with_detail=False
            )
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[qzone-promax] 拉取自身动态失败: {e}")
            return
        opened = 0
        for p in own:
            if int(getattr(p, "uin", 0) or 0) == 0:
                p.uin = uin
            if self._open_watch(p):
                opened += 1
        if opened:
            logger.info(f"[qzone-promax] 冷启动纳入自身动态 {opened} 条进入巡检")

    async def _snipe_new_posts(self, uin: int) -> None:
        """轻量嗅探名单里的人有没有发新动态

        每轮只查一个人的前几条（不带详情，代价远低于巡检详情），发现新的才开窗。
        平静期整个插件就只有这 1 次请求，而不是把每个人的空间全拉一遍。
        """
        targets = [q for q in self._watch_targets() if q != str(uin)]
        if not targets:
            return
        target = targets[self._snipe_cursor % len(targets)]
        self._snipe_cursor = (self._snipe_cursor + 1) % len(targets)
        try:
            feeds = await self.service.query_feeds(
                target_id=target, pos=0, num=SNIPE_NUM, with_detail=False
            )
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[qzone-promax] 嗅探 {target} 失败: {e}")
            return
        opened = 0
        for p in feeds:
            if int(getattr(p, "uin", 0) or 0) == 0 and target.isdigit():
                p.uin = int(target)
            if self._open_watch(p):
                opened += 1
        if opened:
            logger.info(f"[qzone-promax] 嗅探 {target} 新动态 {opened} 条，已开启巡检")

    def _prune_watch(self):
        now = int(time.time())
        ttl = self._watch_ttl_sec()
        before = len(self._watched)
        self._watched = [
            w for w in self._watched if now - int(w.get("ts", 0)) < ttl
        ][-WATCH_MAX:]
        if len(self._watched) < before:
            logger.info(
                f"[qzone-promax] 到期关闭巡检窗口 {before - len(self._watched)} 个，"
                f"当前还剩 {len(self._watched)} 个"
            )

    def _next_watched(self, n: int) -> list[dict]:
        """轮转取出本轮要巡检的外部说说，避免每次都打同几条"""
        if not self._watched or n <= 0:
            return []
        n = min(n, len(self._watched))
        out = []
        for _ in range(n):
            out.append(self._watched[self._watch_cursor % len(self._watched)])
            self._watch_cursor = (self._watch_cursor + 1) % len(self._watched)
        return out

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------
    def _capture_client(self):
        """抓 aiocqhttp 的 bot 实例，供后台任务主动发消息"""
        if self.cfg.client is not None:
            return self.cfg.client
        try:
            platform = self.context.get_platform("aiocqhttp")
        except Exception:  # noqa: BLE001
            platform = None
        if platform is None:
            try:
                for inst in self.context.platform_manager.platform_insts:
                    if getattr(inst.meta(), "name", "") == "aiocqhttp":
                        platform = inst
                        break
            except Exception:  # noqa: BLE001
                platform = None
        bot = getattr(platform, "bot", None)
        if bot is not None:
            self.cfg.client = bot
        return self.cfg.client

    def _owner_qq(self) -> str:
        return str(self.cfg.pro.owner_qq or self.cfg.admin_id or "").strip()

    @staticmethod
    def _clean(text: str, limit: int) -> str:
        """清掉 AI 可能带出来的 @/表情码/HTML，压缩空白并限长"""
        t = (text or "").strip()
        t = re.sub(r"\{uin:\d+[^}]*\}", "", t)  # @{uin:123,nick:xx,who:1}
        t = re.sub(r"\[em\]e?\d+\[/em\]", "", t)  # QQ 旧版表情码
        t = re.sub(r"<[^>]+>", "", t)  # HTML 标签
        t = re.sub(r"^\s*@\S+\s*", "", t)  # 开头的 @某人
        t = re.sub(r"[\s\u3000]+", " ", t).strip().strip('"“”')
        if limit and len(t) > limit:
            t = t[:limit].rstrip("，,、；;：: ")
        return t

    async def _notify_owner(self, text: str):
        """把自动回复结果私信给主人"""
        if not self.cfg.pro.notify_owner:
            return
        owner = self._owner_qq()
        client = self._capture_client()
        if not owner.isdigit() or client is None:
            return
        try:
            await client.send_private_msg(user_id=int(owner), message=text)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 通知主人失败: {e}")

    # ------------------------------------------------------------------
    # 内容生成
    # ------------------------------------------------------------------
    async def _gen_smart_comment(self, post: Post) -> str:
        """智能评论：带上正文 / 图片数 / 已有评论一起生成；失败回退插件自带逻辑"""
        try:
            provider = self.context.get_using_provider()
            if provider is None:
                return await self._fallback_comment(post)
            exists = (
                "\n".join(
                    f"- {c.nickname}：{c.content}" for c in (post.comments or [])[-5:]
                )
                or "（暂无评论）"
            )
            system_prompt = (
                "你是一个真实的人，正在 QQ 空间里评论好友的说说。"
                "说人话、短句、口语化，像随手接一句，不要客套、不要总结、不要写成朋友圈文案。"
            )
            prompt = (
                f"## 这条说说\n{post.text or '（无文字）'}\n"
                f"图片：{len(post.images or [])} 张\n\n"
                f"## 已有评论\n{exists}\n\n"
                f"## 你的任务\n写一句评论，不超过 {self.cfg.pro.max_comment_len} 字，"
                f"不要和已有评论重复，只输出评论本身。\n风格提示：{self.cfg.pro.smart_style}"
            )
            resp = await provider.text_chat(
                system_prompt=system_prompt, prompt=prompt, contexts=[]
            )
            text = self._clean(resp.completion_text, self.cfg.pro.max_comment_len)
            if text:
                return text
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 智能评论生成失败，回退默认: {e}")
        return await self._fallback_comment(post)

    async def _fallback_comment(self, post: Post) -> str:
        try:
            return await self.llm.generate_comment(post) or ""
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 默认评论生成也失败: {e}")
            return ""

    async def _gen_reply_text(self, post: Post, comment) -> str:
        """生成楼中楼回复"""
        try:
            provider = self.context.get_using_provider()
            if provider is None:
                return await self._fallback_reply(post, comment)
            system_prompt = (
                "有人在 QQ 空间里回复了你的评论。你是一个真实的人，"
                "自然地接一句，短句、口语化，不要客套、不要解释。"
            )
            prompt = (
                f"## 这条说说\n{post.text or '（无文字）'}\n\n"
                f"## {comment.nickname} 回复你\n{comment.content}\n\n"
                f"## 你的任务\n写一句回复，不超过 {self.cfg.pro.max_comment_len} 字，"
                f"只输出回复内容。\n风格提示：{self.cfg.pro.smart_style}"
            )
            resp = await provider.text_chat(
                system_prompt=system_prompt, prompt=prompt, contexts=[]
            )
            text = self._clean(resp.completion_text, self.cfg.pro.max_comment_len)
            if text:
                return text
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 回复生成失败，回退默认: {e}")
        return await self._fallback_reply(post, comment)

    async def _fallback_reply(self, post: Post, comment) -> str:
        try:
            return await self.llm.generate_reply(post, comment) or ""
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 默认回复生成也失败: {e}")
            return ""

    # ------------------------------------------------------------------
    # 功能三：楼中楼自主回复
    # ------------------------------------------------------------------
    async def _wait_client(self, timeout: int = 120) -> bool:
        """等 NapCat 的反向 WS 真的连上来

        插件加载时 aiocqhttp 的 bot 对象可能已经有了，但反向 WS 还没握手，
        这时候调 API 会直接抛 ApiNotAvailable。所以必须真的试一次才知道。
        """
        waited = 0
        while waited < timeout:
            self._capture_client()
            try:
                await self.session.get_uin()
                return True
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(5)
            waited += 5
        return False

    async def _reply_loop(self):
        """后台巡检循环"""
        if not await self._wait_client():
            logger.warning(
                "[qzone-promax] 没等到 aiocqhttp 平台，巡检照跑，但可能拿不到登录态"
            )
        # 冷启动补一遍：发布钩子覆盖不到「重启之前发的动态」，这里一次性纳入
        try:
            uin0 = int(await self.session.get_uin() or 0)
            if uin0:
                await self._bootstrap_watch(uin0)
        except Exception as e:  # noqa: BLE001
            logger.debug(f"[qzone-promax] 冷启动补充巡检窗口失败: {e}")
        while True:
            try:
                results = await self.scan_and_reply()
                if results:
                    logger.info(
                        f"[qzone-promax] 本轮自动回复 {len(results)} 条："
                        + " | ".join(results)
                    )
                    await self._notify_owner(
                        "【QQ空间】我刚帮你回了：\n" + "\n".join(results)
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.error(f"[qzone-promax] 巡检异常: {e}")
            await asyncio.sleep(max(60, self.cfg.pro.poll_interval_min * 60))

    async def _collect_posts(self, uin: int) -> list[Post]:
        """收集本轮要检查的说说

        事件驱动模式：只查「巡检窗口」里的说说。
        窗口由三件事打开——机器人发动态、机器人评论过、嗅探到名单里的人发新动态。
        窗口为空时这里是 0 次请求，平静期不再无谓地刷接口。
        """
        posts: list[Post] = []
        seen: set[str] = set()

        # 先做一次轻量嗅探，看看名单里的人有没有发新动态（每轮只查一个人）
        await self._snipe_new_posts(uin)

        # 再对窗口内的说说逐条拉详情
        for w in self._next_watched(self.cfg.pro.scan_post_count):
            key = f"{w.get('uin')}:{w.get('tid')}"
            if key in seen:
                continue
            try:
                stub = Post(uin=int(w.get("uin") or 0), tid=str(w.get("tid") or ""))
                resp = await self.qzone.get_detail(stub)
                if not resp.ok or not resp.data:
                    continue
                parsed = QzoneParser.parse_feeds([resp.data])
                if not parsed:
                    continue
                seen.add(key)
                posts.append(parsed[0])
                await asyncio.sleep(1)
            except Exception as e:  # noqa: BLE001
                logger.warning(f"[qzone-promax] 巡检窗口内说说失败 {key}: {e}")

        return posts

    def _is_reply_target(self, post: Post, c, my_tids: set, uin: int) -> bool:
        """这条评论值不值得接话

        1) 别人回复了我的评论 —— 最核心的场景，永远处理
        2) 别人在我自己的说说下直接评论 —— 看 reply_own_post_comments 开关
        3) 别人在别人的说说下互相聊天 —— 与我无关，不插嘴
        """
        if c.uin == uin or not c.tid:
            return False
        if c.parent_tid and c.parent_tid in my_tids:
            return True
        if (
            post.uin == uin
            and not c.parent_tid
            and self.cfg.pro.reply_own_post_comments
        ):
            return True
        return False

    def _prime_state(self, posts: list[Post], uin: int) -> int:
        """冷启动：把此刻已经存在的评论全部标记成"早就见过了"，一条都不回

        否则第一次巡检会把几个月前的旧评论当成新回复，一口气全回一遍。
        """
        marked = 0
        for post in posts:
            comments = post.comments or []
            my_tids = {c.tid for c in comments if c.uin == uin and c.tid}
            for c in comments:
                if self._is_reply_target(post, c, my_tids, uin):
                    self._replied.add(f"{post.tid}:{c.tid}")
                    marked += 1
        return marked

    async def scan_and_reply(self) -> list[str]:
        """巡检：找出"回复了我评论"的人并自动回复"""
        policy = self.cfg.pro.reply_policy
        if policy == "off":
            return []

        # 插件加载时 aiocqhttp 平台可能还没就绪，所以每次扫描前再抓一次
        self._capture_client()

        self._roll_daily()

        # 今天的额度用完了就彻底停手，直接不进后面的流程
        daily_left = self._daily_left()
        if daily_left <= 0:
            if self._quota_warned != self._daily_date:
                self._quota_warned = self._daily_date
                logger.info(
                    f"[qzone-promax] 今日自动回复额度已用完（{self.cfg.pro.daily_reply_limit} 条），"
                    f"明天 0 点自动恢复"
                )
            return []

        # 上一轮撞过频率限制的话，退避窗口内直接躺平，别在风控期里反复试探
        if time.time() < self._throttle_until:
            return []

        cd = self.cfg.pro.reply_cooldown_min * 60
        limit = min(self.cfg.pro.max_replies_per_round, daily_left)
        if policy == "everyone":  # 激进档：冷却减半、单轮上限翻倍
            cd //= 2
            limit = min(self.cfg.pro.max_replies_per_round * 2, daily_left)
        if time.time() - self._last_reply_ts < cd:
            return []

        try:
            uin = await self.session.get_uin()
        except Exception as e:  # noqa: BLE001
            logger.warning(
                f"[qzone-promax] 未登录或登录态失效，跳过本轮: "
                f"{type(e).__name__}: {e!r}（client={self.cfg.client is not None}）"
            )
            return []

        owner = self._owner_qq()
        posts = await self._collect_posts(uin)

        # 冷启动：第一次巡检只把历史评论登记成"已见过"，不回任何一条
        if not self._primed:
            marked = self._prime_state(posts, uin)
            self._primed = True
            self._save_state()
            logger.info(
                f"[qzone-promax] 冷启动：已登记 {marked} 条历史评论为已读，"
                f"从现在起的新回复才会被处理"
            )
            return []

        # 先收集所有待回复项，再按时间倒序处理（最新的先回，像刷评论区一样）
        pending: list[tuple[Post, object, float]] = []
        for post in posts:
            comments = post.comments or []
            if not comments:
                continue
            my_tids = {c.tid for c in comments if c.uin == uin and c.tid}
            if not my_tids and not (
                post.uin == uin and self.cfg.pro.reply_own_post_comments
            ):
                continue
            for c in comments:
                if not self._is_reply_target(post, c, my_tids, uin):
                    continue
                pending.append((post, c, float(getattr(c, "create_time", 0) or 0)))

        pending.sort(key=lambda x: x[2], reverse=True)
        if pending:
            logger.info(
                f"[qzone-promax] 本轮扫到 {len(pending)} 条待回复候选，本轮最多处理 {limit} 条"
            )

        results: list[str] = []
        thread_used: dict[str, int] = {}
        throttled_posts: set[str] = set()  # 本轮撞上限流的说说，跳过它剩下的评论
        for post, c, _ts in pending:
            if len(results) >= limit or self._daily_left() <= 0:
                break

            if str(post.tid) in throttled_posts:
                continue

            key = f"{post.tid}:{c.tid}"
            if key in self._replied:
                continue

            comments = post.comments or []
            # 楼中楼限额：同一个主楼里我最多回 N 条，绝不在一个楼里连刷
            root_tid = c.parent_tid or c.tid
            already = sum(
                1
                for rc in comments
                if rc.uin == uin
                and (rc.parent_tid or rc.tid) == root_tid
                and rc.tid != c.tid
            )
            used = thread_used.get(root_tid, already)
            if used >= self.cfg.pro.max_replies_per_thread:
                continue
            # 轮次限制：对同一个人的同一条评论，最多来回接 N 轮
            # （1 = 对方评论我只回一句，再回就不再接了，防止两个人无限套娃）
            rounds = sum(
                1 for rc in comments if rc.uin == uin and rc.parent_tid == c.tid
            )
            if rounds >= max(1, int(self.cfg.pro.per_comment_round_limit or 1)):
                self._replied.add(key)
                continue
            if policy == "owner_only" and str(c.uin) != owner:
                continue
            if str(c.uin) in (self.cfg.source.ignore_users or []):
                continue
            # 这个人的当日额度用完了，直接跳过，不浪费一次 LLM 调用
            if self._user_left(str(c.uin)) <= 0:
                continue

            content = await self._gen_reply_text(post, c)
            if not content:
                continue

            resp = await self.qzone.reply(post, c, content)
            if not resp.ok:
                msg = resp.message or ""
                logger.warning(
                    f"[qzone-promax] 回复失败 [{c.nickname}({c.uin}) "
                    f"@说说{post.tid[:8]}]: {msg}"
                )
                # 「使用人数过多/操作频繁」是平台限流，硬怼只会延长风控，
                # 退避一整个巡检周期再试，并让本轮彻底收手
                if any(k in msg for k in ("人数过多", "稍后", "频繁", "太快")):
                    # 限流常常只针对单条说说，直接收手会导致每轮都撞同一条而卡死，
                    # 所以只跳过这条说说，其余照常处理
                    throttled_posts.add(str(post.tid))
                    self._throttle_until = time.time() + max(1800, cd)
                    logger.warning(
                        f"[qzone-promax] 触发平台限流，跳过说说 {post.tid[:8]}，"
                        f"巡检暂停 {max(1800, cd) // 60} 分钟后再试"
                    )
                continue

            self._replied.add(key)
            self._consume_quota(str(c.uin))
            self._last_reply_ts = time.time()
            thread_used[root_tid] = used + 1
            results.append(f"{c.nickname} 的「{c.content}」→ {content}")
            await asyncio.sleep(max(1, self.cfg.pro.reply_gap_sec))

        if results:
            self._save_state()
        return results

    # ------------------------------------------------------------------
    # 命令
    # ------------------------------------------------------------------
    async def _fetch_one(self, pos: int, user_id: str = "") -> Post | None:
        self._capture_client()
        try:
            posts = await self.service.query_feeds(
                target_id=user_id or None, pos=pos, num=1, with_detail=True
            )
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"拉取说说失败：{e}") from e
        return posts[0] if posts else None

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("发说说")
    async def cmd_publish(self, event: AiocqhttpMessageEvent):
        """发说说 [主题]：让她写一条动态并发到空间。不带主题就由她自己挑话题"""
        raw = event.message_str.strip()
        topic = raw.split(" ", 1)[1].strip() if " " in raw else ""
        yield event.plain_result("让我想想写点啥…")
        result = await self._do_publish(event, topic=topic)
        yield event.plain_result(result)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("发原话")
    async def cmd_publish_raw(self, event: AiocqhttpMessageEvent):
        """发原话 <内容>：按你给的原话，原封不动发一条动态"""
        parts = event.message_str.split(" ", 1)
        if len(parts) < 2 or not parts[1].strip():
            yield event.plain_result("用法：发原话 <内容>\n例如：发原话 今天天气不错")
            return
        result = await self._do_publish(event, text=parts[1].strip())
        yield event.plain_result(result)

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("删说说")
    async def cmd_delete_post(self, event: AiocqhttpMessageEvent):
        """删说说 [序号]：删掉自己空间里的第 N 条说说，默认 0（最新一条）"""
        parts = event.message_str.split(" ", 1)
        raw_pos = parts[1].strip() if len(parts) > 1 else "0"
        pos = int(raw_pos) if raw_pos.lstrip("-").isdigit() else 0
        try:
            self._capture_client()
            uin = str(await self.session.get_uin())
            posts = await self.service.query_feeds(
                target_id=uin, pos=pos, num=1, with_detail=False
            )
        except Exception as e:  # noqa: BLE001
            yield event.plain_result(f"拉取说说失败：{e}")
            return
        if not posts:
            yield event.plain_result("没找到这条说说")
            return
        post = posts[0]
        preview = (post.text or "").strip().replace("\n", " ")[:30]
        try:
            await self.service.delete_post(post)
        except Exception as e:  # noqa: BLE001
            yield event.plain_result(f"删除失败：{e}")
            return
        yield event.plain_result(f"已删除第 {pos} 条说说：{preview}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("动态配置")
    async def cmd_publish_status(self, event: AiocqhttpMessageEvent):
        """动态配置：查看发动态与楼中楼的当前设置"""
        p = self.cfg.publish
        pr = self.cfg.pro
        hours = p.hours()
        lines = [
            "【发动态】",
            f"定时自动发：{'开' if p.auto_enabled else '关'}",
            f"每天自动 {p.times_per_day} 条，时间点：{hours or '未设置'}",
            f"每天上限 {p.max_per_day} 条，两条间隔 ≥ {p.gap_min} 分钟",
            f"抖动 ±{p.jitter_min} 分钟，审核：{'要' if p.need_confirm else '不要'}",
            f"素材来源：{p.topic_source}，每条最多 {p.max_len} 字",
            "",
            "【楼中楼】",
            f"策略：{pr.reply_policy}，巡检间隔 {pr.poll_interval_min} 分钟",
            f"单轮最多回 {pr.max_replies_per_round} 人",
            f"每人每天最多 {pr.per_user_daily_limit} 条",
            f"同一主楼最多 {pr.max_replies_per_thread} 条",
            f"同一条评论最多接 {pr.per_comment_round_limit} 轮",
            f"回复间隔 {pr.reply_gap_sec} 秒，每条最多 {pr.max_comment_len} 字",
        ]
        yield event.plain_result("\n".join(lines))

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("评论说说")
    async def cmd_comment_specified(self, event: AiocqhttpMessageEvent):
        """评论说说 <序号> <内容>：按你指定的原话评论某条说说"""
        parts = event.message_str.split(" ", 2)
        if len(parts) < 3 or not parts[2].strip():
            yield event.plain_result(
                "用法：评论说说 <序号> <内容>\n例如：评论说说 0 这条我看了三遍"
            )
            return
        raw_pos, text = parts[1].strip(), parts[2].strip()
        pos = int(raw_pos) if raw_pos.lstrip("-").isdigit() else 0
        try:
            post = await self._fetch_one(pos)
        except RuntimeError as e:
            yield event.plain_result(str(e))
            return
        if post is None:
            yield event.plain_result("没找到这条说说")
            return

        try:
            await self.service.comment_posts(post, event=event, text=text)
        except Exception as e:  # noqa: BLE001
            yield event.plain_result(f"评论失败：{e}")
            return
        self._open_watch(post)
        await self.sender.send_post(event, post, message=f"已按你的原话评论：{text}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("智能评论")
    async def cmd_smart_comment(self, event: AiocqhttpMessageEvent):
        """智能评论 <序号>：结合上下文自动生成一句评论"""
        parts = event.message_str.split(" ", 1)
        raw_pos = parts[1].strip() if len(parts) > 1 else "0"
        pos = int(raw_pos) if raw_pos.lstrip("-").isdigit() else 0
        try:
            post = await self._fetch_one(pos)
        except RuntimeError as e:
            yield event.plain_result(str(e))
            return
        if post is None:
            yield event.plain_result("没找到这条说说")
            return

        text = await self._gen_smart_comment(post)
        if not text:
            yield event.plain_result("生成评论失败（LLM 没返回内容）")
            return
        try:
            await self.service.comment_posts(post, event=event, text=text)
        except Exception as e:  # noqa: BLE001
            yield event.plain_result(f"评论失败：{e}")
            return
        self._open_watch(post)
        await self.sender.send_post(event, post, message=f"已智能评论：{text}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("楼中楼")
    async def cmd_reply_now(self, event: AiocqhttpMessageEvent):
        """楼中楼：立刻巡检并回复一次"""
        if self.cfg.pro.reply_policy == "off":
            yield event.plain_result(
                "当前策略是「关闭」，先到插件配置 → 增强功能里把策略改掉"
            )
            return
        results = await self.scan_and_reply()
        if results:
            yield event.plain_result("已回复：\n" + "\n".join(results))
        else:
            yield event.plain_result("没有待回复的内容（或还在冷却中）")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("楼中楼状态")
    async def cmd_reply_status(self, event: AiocqhttpMessageEvent):
        """楼中楼状态：查看当前策略、冷却与今日额度"""
        self._roll_daily()
        waited = int(
            max(
                0.0,
                self.cfg.pro.reply_cooldown_min * 60
                - (time.time() - self._last_reply_ts),
            )
        )
        used = self._daily_total
        total = self.cfg.pro.daily_reply_limit
        left = self._daily_left()
        bar_n = 10
        filled = 0 if total <= 0 else round(bar_n * used / total)
        bar = "█" * filled + "░" * (bar_n - filled)

        lines = [
            f"【今日额度】{used}/{total} 条  剩 {left} 条",
            f"{bar} {round(0 if total <= 0 else 100 * used / total)}%",
            f"【策略】{self.cfg.pro.reply_policy}（巡检每 {self.cfg.pro.poll_interval_min} 分钟）",
            f"【冷却】还剩约 {waited // 60} 分 {waited % 60} 秒",
            f"【单轮上限】{self.cfg.pro.max_replies_per_round} 条，"
            f"【每人每天】{self.cfg.pro.per_user_daily_limit} 条，"
            f"【每楼上限】{self.cfg.pro.max_replies_per_thread} 条",
            f"【字数上限】{self.cfg.pro.max_comment_len} 字",
            f"【接话范围】{'也会回自己说说下的评论' if self.cfg.pro.reply_own_post_comments else '只回别人对我的回复'}",
            f"【累计】已回过 {len(self._replied)} 条，盯着 {len(self._watched)} 条说说",
            f"【主人QQ】{self._owner_qq() or '未设置'}",
        ]
        if self._daily_users:
            top = sorted(
                self._daily_users.items(), key=lambda kv: kv[1], reverse=True
            )[:5]
            lines.append("【今天回过的人】" + "、".join(f"{u}×{n}" for u, n in top))
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # LLM 工具：让她能自己决定什么时候用
    # ------------------------------------------------------------------
    _SELF_ALIASES = {
        "me",
        "self",
        "my",
        "mine",
        "我",
        "自己",
        "我的",
        "本人",
        "主人",
        "bot",
        "机器人",
    }

    async def _lookup_friends(self, key: str) -> list[tuple[str, str]]:
        """按 QQ 号或昵称/备注在好友列表里找人，返回 [(uin, 显示名), ...]"""
        client = self._capture_client()
        if client is None:
            return []
        try:
            friends = await client.call_action("get_friend_list")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 拉好友列表失败: {e}")
            return []
        k = key.strip().lower()
        hits: list[tuple[str, str]] = []
        for f in friends or []:
            uin = str(f.get("user_id") or "")
            nick = str(f.get("nickname") or "")
            remark = str(f.get("remark") or "")
            if not uin:
                continue
            if k == uin or (k and (k in nick.lower() or k in remark.lower())):
                hits.append((uin, remark or nick or uin))
        return hits

    async def _resolve_target(self, user_id: str, mine: bool) -> str:
        """解析出目标QQ号；mine=False 且解析不出来时直接抛错，绝不静默回退"""
        if mine:
            self._capture_client()
            return str(await self.session.get_uin())
        u = (user_id or "").strip()
        if not u:
            raise ValueError(
                "参数错误：user_id 不能留空。请先向主人确认要评论哪位好友"
                "（QQ号或昵称），评论主人自己的说说请改用 llm_comment_my_feed"
            )
        if u.lower() in self._SELF_ALIASES:
            self._capture_client()
            return str(await self.session.get_uin())
        if u.isdigit():
            return u
        # 不是纯数字：按昵称/备注去好友列表里找
        hits = await self._lookup_friends(u)
        if len(hits) == 1:
            return hits[0][0]
        if len(hits) > 1:
            names = "、".join(f"{name}({uin})" for uin, name in hits[:5])
            raise ValueError(
                f"「{u}」匹配到多个好友：{names}。请主人确认具体是哪位"
            )
        raise ValueError(
            f"好友列表里没有找到「{u}」。请主人提供准确的QQ号，"
            f"或确认这个昵称是否写对了"
        )

    async def _verify_landed(self, post: Post, my_uin: int, text: str) -> bool:
        """回读目标说说，确认评论真的显示出来了（QQ空间风控会静默吞评论）

        注意：必须用 get_detail 精确回读这一条，不能用 query_feeds(with_detail=False)。
        后者只返回顶层评论预览，不含 list_3（楼中楼），会导致回复明明成功却被误判成"被风控吞掉"。
        """
        try:
            resp = await self.qzone.get_detail(post)
            if not resp.ok or not resp.data:
                return True  # 回读失败/说说没了，不误报
            parsed = QzoneParser.parse_feeds([resp.data])
            if not parsed:
                return True
            target = parsed[0]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 落地校验回读失败（按成功处理）: {e}")
            return True

        want = text.strip()
        for c in target.comments or []:  # build_list 已 flatten 主楼 + 楼中楼
            if c.uin == my_uin and (c.content or "").strip() == want:
                return True
        return False  # 说说在、评论没出现 → 确实被吞

    async def _do_comment(
        self,
        event: AiocqhttpMessageEvent,
        *,
        user_id: str,
        pos: int,
        text: str,
        like: bool,
        mine: bool,
    ) -> str:
        try:
            target = await self._resolve_target(user_id, mine)
            post = await self._fetch_one(pos, target)
            if post is None:
                return "没找到这条说说"

            # 主人指定了"我的动态"，那就必须真的是自己的说说，不是就罢手
            self._capture_client()
            my_uin = int(await self.session.get_uin())
            if mine and post.uin != my_uin:
                return (
                    f"这条说说是 {post.name}（{post.uin}）发的，不是你自己的，"
                    f"已停止评论，请先和主人确认"
                )
            # 反向保险：评论好友却说到了自己头上，也罢手
            if not mine and post.uin == my_uin:
                return (
                    "这条说说其实是你自己发的，不是好友的。"
                    "如果主人要评论主人自己的动态，请改用 llm_comment_my_feed"
                )

            # 主人原话不清洗长度（不擅自改人家的话），AI 生成的才限长
            final_text = self._clean(text or "", 0)
            if not final_text:
                final_text = self._clean(
                    await self._gen_smart_comment(post), self.cfg.pro.max_comment_len
                )
            if not final_text:
                return "生成评论内容失败"

            # 幂等：这条说说下已经有我说过一模一样的话，直接跳过（不花 token 不重复发）
            for c in post.comments or []:
                if c.uin == my_uin and (c.content or "").strip() == final_text:
                    return f"这条说说下刚刚已经说过一模一样的话，没有重复发：{final_text}"

            await self.service.comment_posts(post, event=event, text=final_text)
            self._open_watch(post)

            # 落地校验：QQ空间风控会「返回成功但静默吞评论」，必须回读确认
            await asyncio.sleep(3)
            if not await self._verify_landed(post, my_uin, final_text):
                logger.warning(f"[qzone-promax] 评论疑似被风控吞掉：{final_text}")
                return (
                    f"评论「{final_text}」接口返回成功，但 QQ 空间里没有出现——"
                    "疑似被风控静默拦截（今天评论太频繁或内容重复）。"
                    "请不要重试，告诉主人这个号需要冷静几个小时"
                )

            # 点赞是附加动作，失败不能把已经成功的评论报成失败
            like_note = ""
            if like:
                try:
                    await self.service.like_posts(post)
                except Exception as e:  # noqa: BLE001
                    like_note = "（点赞失败，但评论已发出）"
                    logger.warning(
                        f"[qzone-promax] 点赞失败（评论已成功）: {type(e).__name__}: {e}"
                    )
            liked = "含点赞" if like and not like_note else "未点赞"
            return f"已评论到 {post.name} 的说说（{liked}）{like_note}：{final_text}"
        except Exception as e:  # noqa: BLE001
            logger.exception(f"[qzone-promax] 评论工具执行失败")
            return f"评论失败：{type(e).__name__}: {e}"

    # ------------------------------------------------------------------
    # 发动态
    # ------------------------------------------------------------------
    def _clean_publish_text(self, text: str) -> str:
        """清洗待发布的动态正文：去 HTML/表情码，压缩空白，按配置限长"""
        t = (text or "").strip()
        t = re.sub(r"\{uin:\d+[^}]*\}", "", t)
        t = re.sub(r"\[em\]e?\d+\[/em\]", "", t)
        t = re.sub(r"<[^>]+>", "", t)
        t = re.sub(r"[\s\u3000]+", " ", t).strip()
        t = t.strip("\"'“”`").strip()
        limit = int(self.cfg.publish.max_len or 0)
        if limit and len(t) > limit:
            t = t[:limit].rstrip("，,、；;：: ")
        return t

    async def _verify_published(self, tid: str) -> bool:
        """回读自己最新的几条说说，确认动态真的显示出来了"""
        if not tid:
            return True
        try:
            self._capture_client()
            uin = str(await self.session.get_uin())
            posts = await self.service.query_feeds(
                target_id=uin, pos=0, num=5, with_detail=False
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 动态落地校验回读失败（按成功处理）: {e}")
            return True
        return any(p.tid == tid for p in posts)

    async def _do_publish(
        self,
        event: AiocqhttpMessageEvent,
        *,
        topic: str = "",
        text: str = "",
    ) -> str:
        """发一条动态。text 优先（原样发布），否则按 topic 让 AI 写。"""
        try:
            if self._pub_left() <= 0:
                return (
                    f"今天已经发了 {self._pub_count} 条动态，到上限了"
                    f"（{self.cfg.publish.max_per_day} 条），0 点后自动恢复"
                )
            wait = self._pub_gap_left_sec()
            if wait > 0:
                return (
                    f"两条动态之间要间隔 {self.cfg.publish.gap_min} 分钟，"
                    f"还得等 {int(wait // 60) + 1} 分钟"
                )

            # 主人给了原话就原样发，只做安全清洗，不擅自改写
            final_text = self._clean_publish_text(text)
            if not final_text:
                source = (self.cfg.publish.topic_source or "chat").strip()
                style = (self.cfg.publish.style_prompt or "").strip()
                # 把当前对话拉进来当素材——动态基于真实发生的事，不凭空编
                chat_ctx = await self._chat_contexts(event)
                if event is not None or source in ("topic", "news"):
                    # 手动触发 / 自主选题 / 新闻：自由生成，带对话素材
                    default_topic = (
                        "从今天刷到的科技/AI新闻里挑一件你觉得最有趣的，说一句你的看法"
                        if source == "news"
                        else "基于你和主人最近的聊天挑一件真实的事写；"
                        "实在没有，就写一句此刻的日常心情"
                    )
                    final_text = self._clean_publish_text(
                        await self.service.llm.generate_free_post(
                            topic=topic.strip() or default_topic,
                            style_prompt=style,
                            event=event,
                            chat_contexts=chat_ctx,
                        )
                    )
                else:
                    # 定时任务（无对话上下文）：沿用群聊取材，失败兜底为自由生成
                    try:
                        final_text = self._clean_publish_text(
                            await self.service.llm.generate_post(
                                topic=topic.strip() or None, event=event
                            )
                        )
                    except Exception as e:  # noqa: BLE001
                        logger.warning(
                            f"[qzone-promax] 聊天记录取材失败，改用自主选题: {e}"
                        )
                        final_text = ""
                    if not final_text:
                        final_text = self._clean_publish_text(
                            await self.service.llm.generate_free_post(
                                topic=topic.strip()
                                or "随便挑一件你此刻想说的日常小事",
                                style_prompt=style,
                                event=event,
                            )
                        )
            if not final_text:
                return "生成动态内容失败（LLM 没返回内容）"

            post = await self.service.publish_post(text=final_text)
            self._consume_pub_quota()

            # 自己刚发的动态：立刻开巡检窗口，接下来 N 天盯它下面的评论
            try:
                if not int(getattr(post, "uin", 0) or 0):
                    post.uin = int(await self.session.get_uin() or 0)
                self._open_watch(post)
            except Exception as e:  # noqa: BLE001
                logger.debug(f"[qzone-promax] 发布后开启巡检窗口失败: {e}")

            # 落地校验：发布同样可能被风控静默吞掉
            await asyncio.sleep(3)
            if not await self._verify_published(post.tid):
                logger.warning(f"[qzone-promax] 动态疑似被风控吞掉：{final_text}")
                return (
                    f"动态「{final_text}」接口返回成功，但空间里没出现——"
                    "疑似被风控拦截。请别重试，告诉主人这个号要冷静一下"
                )

            # 把已发内容写回会话历史，主人问起时她知道自己发了什么
            await self._note_publish_to_chat(event, final_text)
            return f"已发到空间啦：{final_text}"
        except Exception as e:  # noqa: BLE001
            logger.exception("[qzone-promax] 发动态失败")
            return f"发动态失败：{type(e).__name__}: {e}"

    async def _chat_contexts(self, event, limit: int = 12) -> list[dict]:
        """拉当前会话最近几条消息，给发动态当真实素材（防止凭空编造情节）"""
        if event is None:
            return []
        try:
            cm = getattr(self.context, "conversation_manager", None)
            if cm is None:
                return []
            umo = str(event.unified_msg_origin)
            cid = await cm.get_curr_conversation_id(umo)
            if not cid:
                return []
            conv = await cm.get_conversation(umo, cid)
            if not conv:
                return []
            history = json.loads(conv.history or "[]")
            return [
                {"role": m.get("role"), "content": m.get("content")}
                for m in history[-limit:]
                if m.get("role") in ("user", "assistant") and m.get("content")
            ]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 拉会话历史失败: {e}")
            return []

    async def _note_publish_to_chat(self, event, text: str) -> None:
        """把已发动态写进当前会话历史，免得主人问起时她一脸懵"""
        if event is None:
            return
        try:
            cm = getattr(self.context, "conversation_manager", None)
            if cm is None:
                return
            umo = str(event.unified_msg_origin)
            cid = await cm.get_curr_conversation_id(umo)
            if not cid:
                return
            conv = await cm.get_conversation(umo, cid)
            if not conv:
                return
            history = json.loads(conv.history or "[]")
            history.append(
                {
                    "role": "assistant",
                    "content": (
                        "（我刚刚在QQ空间发布了一条动态，正文是："
                        f"{text}。主人问起的话我知道这件事。）"
                    ),
                }
            )
            await cm.update_conversation(umo, conversation_id=cid, history=history)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[qzone-promax] 写回会话历史失败: {e}")

    @filter.llm_tool()
    async def llm_comment_feed(
        self,
        event: AiocqhttpMessageEvent,
        user_id: str = "",
        pos: int = 0,
        text: str = "",
        like: bool = False,
    ):
        """
        评论【好友】的QQ空间说说。

        注意：主人说的如果是"我的动态""我空间里那条""发到我动态下面"，
        绝对不要用本工具，请改用 llm_comment_my_feed！

        Args:
            user_id(string): 好友的QQ号【或好友昵称/备注】。填QQ号最稳；填昵称也行（会自动去好友列表匹配）。绝对不能留空
            pos(number): 这位好友的说说序号，0 表示最新一条
            text(string): 要评论的具体内容。主人指定了说什么就原样传入，没指定才留空由AI自己写
            like(boolean): 是否顺便点赞
        """
        return await self._do_comment(
            event, user_id=user_id, pos=pos, text=text, like=like, mine=False
        )

    @filter.llm_tool()
    async def llm_comment_my_feed(
        self,
        event: AiocqhttpMessageEvent,
        pos: int = 0,
        text: str = "",
        like: bool = False,
    ):
        """
        评论【你自己】发在QQ空间里的说说。

        主人说"我的动态""我的说说""发到我空间""发到自己动态下面"时，用这个工具。
        主人评论好友的说说时不要用这个。

        Args:
            pos(number): 你自己的说说序号，0 表示最新一条
            text(string): 要评论的具体内容。主人指定了说什么就原样传入，没指定才留空由AI自己写
            like(boolean): 是否顺便点赞
        """
        return await self._do_comment(
            event, user_id="", pos=pos, text=text, like=like, mine=True
        )

    @filter.llm_tool()
    async def llm_reply_pending(self, event: AiocqhttpMessageEvent):
        """
        检查QQ空间里有没有人回复了你的评论，并自动生成楼中楼回复。

        当你想处理"别人回复我"、或者主人问起有没有新回复的时候调用。
        """
        if self.cfg.pro.reply_policy == "off":
            return "楼中楼自动回复当前是关闭状态，请先到插件配置里开启"
        results = await self.scan_and_reply()
        return "\n".join(results) if results else "暂时没有需要回复的内容"

    # 主人自然语言的发布意图。LLM 在角色扮演语境下经常"口头发了"却不调工具，
    # 所以这里在代码层直接拦：命中就真发，发完终止事件，不给她糊弄的机会。
    _PUBLISH_INTENT = re.compile(
        r"(发(一?[个条篇])?\s*(动态|说说|空间)"
        r"|更新(一?[下个条])?\s*(动态|说说|空间)"
        r"|(动态|说说|空间)\s*(发|更新)(一?[个条下])?)"
    )
    _NEGATIVE = ("没发", "没看到", "看不到", "骗", "撒谎", "假的", "为什么", "咋", "怎么")
    _REDO = ("再发", "重新发", "重发", "再来一条", "重新来")
    _OWN_CMDS = (
        "发说说", "发原话", "删说说", "动态配置",
        "评论说说", "智能评论", "楼中楼",
    )

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_publish_intent(self, event: AiocqhttpMessageEvent):
        """主人用自然语言说要发动态时，直接在代码层拦下来真发，不依赖 LLM 自觉调工具"""
        if self._pending_publish:  # 有草稿等确认，交给 on_confirm_publish 处理
            return
        if str(event.get_sender_id()) != str(self._owner_qq()):
            return

        text = (event.message_str or "").strip()
        if not text:
            return
        if any(text.startswith(c) for c in self._OWN_CMDS):
            return  # 走各自命令，别抢
        # "再发一个"这种把"动态"俩字省掉的也算明确指令
        redo = any(w in text for w in self._REDO)
        if not redo and not self._PUBLISH_INTENT.search(text):
            return
        # "你还是没发呀" 这类抱怨不能被当成新指令
        if not redo and any(w in text for w in self._NEGATIVE):
            return

        logger.info(f"[qzone-promax] 捕获到主人发布意图：{text[:30]}")
        # 把「发个动态」这类指令词从主题里剔掉，剩下的才是话题
        topic = self._PUBLISH_INTENT.sub("", text)
        for w in self._REDO:
            topic = topic.replace(w, "")
        topic = topic.strip(" ，。,.！!？?~～啊吧呢啦呀哈")
        result = await self._do_publish(event, topic=topic)
        yield event.plain_result(result)
        event.stop_event()  # 结果已经说清楚了，别再让 LLM 补一句"我发啦"

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_confirm_publish(self, event: AiocqhttpMessageEvent):
        """主人回复「发」「发了」「发布」时，把待审核的动态发出去"""
        if not self._pending_publish:
            return
        if str(event.get_sender_id()) != str(self._owner_qq()):
            return
        text = (event.message_str or "").strip()
        if text not in ("发", "发了", "发布", "发吧", "发吧。"):
            return
        draft = self._pending_publish
        self._pending_publish = ""
        result = await self._do_publish(event, text=draft)
        yield event.plain_result(result)

    @filter.llm_tool()
    async def llm_publish_feed(
        self,
        event: AiocqhttpMessageEvent,
        text: str = "",
        topic: str = "",
    ):
        """
        写一篇说说并发布到QQ空间

        Args:
            text(string): 要发布的说说内容
            topic(string): 内容主题，text 留空时按它来写
        """
        return await self._do_publish(event, topic=topic, text=text)
