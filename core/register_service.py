import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from core.account import load_accounts_from_source
from core.base_task_service import BaseTask, BaseTaskService, TaskCancelledError, TaskStatus
from core.config import config
from core.duckmail_client import DuckMailClient
from core.gemini_automation import GeminiAutomation
from core.gemini_automation_uc import GeminiAutomationUC

logger = logging.getLogger("gemini.register")


@dataclass
class RegisterTask(BaseTask):
    """注册任务数据类"""
    count: int = 0
    domain: Optional[str] = None

    def to_dict(self) -> dict:
        """转换为字典"""
        base_dict = super().to_dict()
        base_dict["count"] = self.count
        base_dict["domain"] = self.domain
        return base_dict


class RegisterService(BaseTaskService[RegisterTask]):
    """注册服务类"""

    def __init__(
        self,
        multi_account_mgr,
        http_client,
        user_agent: str,
        account_failure_threshold: int,
        rate_limit_cooldown_seconds: int,
        session_cache_ttl_seconds: int,
        global_stats_provider: Callable[[], dict],
        set_multi_account_mgr: Optional[Callable[[Any], None]] = None,
    ) -> None:
        super().__init__(
            multi_account_mgr,
            http_client,
            user_agent,
            account_failure_threshold,
            rate_limit_cooldown_seconds,
            session_cache_ttl_seconds,
            global_stats_provider,
            set_multi_account_mgr,
            log_prefix="REGISTER",
        )

    async def start_register(self, count: Optional[int] = None, domain: Optional[str] = None) -> RegisterTask:
        """启动注册任务（支持排队）。"""
        async with self._lock:
            if os.environ.get("ACCOUNTS_CONFIG"):
                raise ValueError("ACCOUNTS_CONFIG is set; register is disabled")

            domain_value = (domain or "").strip()
            if not domain_value:
                domain_value = (config.basic.register_domain or "").strip() or None

            register_count = count or config.basic.register_default_count
            register_count = max(1, min(30, int(register_count)))
            task = RegisterTask(id=str(uuid.uuid4()), count=register_count, domain=domain_value)
            self._tasks[task.id] = task
            # 将 domain 记录在日志里，便于排查
            self._append_log(task, "info", f"register task queued (count={register_count}, domain={domain_value or 'default'})")
            await self._enqueue_task(task)
            return task

    def _execute_task(self, task: RegisterTask):
        return self._run_register_async(task, task.domain)

    async def _run_register_async(self, task: RegisterTask, domain: Optional[str]) -> None:
        """异步执行注册任务（支持取消）。"""
        loop = asyncio.get_running_loop()
        self._append_log(task, "info", "register task started")

        for _ in range(task.count):
            if task.cancel_requested:
                self._append_log(task, "warning", f"register task cancelled: {task.cancel_reason or 'cancelled'}")
                task.status = TaskStatus.CANCELLED
                task.finished_at = time.time()
                return
            try:
                result = await loop.run_in_executor(self._executor, self._register_one, domain, task)
            except TaskCancelledError:
                task.status = TaskStatus.CANCELLED
                task.finished_at = time.time()
                return
            except Exception as exc:
                result = {"success": False, "error": str(exc)}
            task.progress += 1
            task.results.append(result)

            if result.get("success"):
                task.success_count += 1
                self._append_log(task, "info", f"register success: {result.get('email')}")
            else:
                task.fail_count += 1
                self._append_log(task, "error", f"register failed: {result.get('error')}")

        if task.cancel_requested:
            task.status = TaskStatus.CANCELLED
        else:
            task.status = TaskStatus.SUCCESS if task.fail_count == 0 else TaskStatus.FAILED
        task.finished_at = time.time()
        self._append_log(task, "info", f"register task finished ({task.success_count}/{task.count})")

    def _register_one(self, domain: Optional[str], task: RegisterTask) -> dict:
        """注册单个账户"""
        def log_cb(level, message):
            self._append_log(task, level, message)
        client = DuckMailClient(
            base_url=config.basic.duckmail_base_url,
            proxy=config.basic.proxy,
            verify_ssl=config.basic.duckmail_verify_ssl,
            api_key=config.basic.duckmail_api_key,
            log_callback=log_cb,
        )
        if not client.register_account(domain=domain):
            return {"success": False, "error": "duckmail register failed"}

        # 根据配置选择浏览器引擎
        browser_engine = (config.basic.browser_engine or "dp").lower()
        headless = config.basic.browser_headless

        # Linux 环境强制使用 DP 无头模式（无图形界面无法运行有头模式）
        import sys
        is_linux = sys.platform.startswith("linux")
        if is_linux:
            if browser_engine != "dp" or not headless:
                log_cb("warning", "Linux environment: forcing DP engine with headless mode")
                browser_engine = "dp"
                headless = True

        if browser_engine == "dp":
            # DrissionPage 引擎：支持有头和无头模式
            automation = GeminiAutomation(
                user_agent=self.user_agent,
                proxy=config.basic.proxy,
                headless=headless,
                log_callback=log_cb,
            )
        else:
            # undetected-chromedriver 引擎：仅有头模式可用
            automation = GeminiAutomationUC(
                user_agent=self.user_agent,
                proxy=config.basic.proxy,
                headless=headless,
                log_callback=log_cb,
            )
        # 允许外部取消时立刻关闭浏览器
        self._add_cancel_hook(task.id, lambda: getattr(automation, "stop", lambda: None)())

        try:
            result = automation.login_and_extract(client.email, client)
        except Exception as exc:
            return {"success": False, "error": str(exc)}
        if not result.get("success"):
            return {"success": False, "error": result.get("error", "automation failed")}

        config_data = result["config"]
        config_data["mail_provider"] = "duckmail"
        config_data["mail_address"] = client.email
        config_data["mail_password"] = client.password

        accounts_data = load_accounts_from_source()
        updated = False
        for acc in accounts_data:
            if acc.get("id") == config_data["id"]:
                acc.update(config_data)
                updated = True
                break
        if not updated:
            accounts_data.append(config_data)

        self._apply_accounts_update(accounts_data)

        return {"success": True, "email": client.email, "config": config_data}
