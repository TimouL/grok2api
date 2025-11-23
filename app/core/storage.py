"""存储抽象层 - 支持文件、Git、MySQL和Redis存储"""

import os
import orjson
import toml
import asyncio
import warnings
import aiofiles
import subprocess
import urllib.parse
import os
import shutil
from typing import Tuple
from pathlib import Path
from typing import Dict, Any, Optional, Literal
from abc import ABC, abstractmethod
from urllib.parse import urlparse, unquote

from app.core.logger import logger


StorageMode = Literal["file", "mysql", "redis"]


class BaseStorage(ABC):
    """存储基类"""

    @abstractmethod
    async def init_db(self) -> None:
        """初始化数据库"""
        pass

    @abstractmethod
    async def load_tokens(self) -> Dict[str, Any]:
        """加载token数据"""
        pass

    @abstractmethod
    async def save_tokens(self, data: Dict[str, Any]) -> None:
        """保存token数据"""
        pass

    @abstractmethod
    async def load_config(self) -> Dict[str, Any]:
        """加载配置数据"""
        pass

    @abstractmethod
    async def save_config(self, data: Dict[str, Any]) -> None:
        """保存配置数据"""
        pass


class FileStorage(BaseStorage):
    """文件存储"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.token_file = data_dir / "token.json"
        self.config_file = data_dir / "setting.toml"
        self._token_lock = asyncio.Lock()
        self._config_lock = asyncio.Lock()

    async def init_db(self) -> None:
        """初始化文件存储"""
        self.data_dir.mkdir(parents=True, exist_ok=True)

        if not self.token_file.exists():
            await self._write(self.token_file, orjson.dumps({"sso": {}, "ssoSuper": {}}, option=orjson.OPT_INDENT_2).decode())
            logger.info("[Storage] 创建token文件")

        if not self.config_file.exists():
            default = {
                "global": {"api_keys": [], "admin_username": "admin", "admin_password": "admin"},
                "grok": {"proxy_url": "", "cf_clearance": "", "x_statsig_id": ""}
            }
            await self._write(self.config_file, toml.dumps(default))
            logger.info("[Storage] 创建配置文件")

    async def _read(self, path: Path) -> str:
        """读取文件"""
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            return await f.read()

    async def _write(self, path: Path, content: str) -> None:
        """写入文件"""
        async with aiofiles.open(path, "w", encoding="utf-8") as f:
            await f.write(content)

    async def _load_json(self, path: Path, default: Dict, lock: asyncio.Lock) -> Dict[str, Any]:
        """加载JSON"""
        try:
            async with lock:
                if not path.exists():
                    return default
                return orjson.loads(await self._read(path))
        except Exception as e:
            logger.error(f"[Storage] 加载{path.name}失败: {e}")
            return default

    async def _save_json(self, path: Path, data: Dict, lock: asyncio.Lock) -> None:
        """保存JSON"""
        try:
            async with lock:
                await self._write(path, orjson.dumps(data, option=orjson.OPT_INDENT_2).decode())
        except Exception as e:
            logger.error(f"[Storage] 保存{path.name}失败: {e}")
            raise

    async def _load_toml(self, path: Path, default: Dict, lock: asyncio.Lock) -> Dict[str, Any]:
        """加载TOML"""
        try:
            async with lock:
                if not path.exists():
                    return default
                return toml.loads(await self._read(path))
        except Exception as e:
            logger.error(f"[Storage] 加载{path.name}失败: {e}")
            return default

    async def _save_toml(self, path: Path, data: Dict, lock: asyncio.Lock) -> None:
        """保存TOML"""
        try:
            async with lock:
                await self._write(path, toml.dumps(data))
        except Exception as e:
            logger.error(f"[Storage] 保存{path.name}失败: {e}")
            raise

    async def load_tokens(self) -> Dict[str, Any]:
        """加载token"""
        return await self._load_json(self.token_file, {"sso": {}, "ssoSuper": {}}, self._token_lock)

    async def save_tokens(self, data: Dict[str, Any]) -> None:
        """保存token"""
        await self._save_json(self.token_file, data, self._token_lock)

    async def load_config(self) -> Dict[str, Any]:
        """加载配置"""
        return await self._load_toml(self.config_file, {"global": {}, "grok": {}}, self._config_lock)

    async def save_config(self, data: Dict[str, Any]) -> None:
        """保存配置"""
        await self._save_toml(self.config_file, data, self._config_lock)


class GitStoreError(Exception):
    """GitStore 基础异常"""


class GitStoreConfigError(GitStoreError):
    """配置缺失或无效"""


class GitStoreAuthError(GitStoreError):
    """鉴权失败"""


class GitStoreRepoNotFoundError(GitStoreError):
    """仓库不存在"""


class GitStoreStorage(FileStorage):
    """基于 Git 仓库的文件存储，强制单提交历史"""

    def __init__(self, repo_url: str, username: str, token: str, data_dir: Path, branch: str = "main"):
        super().__init__(data_dir)
        self.repo_url = repo_url
        self.username = username
        self.token = token
        self.branch = branch
        self._validated = False
        self._git_lock = asyncio.Lock()
        self._askpass_path: Path | None = None
        self._mirror_dir = Path(__file__).parents[2] / "data"
        self._mirror_token_lock = asyncio.Lock()
        self._mirror_config_lock = asyncio.Lock()
        self._state: Literal["active", "degraded"] = "active"
        self._pending_sync: bool = False
        self._last_error: str | None = None
        self._retry_task: Optional[asyncio.Task] = None

    # === Git helpers ===
    def _remote_with_auth(self) -> str:
        parsed = urllib.parse.urlparse(self.repo_url)
        if not parsed.scheme or not parsed.netloc:
            raise GitStoreConfigError("无效的 GITSTORE_GIT_URL")

        safe_user = urllib.parse.quote(self.username, safe="")
        netloc = f"{safe_user}@{parsed.netloc}"
        rebuilt = parsed._replace(netloc=netloc)
        return urllib.parse.urlunparse(rebuilt)

    def _ensure_askpass(self) -> Path:
        if self._askpass_path and self._askpass_path.exists():
            return self._askpass_path

        self.data_dir.mkdir(parents=True, exist_ok=True)
        path = self.data_dir / ".git-askpass.sh"
        script = """#!/bin/sh
case "$1" in
  *'Username for'* ) echo '{username}' ;;
  *'Password for'* ) echo '{token}' ;;
  *'password'* ) echo '{token}' ;;
  * ) echo '{token}' ;;
esac
""".format(username=self.username.replace("'", "\\'"), token=self.token.replace("'", "\\'"))
        path.write_text(script, encoding="utf-8")
        path.chmod(0o700)
        self._askpass_path = path
        return path

    def _git_env(self) -> dict:
        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_ASKPASS"] = str(self._ensure_askpass())
        return env

    def _bootstrap_from_mirror_if_absent(self) -> None:
        """当远端缺少文件时，用本地 data 下已有文件初始化 gitstore 目录。"""
        mirror_setting = self._mirror_dir / "setting.toml"
        mirror_token = self._mirror_dir / "token.json"
        git_setting = self.data_dir / "setting.toml"
        git_token = self.data_dir / "token.json"

        if mirror_setting.exists() and not git_setting.exists():
            git_setting.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(mirror_setting, git_setting)

        if mirror_token.exists() and not git_token.exists():
            git_token.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(mirror_token, git_token)

    def _set_active(self):
        self._state = "active"
        self._pending_sync = False
        self._last_error = None

    def _set_degraded(self, reason: str):
        self._state = "degraded"
        self._pending_sync = True
        self._last_error = reason
        logger.warning(f"[GitStore] 已降级为本地缓存: {reason}")

    async def _mirror_tokens(self, data: Dict[str, Any]) -> None:
        path = self._mirror_dir / "token.json"
        self._mirror_dir.mkdir(parents=True, exist_ok=True)
        async with self._mirror_token_lock:
            async with aiofiles.open(path, "w", encoding="utf-8") as f:
                await f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2).decode())

    async def _mirror_config(self, data: Dict[str, Any]) -> None:
        path = self._mirror_dir / "setting.toml"
        self._mirror_dir.mkdir(parents=True, exist_ok=True)
        async with self._mirror_config_lock:
            async with aiofiles.open(path, "w", encoding="utf-8") as f:
                await f.write(toml.dumps(data))

    def _run_git(self, args: list[str], allow_fail: bool = False) -> subprocess.CompletedProcess:
        cmd = ["git", *args]
        result = subprocess.run(cmd, cwd=self.data_dir, capture_output=True, text=True, env=self._git_env())
        if result.returncode != 0 and not allow_fail:
            stderr = result.stderr.lower()
            if "authentication" in stderr or "auth" in stderr or "permission denied" in stderr:
                raise GitStoreAuthError(result.stderr.strip())
            if "repository not found" in stderr:
                raise GitStoreRepoNotFoundError(result.stderr.strip())
            raise GitStoreError(result.stderr.strip())
        return result

    def _ensure_repo(self) -> None:
        if not self.data_dir.exists():
            self.data_dir.mkdir(parents=True, exist_ok=True)

        git_dir = self.data_dir / ".git"
        remote = self._remote_with_auth()

        if not git_dir.exists():
            clone_cmd = ["clone", "--no-checkout", remote, str(self.data_dir)]
            result = subprocess.run(["git", *clone_cmd], capture_output=True, text=True, env=self._git_env())
            if result.returncode != 0:
                stderr = result.stderr.lower()
                if "already exists and is not an empty directory" in stderr:
                    # 目录非空，无.git，使用 init+fetch 模式
                    self._run_git(["init"])
                    self._run_git(["remote", "add", "origin", remote], allow_fail=True)
                    if self._has_remote_branch():
                        self._run_git(["fetch", "origin", self.branch])
                        self._run_git(["checkout", "-B", self.branch, f"origin/{self.branch}"])
                        self._run_git(["reset", "--hard", f"origin/{self.branch}"], allow_fail=True)
                        self._run_git(["clean", "-fdx"], allow_fail=True)
                    else:
                        self._run_git(["checkout", "-B", self.branch], allow_fail=True)
                elif "repository not found" in stderr:
                    raise GitStoreRepoNotFoundError(result.stderr.strip())
                elif "authentication" in stderr or "permission denied" in stderr:
                    raise GitStoreAuthError(result.stderr.strip())
                else:
                    raise GitStoreError(result.stderr.strip())

        self._ensure_askpass()
        self._run_git(["remote", "set-url", "origin", remote], allow_fail=True)
        self._run_git(["config", "user.name", self.username], allow_fail=True)
        self._run_git(["config", "user.email", f"{self.username}@users.noreply.github.com"], allow_fail=True)

    def _has_remote_branch(self) -> bool:
        result = subprocess.run(
            ["git", "ls-remote", "--heads", "origin", self.branch],
            cwd=self.data_dir,
            capture_output=True,
            text=True,
            env=self._git_env(),
        )
        return result.returncode == 0 and result.stdout.strip() != ""

    def _has_head(self) -> bool:
        result = subprocess.run(["git", "rev-parse", "--verify", "HEAD"], cwd=self.data_dir, env=self._git_env())
        return result.returncode == 0

    def _sync_remote(self) -> None:
        has_branch = self._has_remote_branch()
        if has_branch:
            self._run_git(["fetch", "origin", self.branch])
            self._run_git(["checkout", "-B", self.branch, f"origin/{self.branch}"])
        else:
            self._run_git(["checkout", "-B", self.branch], allow_fail=True)

    async def _commit_and_push(self, message: str) -> None:
        attempts = 3
        delay = 3

        # 确保文件存在
        (self.data_dir / "setting.toml").touch(exist_ok=True)
        (self.data_dir / "token.json").touch(exist_ok=True)

        for i in range(attempts):
            try:
                self._sync_remote()
                self._run_git(["add", "setting.toml", "token.json"])

                diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=self.data_dir, env=self._git_env())
                if diff.returncode == 0:
                    self._set_active()
                    return

                if self._has_head():
                    self._run_git(["commit", "--amend", "-m", message])
                else:
                    self._run_git(["commit", "-m", message])

                self._run_git(["push", "origin", f"HEAD:{self.branch}", "--force"])
                self._set_active()
                return
            except (GitStoreAuthError, GitStoreRepoNotFoundError, GitStoreError) as e:
                reason = str(e)
                if i < attempts - 1:
                    await asyncio.sleep(delay)
                    continue
                self._set_degraded(reason)
                return

    async def _retry_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            if self._state == "degraded" and self._pending_sync:
                try:
                    await self._commit_and_push("retry pending sync")
                except Exception:
                    # 已在 _commit_and_push 处理降级状态
                    continue

    async def init_db(self) -> None:
        async with self._git_lock:
            self._ensure_repo()
            self._sync_remote()

            # 如果远端/工作区缺少文件，尝试用本地 data 现有文件引导
            self._bootstrap_from_mirror_if_absent()

            await super().init_db()

            # 初始化后同步一份到本地 data 目录，便于回退时可用
            await self._mirror_tokens(await super().load_tokens())
            await self._mirror_config(await super().load_config())

            # 首次或远端缺少文件时也会提交（若无变更则自动跳过）
            await self._commit_and_push("init gitstore data")

            self._validated = True
            if not self._retry_task:
                self._retry_task = asyncio.create_task(self._retry_loop())
            logger.info("[GitStore] 已启用 GitHub 持久化存储")

    async def load_tokens(self) -> Dict[str, Any]:
        async with self._git_lock:
            self._sync_remote()
            return await super().load_tokens()

    async def load_config(self) -> Dict[str, Any]:
        async with self._git_lock:
            self._sync_remote()
            return await super().load_config()

    async def save_tokens(self, data: Dict[str, Any]) -> None:
        async with self._git_lock:
            self._sync_remote()
            await super().save_tokens(data)
            await self._mirror_tokens(data)
            await self._commit_and_push("update tokens")

    async def save_config(self, data: Dict[str, Any]) -> None:
        async with self._git_lock:
            self._sync_remote()
            await super().save_config(data)
            await self._mirror_config(data)
            await self._commit_and_push("update settings")

    def get_status(self) -> Tuple[str, bool, Optional[str]]:
        return self._state, self._pending_sync, self._last_error


class MysqlStorage(BaseStorage):
    """MySQL存储"""

    def __init__(self, database_url: str, data_dir: Path):
        self.database_url = database_url
        self.data_dir = data_dir
        self._pool = None
        self._file = FileStorage(data_dir)

    async def init_db(self) -> None:
        """初始化MySQL"""
        try:
            import aiomysql
            parsed = self._parse_url(self.database_url)
            logger.info(f"[Storage] MySQL: {parsed['user']}@{parsed['host']}:{parsed['port']}/{parsed['db']}")

            await self._create_db(parsed)
            self._pool = await aiomysql.create_pool(
                host=parsed['host'], port=parsed['port'], user=parsed['user'],
                password=parsed['password'], db=parsed['db'], charset="utf8mb4",
                autocommit=True, maxsize=10
            )
            await self._create_tables()
            await self._file.init_db()
            await self._sync_data()

        except ImportError:
            raise Exception("aiomysql未安装")
        except Exception as e:
            logger.error(f"[Storage] MySQL初始化失败: {e}")
            raise

    def _parse_url(self, url: str) -> Dict[str, Any]:
        """解析URL"""
        p = urlparse(url)
        return {
            'user': unquote(p.username) if p.username else "",
            'password': unquote(p.password) if p.password else "",
            'host': p.hostname,
            'port': p.port or 3306,
            'db': p.path[1:] if p.path else "grok2api"
        }

    async def _create_db(self, parsed: Dict) -> None:
        """创建数据库"""
        import aiomysql
        pool = await aiomysql.create_pool(
            host=parsed['host'], port=parsed['port'], user=parsed['user'],
            password=parsed['password'], charset="utf8mb4", autocommit=True, maxsize=1
        )

        try:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    with warnings.catch_warnings():
                        warnings.filterwarnings('ignore', message='.*database exists')
                        await cursor.execute(
                            f"CREATE DATABASE IF NOT EXISTS `{parsed['db']}` "
                            f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                        )
                    logger.info(f"[Storage] 数据库 '{parsed['db']}' 就绪")
        finally:
            pool.close()
            await pool.wait_closed()

    async def _create_tables(self) -> None:
        """创建表"""
        tables = {
            "grok_tokens": """
                CREATE TABLE IF NOT EXISTS grok_tokens (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data JSON NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            "grok_settings": """
                CREATE TABLE IF NOT EXISTS grok_settings (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data JSON NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        }

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                with warnings.catch_warnings():
                    warnings.filterwarnings('ignore', message='.*already exists')
                    for sql in tables.values():
                        await cursor.execute(sql)
                logger.info("[Storage] MySQL表就绪")

    async def _sync_data(self) -> None:
        """同步数据"""
        try:
            for table, key in [("grok_tokens", "sso"), ("grok_settings", "global")]:
                data = await self._load_db(table)
                if data:
                    if table == "grok_tokens":
                        await self._file.save_tokens(data)
                    else:
                        await self._file.save_config(data)
                    logger.info(f"[Storage] {table.split('_')[1]}数据已从DB同步")
                else:
                    file_data = await (self._file.load_tokens() if table == "grok_tokens" else self._file.load_config())
                    if file_data.get(key) or (table == "grok_tokens" and file_data.get("ssoSuper")):
                        await self._save_db(table, file_data)
                        logger.info(f"[Storage] {table.split('_')[1]}数据已初始化到DB")
        except Exception as e:
            logger.warning(f"[Storage] 同步失败: {e}")

    async def _load_db(self, table: str) -> Optional[Dict]:
        """从DB加载"""
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(f"SELECT data FROM {table} ORDER BY id DESC LIMIT 1")
                    result = await cursor.fetchone()
                    return orjson.loads(result[0]) if result else None
        except Exception as e:
            logger.error(f"[Storage] 加载{table}失败: {e}")
            return None

    async def _save_db(self, table: str, data: Dict) -> None:
        """保存到DB"""
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    json_data = orjson.dumps(data).decode()
                    await cursor.execute(f"SELECT id FROM {table} ORDER BY id DESC LIMIT 1")
                    result = await cursor.fetchone()

                    if result:
                        await cursor.execute(f"UPDATE {table} SET data = %s WHERE id = %s", (json_data, result[0]))
                    else:
                        await cursor.execute(f"INSERT INTO {table} (data) VALUES (%s)", (json_data,))
        except Exception as e:
            logger.error(f"[Storage] 保存{table}失败: {e}")
            raise

    async def load_tokens(self) -> Dict[str, Any]:
        """加载token"""
        return await self._file.load_tokens()

    async def save_tokens(self, data: Dict[str, Any]) -> None:
        """保存token"""
        await self._file.save_tokens(data)
        await self._save_db("grok_tokens", data)

    async def load_config(self) -> Dict[str, Any]:
        """加载配置"""
        return await self._file.load_config()

    async def save_config(self, data: Dict[str, Any]) -> None:
        """保存配置"""
        await self._file.save_config(data)
        await self._save_db("grok_settings", data)

    async def close(self) -> None:
        """关闭连接"""
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            logger.info("[Storage] MySQL已关闭")


class RedisStorage(BaseStorage):
    """Redis存储"""

    def __init__(self, redis_url: str, data_dir: Path):
        self.redis_url = redis_url
        self.data_dir = data_dir
        self._redis = None
        self._file = FileStorage(data_dir)

    async def init_db(self) -> None:
        """初始化Redis"""
        try:
            import redis.asyncio as aioredis
            parsed = urlparse(self.redis_url)
            db = int(parsed.path.lstrip('/')) if parsed.path and parsed.path != '/' else 0
            logger.info(f"[Storage] Redis: {parsed.hostname}:{parsed.port or 6379}/{db}")

            self._redis = aioredis.Redis.from_url(
                self.redis_url, encoding="utf-8", decode_responses=True
            )

            await self._redis.ping()
            logger.info(f"[Storage] Redis连接成功")

            await self._file.init_db()
            await self._sync_data()

        except ImportError:
            raise Exception("redis未安装")
        except Exception as e:
            logger.error(f"[Storage] Redis初始化失败: {e}")
            raise

    async def _sync_data(self) -> None:
        """同步数据"""
        try:
            for key, file_func, key_name in [
                ("grok:tokens", self._file.load_tokens, "sso"),
                ("grok:settings", self._file.load_config, "global")
            ]:
                data = await self._redis.get(key)
                if data:
                    parsed = orjson.loads(data)
                    if key == "grok:tokens":
                        await self._file.save_tokens(parsed)
                    else:
                        await self._file.save_config(parsed)
                    logger.info(f"[Storage] {key.split(':')[1]}数据已从Redis同步")
                else:
                    file_data = await file_func()
                    if file_data.get(key_name) or (key == "grok:tokens" and file_data.get("ssoSuper")):
                        await self._redis.set(key, orjson.dumps(file_data).decode())
                        logger.info(f"[Storage] {key.split(':')[1]}数据已初始化到Redis")
        except Exception as e:
            logger.warning(f"[Storage] 同步失败: {e}")

    async def _save_redis(self, key: str, data: Dict) -> None:
        """保存到Redis"""
        try:
            await self._redis.set(key, orjson.dumps(data).decode())
        except Exception as e:
            logger.error(f"[Storage] 保存Redis失败: {e}")
            raise

    async def load_tokens(self) -> Dict[str, Any]:
        """加载token"""
        return await self._file.load_tokens()

    async def save_tokens(self, data: Dict[str, Any]) -> None:
        """保存token"""
        await self._file.save_tokens(data)
        await self._save_redis("grok:tokens", data)

    async def load_config(self) -> Dict[str, Any]:
        """加载配置"""
        return await self._file.load_config()

    async def save_config(self, data: Dict[str, Any]) -> None:
        """保存配置"""
        await self._file.save_config(data)
        await self._save_redis("grok:settings", data)

    async def close(self) -> None:
        """关闭连接"""
        if self._redis:
            await self._redis.close()
            logger.info("[Storage] Redis已关闭")


class StorageManager:
    """存储管理器（单例）"""

    _instance: Optional['StorageManager'] = None
    _storage: Optional[BaseStorage] = None
    _initialized: bool = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def init(self) -> None:
        """初始化存储"""
        if self._initialized:
            return

        mode = os.getenv("STORAGE_MODE", "file").lower()
        url = os.getenv("DATABASE_URL", "")
        data_dir = Path(__file__).parents[2] / "data"

        classes = {"mysql": MysqlStorage, "redis": RedisStorage, "file": FileStorage}

        if mode in ("mysql", "redis") and not url:
            raise ValueError(f"{mode.upper()}模式需要DATABASE_URL")

        git_url = os.getenv("GITSTORE_GIT_URL", "").strip()
        git_user = os.getenv("GITSTORE_GIT_USERNAME", "").strip()
        git_token = os.getenv("GITSTORE_GIT_TOKEN", "").strip()
        git_enabled = mode == "file" and git_url and git_user and git_token

        self.gitstore_state = "LOCAL"
        self.gitstore_pending = False
        self.gitstore_error: Optional[str] = None

        if git_enabled:
            try:
                git_dir = data_dir / "gitstore"
                self._storage = GitStoreStorage(git_url, git_user, git_token, git_dir)
                await self._storage.init_db()
                self._initialized = True
                self.gitstore_state = "ACTIVE"
                logger.info("[Storage] 使用 gitstore 模式")
                return
            except (GitStoreConfigError, GitStoreRepoNotFoundError) as e:
                logger.warning(f"[Storage] Gitstore 不可用，回退本地文件: {e}")
                self.gitstore_error = str(e)
            except GitStoreAuthError as e:
                logger.error(f"[Storage] Gitstore 鉴权失败，回退本地文件: {e}")
                self.gitstore_error = str(e)
            except GitStoreError as e:
                logger.warning(f"[Storage] Gitstore 初始化失败，回退本地文件: {e}")
                self.gitstore_error = str(e)

        storage_class = classes.get(mode, FileStorage)
        self._storage = storage_class(url, data_dir) if mode != "file" else storage_class(data_dir)

        await self._storage.init_db()
        self._initialized = True
        logger.info(f"[Storage] 使用{mode}模式")

    def get_status(self) -> Dict[str, Any]:
        state = self.gitstore_state
        pending = self.gitstore_pending
        error = self.gitstore_error
        if isinstance(self._storage, GitStoreStorage):
            gs_state, gs_pending, gs_error = self._storage.get_status()
            state = "ACTIVE" if gs_state == "active" else "DEGRADED"
            pending = gs_pending
            error = gs_error
        return {"mode": os.getenv("STORAGE_MODE", "file").upper(), "gitstore_state": state, "gitstore_pending": pending, "gitstore_error": error}

    def get_storage(self) -> BaseStorage:
        """获取存储实例"""
        if not self._initialized or not self._storage:
            raise RuntimeError("StorageManager未初始化")
        return self._storage

    async def close(self) -> None:
        """关闭存储"""
        if self._storage and hasattr(self._storage, 'close'):
            await self._storage.close()


# 全局实例
storage_manager = StorageManager()
