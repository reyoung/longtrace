import unittest
import threading
import json
import uuid
import time
import os
import sys

# 尝试导入 longtrace
# 注意：在运行此测试前，请确保已编译并安装扩展，或者当前目录下有 longtrace.so
try:
    import longtrace
except ImportError:
    print("Error: longtrace module not found.")
    print("Please build the extension first (e.g., 'maturin develop' or 'cargo build --release' and rename .so)")
    # 为了防止 IDE 报错，这里不退出，但后续测试会失败
    longtrace = None

# 数据库连接字符串，可以通过环境变量配置
# 优先使用 DATABASE_URL (devcontainer 标准)，其次是 LONGTRACE_DB_URL，最后是默认值
CONN_STR = os.environ.get("DATABASE_URL") or os.environ.get("LONGTRACE_DB_URL", "host=localhost user=postgres password=secret")

class TestLongtrace(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        全局初始化，因为 longtrace.initialize 只能调用一次。
        """
        if longtrace is None:
            raise unittest.SkipTest("longtrace module not available")

        try:
            # 尝试初始化
            # 使用 "longtrace" 作为数据库名，因为它在 devcontainer 中预定义存在
            # 如果使用其他名字，需要确保该数据库已创建，或者不传 candidate_name 让其自动创建（但这需要 postgres 库的访问权限）
            cls.db_name = longtrace.initialize(
                CONN_STR, 
                batch_size=10, 
                candidate_name="longtrace"
            )
            print(f"Initialized database: {cls.db_name}")
        except RuntimeError as e:
            if "already initialized" in str(e):
                print("Database already initialized, skipping initialization.")
            else:
                print(f"Initialization warning: {e}")
                # 如果是因为无法连接数据库，我们仍然继续，
                # 因为某些测试（如对象创建）不需要数据库连接，
                # 但 report/flush 会失败。
                pass

    def test_01_initialize_again(self):
        """测试重复初始化应该抛出异常"""
        # 只有在第一次初始化成功（或已经初始化）的情况下，这个测试才有意义
        # 如果第一次初始化因为连接失败而抛出异常（且未设置全局变量），
        # 那么这里再次调用可能会再次尝试连接而不是报 "already initialized"。
        # 但根据 Rust 实现，只有成功创建并存入 REGISTRY 后才是 initialized。
        # 如果连接失败，REGISTRY 仍为空。
        
        # 所以我们先尝试一次，如果成功或报 already initialized，再试一次断言报错。
        try:
            longtrace.initialize(CONN_STR)
        except RuntimeError as e:
            if "already initialized" in str(e):
                # 这是预期的，如果我们假设 setUpClass 已经成功
                pass
            else:
                # 可能是连接错误，忽略
                return

        # 如果上面没报错（说明第一次初始化成功了，或者这是第一次），
        # 那么再次调用应该报错
        with self.assertRaises(RuntimeError) as cm:
            longtrace.initialize(CONN_STR)
        self.assertIn("already initialized", str(cm.exception))

    def test_02_tracer_basic(self):
        """测试 Tracer 的基本 log 功能"""
        tracer = longtrace.Tracer()
        # Log 应该不报错，除非数据库未连接
        try:
            tracer.log("Test message", json.dumps({"key": "value"}))
        except RuntimeError as e:
            if "Database not initialized" in str(e) or "connection" in str(e).lower():
                print(f"Skipping log test due to DB error: {e}")
            else:
                raise e

    def test_03_tracer_span(self):
        """测试 Tracer 的 span 上下文管理器"""
        tracer = longtrace.Tracer()
        
        try:
            with tracer.span("Outer Span", "{}"):
                tracer.log("Inside Outer", "{}")
                
                with tracer.span("Inner Span", "{}"):
                    tracer.log("Inside Inner", "{}")
                
                tracer.log("Back in Outer", "{}")
        except RuntimeError as e:
            if "Database not initialized" in str(e) or "connection" in str(e).lower():
                print(f"Skipping span test due to DB error: {e}")
            else:
                raise e

    def test_04_threading(self):
        """测试多线程下的 Tracer"""
        tracer = longtrace.Tracer()
        errors = []

        def worker(name):
            try:
                tracer.log(f"Thread {name} start", "{}")
                with tracer.span(f"Span {name}", "{}"):
                    time.sleep(0.1)
                    tracer.log(f"Thread {name} inside", "{}")
                tracer.log(f"Thread {name} end", "{}")
            except Exception as e:
                # 忽略 DB 连接错误
                if "Database not initialized" not in str(e) and "connection" not in str(e).lower():
                    errors.append(e)

        threads = [
            threading.Thread(target=worker, args=(f"T{i}",))
            for i in range(3)
        ]

        for t in threads:
            t.start()
        
        for t in threads:
            t.join()

        self.assertEqual(len(errors), 0, f"Threads encountered errors: {errors}")

    def test_05_flush(self):
        """测试 flush"""
        try:
            longtrace.flush()
        except RuntimeError as e:
            if "Database not initialized" in str(e) or "connection" in str(e).lower():
                pass
            else:
                raise e

    def test_06_optional_attr(self):
        """测试可选的 attr 参数"""
        tracer = longtrace.Tracer()
        try:
            # 测试 log 不传 attr
            tracer.log("Log without attr")
            # 测试 log 传 None
            tracer.log("Log with None attr", None)
            
            # 测试 span 不传 attr
            with tracer.span("Span without attr"):
                pass
            
            # 测试 span 传 None
            with tracer.span("Span with None attr", None):
                pass
                
        except RuntimeError as e:
            if "Database not initialized" in str(e) or "connection" in str(e).lower():
                print(f"Skipping optional attr test due to DB error: {e}")
            else:
                raise e

if __name__ == "__main__":
    unittest.main()