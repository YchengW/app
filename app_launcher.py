# app_launcher.py
import os, sys, threading, time, socket, asyncio
import webview
from uvicorn import Config, Server

# 关键：显式导入你的 FastAPI app，确保 PyInstaller 打包 app 包
from app.main import app   # <= 这行非常重要！

# （可选）Windows 上避免 asyncio 事件循环兼容性问题
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

def find_free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    addr, port = s.getsockname()
    s.close()
    return port

def run_server(port: int):
    # 直接传 app 对象，不用字符串路径
    config = Config(app=app, host="127.0.0.1", port=port, reload=False, log_level="info", workers=1)
    server = Server(config)
    server.run()  # 阻塞，放在线程里跑

def wait_port(host="127.0.0.1", port=8000, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.15)
    return False

def main():
    port = find_free_port()
    t = threading.Thread(target=run_server, args=(port,), daemon=True)
    t.start()

    if not wait_port(port=port, timeout=20):
        # 可以弹出一个错误窗口帮助定位
        webview.create_window("出让库软件 - 启动失败", html=f"<h3>后端未启动</h3><p>端口 {port} 等待超时。</p>")
        webview.start()
        return

    window = webview.create_window("出让库软件", f"http://127.0.0.1:{port}",
                                   width=1200, height=800, resizable=True)
    webview.start()

if __name__ == "__main__":
    main()
