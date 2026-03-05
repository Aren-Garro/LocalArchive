"""Desktop GUI launcher for LocalArchive."""

from __future__ import annotations

import threading
import webbrowser
from tkinter import BOTH, DISABLED, END, NORMAL, Button, Entry, Frame, Label, StringVar, Tk

from localarchive.config import Config
from localarchive.ui.app import create_app


class LauncherApp:
    def __init__(self, root: Tk):
        self.root = root
        self.root.title("LocalArchive Launcher")
        self.root.geometry("420x220")
        self.root.resizable(False, False)

        self.server = None
        self.server_thread: threading.Thread | None = None
        self.running = False

        self.status_var = StringVar(value="Status: Stopped")
        self.host_var = StringVar(value="127.0.0.1")
        self.port_var = StringVar(value="8877")

        self._load_defaults()
        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _load_defaults(self) -> None:
        try:
            cfg = Config.load()
            self.host_var.set(cfg.ui.host)
            self.port_var.set(str(cfg.ui.port))
        except Exception:
            # Keep launcher functional even when config has issues.
            pass

    def _build_ui(self) -> None:
        panel = Frame(self.root, padx=18, pady=16)
        panel.pack(fill=BOTH, expand=True)

        Label(panel, text="Start LocalArchive local UI", font=("Segoe UI", 12, "bold")).pack(anchor="w")
        Label(panel, text="Host").pack(anchor="w", pady=(12, 2))
        self.host_entry = Entry(panel, textvariable=self.host_var)
        self.host_entry.pack(fill=BOTH)

        Label(panel, text="Port").pack(anchor="w", pady=(8, 2))
        self.port_entry = Entry(panel, textvariable=self.port_var)
        self.port_entry.pack(fill=BOTH)

        actions = Frame(panel)
        actions.pack(fill=BOTH, pady=(14, 8))
        self.start_btn = Button(actions, text="Start", width=12, command=self.start_server)
        self.start_btn.pack(side="left")
        self.stop_btn = Button(actions, text="Stop", width=12, state=DISABLED, command=self.stop_server)
        self.stop_btn.pack(side="left", padx=(10, 0))

        Label(panel, textvariable=self.status_var).pack(anchor="w")

    def start_server(self) -> None:
        if self.running:
            return
        host = self.host_var.get().strip() or "127.0.0.1"
        port_raw = self.port_var.get().strip()
        try:
            port = int(port_raw)
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            self.status_var.set("Status: Invalid port (use 1-65535)")
            return

        self.start_btn.config(state=DISABLED)
        self.stop_btn.config(state=NORMAL)
        self.host_entry.config(state=DISABLED)
        self.port_entry.config(state=DISABLED)
        self.status_var.set(f"Status: Starting on http://{host}:{port}")

        self.server_thread = threading.Thread(
            target=self._run_server,
            args=(host, port),
            daemon=True,
        )
        self.server_thread.start()
        self.root.after(700, lambda: webbrowser.open(f"http://{host}:{port}"))

    def _run_server(self, host: str, port: int) -> None:
        try:
            import uvicorn

            cfg = Config.load()
            cfg.ui.host = host
            cfg.ui.port = port
            cfg.ensure_dirs()
            create_app(cfg)
            self.server = uvicorn.Server(
                uvicorn.Config(
                    "localarchive.ui.app:app",
                    host=host,
                    port=port,
                    log_level="warning",
                )
            )
            self.running = True
            self.root.after(0, lambda: self.status_var.set(f"Status: Running on http://{host}:{port}"))
            self.server.run()
        except Exception as exc:
            self.root.after(0, lambda: self.status_var.set(f"Status: Failed to start ({exc})"))
        finally:
            self.running = False
            self.server = None
            self.root.after(0, self._reset_controls)

    def stop_server(self) -> None:
        if self.server is not None:
            self.status_var.set("Status: Stopping...")
            self.server.should_exit = True

    def _reset_controls(self) -> None:
        self.start_btn.config(state=NORMAL)
        self.stop_btn.config(state=DISABLED)
        self.host_entry.config(state=NORMAL)
        self.port_entry.config(state=NORMAL)
        if not self.running:
            self.status_var.set("Status: Stopped")

    def _on_close(self) -> None:
        self.stop_server()
        self.root.after(250, self.root.destroy)


def launch_gui() -> None:
    root = Tk()
    app = LauncherApp(root)
    root.mainloop()


if __name__ == "__main__":
    launch_gui()
