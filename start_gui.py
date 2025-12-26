import os
import sys
import time
import subprocess
import tkinter as tk
from tkinter import ttk
from urllib.parse import urlparse
from urllib.request import urlopen


def _load_backend_url() -> str:
    env_path = None
    try:
        from app.paths import app_data_dir

        candidate = app_data_dir() / ".env"
        if candidate.exists():
            env_path = str(candidate)
    except Exception:
        env_path = None
    if not env_path:
        candidate = os.path.join(os.getcwd(), ".env")
        if os.path.exists(candidate):
            env_path = candidate
    if env_path:
        try:
            with open(env_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if line.startswith("BACKEND_URL="):
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass
    return os.getenv("BACKEND_URL", "http://localhost:8123")


class Splash(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("SISTEMA DE GESTION ODS - RECA")
        self.resizable(False, False)
        self.configure(bg="white")
        self.protocol("WM_DELETE_WINDOW", lambda: None)
        try:
            self.attributes("-topmost", True)
        except tk.TclError:
            pass

        container = tk.Frame(self, bg="white")
        container.pack(fill=tk.BOTH, expand=True, padx=24, pady=20)

        logo_path = os.path.join(os.path.dirname(__file__), "logo", "logo_reca.png")
        self.logo_image = None
        if os.path.exists(logo_path):
            try:
                self.logo_image = tk.PhotoImage(file=logo_path).subsample(4)
            except Exception:
                self.logo_image = None
        if self.logo_image:
            tk.Label(container, image=self.logo_image, bg="white").pack(pady=(0, 8))

        tk.Label(
            container,
            text="Cargando aplicacion...",
            font=("Arial", 12, "bold"),
            bg="white",
            fg="#7C3D96",
        ).pack(pady=(0, 8))

        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure(
            "Reca.Horizontal.TProgressbar",
            background="#07B499",
            troughcolor="#EDE7F3",
            bordercolor="#EDE7F3",
            lightcolor="#07B499",
            darkcolor="#07B499",
        )
        self.progress = ttk.Progressbar(
            container,
            length=280,
            mode="indeterminate",
            style="Reca.Horizontal.TProgressbar",
        )
        self.progress.pack(pady=(0, 6))
        self.progress.start(10)

        self.status_label = tk.Label(
            container,
            text="Iniciando...",
            font=("Arial", 10),
            bg="white",
            fg="#07B499",
        )
        self.status_label.pack()

        self._center_window(360, 230)
        self.lift()
        self.update_idletasks()

    def _center_window(self, width, height):
        self.update_idletasks()
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        x = int((screen_w - width) / 2)
        y = int((screen_h - height) / 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

    def set_status(self, message: str) -> None:
        self.status_label.config(text=message)
        self.update_idletasks()
        self.update()

    def close(self) -> None:
        self.progress.stop()
        self.destroy()


def _backend_ready(url: str) -> bool:
    try:
        urlopen(f"{url.rstrip('/')}/health", timeout=1)
        return True
    except Exception:
        return False


def main() -> None:
    try:
        from app.storage import ensure_appdata_files

        ensure_appdata_files()
    except Exception:
        pass

    if "--run-gui" in sys.argv:
        import main_gui

        main_gui.main()
        return

    root = tk.Tk()
    root.withdraw()
    splash = Splash(root)

    cmd = [sys.executable, "--run-gui"]
    subprocess.Popen(cmd, cwd=os.path.dirname(__file__))

    backend_url = _load_backend_url()
    parsed = urlparse(backend_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 8123

    start = time.time()
    while time.time() - start < 20:
        splash.set_status(f"Esperando backend {host}:{port}...")
        if _backend_ready(backend_url):
            break
        time.sleep(0.3)

    splash.set_status("Listo")
    time.sleep(0.3)
    splash.close()
    root.destroy()


if __name__ == "__main__":
    main()
