import os
import sys
import time
import subprocess
import tkinter as tk
from tkinter import ttk
from pathlib import Path


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
        self._progress_mode = "indeterminate"

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

    def set_progress(self, value) -> None:
        if value is None:
            if self._progress_mode != "indeterminate":
                self.progress.config(mode="indeterminate")
                self._progress_mode = "indeterminate"
                self.progress.start(10)
            return
        if self._progress_mode != "determinate":
            self.progress.stop()
            self.progress.config(mode="determinate", maximum=100)
            self._progress_mode = "determinate"
        self.progress["value"] = max(0, min(100, int(value)))
        self.update_idletasks()
        self.update()

    def close(self) -> None:
        self.progress.stop()
        self.destroy()


def _find_installed_exe() -> str:
    base = os.getenv("LOCALAPPDATA")
    if base:
        candidate = Path(base) / "Programs" / "Sistema de Gestión ODS RECA" / "RECA_ODS.exe"
        if candidate.exists():
            return str(candidate)
    return sys.executable


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

    update_applied = False
    try:
        from app.updater import check_and_update

        def _show_status(message: str) -> None:
            splash.set_status(message)

        def _show_progress(value) -> None:
            splash.set_progress(value)

        splash.set_status("Buscando actualizaciones...")
        update_applied = check_and_update(
            status_callback=_show_status,
            progress_callback=_show_progress,
            version_callback=None,
        )
    except Exception:
        update_applied = False

    if update_applied:
        splash.set_status("Reiniciando aplicacion...")
        splash.set_progress(100)
        time.sleep(0.6)
        splash.close()
        root.destroy()
        target = _find_installed_exe()
        subprocess.Popen([target, "--run-gui"], cwd=os.path.dirname(__file__))
        return

    cmd = [sys.executable, "--run-gui"]
    subprocess.Popen(cmd, cwd=os.path.dirname(__file__))

    splash.set_status("Iniciando interfaz...")
    time.sleep(0.5)
    splash.close()
    root.destroy()


if __name__ == "__main__":
    main()
