import os
import sys
import time
import subprocess
import tkinter as tk
from pathlib import Path
from tkinter import ttk

from app.logging_utils import LOGGER_GUI, get_logger

_LOGGER = get_logger(LOGGER_GUI)


class Splash(tk.Toplevel):
    def __init__(self, parent) -> None:
        super().__init__(parent)
        self.title("SISTEMA DE GESTION ODS - RECA")
        self.resizable(False, False)
        self.configure(bg="white")
        self.protocol("WM_DELETE_WINDOW", lambda: None)
        try:
            self.attributes("-topmost", True)
        except tk.TclError:
            _LOGGER.debug("No se pudo marcar splash como topmost.")

        container = tk.Frame(self, bg="white")
        container.pack(fill=tk.BOTH, expand=True, padx=24, pady=20)

        logo_path = os.path.join(os.path.dirname(__file__), "logo", "logo_reca.png")
        self.logo_image = None
        if os.path.exists(logo_path):
            try:
                self.logo_image = tk.PhotoImage(file=logo_path).subsample(4)
            except (tk.TclError, OSError, RuntimeError):
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
            _LOGGER.debug("No se pudo aplicar tema clam en splash.")
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

    def _center_window(self, width: int, height: int) -> None:
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


def run_smoke_test() -> None:
    from app.config import get_settings
    from app.storage import ensure_appdata_files
    from app.version import get_version

    ensure_appdata_files()

    version = get_version().strip()
    if not version or version == "0.0.0":
        raise RuntimeError("Smoke test fallo: VERSION no disponible en runtime.")

    settings = get_settings()
    missing = []
    if not settings.supabase_url:
        missing.append("SUPABASE_URL")
    if not settings.supabase_anon_key:
        missing.append("SUPABASE_ANON_KEY")
    if missing:
        raise RuntimeError(
            f"Smoke test fallo: faltan variables requeridas en runtime: {', '.join(missing)}"
        )

    google_features_configured = any(
        (
            settings.google_drive_shared_folder_id,
            settings.google_drive_template_spreadsheet_name,
            settings.google_sheets_default_spreadsheet_id,
        )
    )
    if google_features_configured:
        if not settings.google_service_account_file:
            raise RuntimeError(
                "Smoke test fallo: Google Drive/Sheets esta configurado pero falta GOOGLE_SERVICE_ACCOUNT_FILE en runtime."
            )
        service_account_path = Path(
            os.path.expandvars(settings.google_service_account_file)
        )
        if not service_account_path.exists():
            raise RuntimeError(
                "Smoke test fallo: GOOGLE_SERVICE_ACCOUNT_FILE apunta a una ruta inexistente en runtime: "
                f"{service_account_path}"
            )

    import app.supabase_client  # noqa: F401
    import app.updater  # noqa: F401
    import main_gui  # noqa: F401


def main() -> None:
    try:
        from app.storage import ensure_appdata_files

        ensure_appdata_files()
    except (OSError, RuntimeError, ValueError) as exc:
        _LOGGER.warning("No se pudo asegurar estructura local de appdata: %s", exc)

    if "--run-gui" in sys.argv:
        import main_gui

        main_gui.main()
        return
    if "--smoke-test" in sys.argv:
        run_smoke_test()
        return

    root = tk.Tk()
    root.withdraw()
    splash = Splash(root)

    splash.set_status("Iniciando aplicacion...")
    cmd = [sys.executable, "--run-gui"]
    try:
        process = subprocess.Popen(cmd, cwd=os.path.dirname(__file__))
    except OSError as exc:
        splash.close()
        root.destroy()
        raise RuntimeError(f"No se pudo iniciar la interfaz principal: {exc}") from exc

    # Valida arranque temprano para evitar procesos fallidos silenciosos.
    for _ in range(10):
        time.sleep(0.1)
        if process.poll() is not None:
            splash.close()
            root.destroy()
            raise RuntimeError(
                f"La interfaz termino antes de iniciar correctamente. Codigo={process.returncode}"
            )

    splash.set_status("Iniciando interfaz...")
    time.sleep(0.5)
    splash.close()
    root.destroy()


if __name__ == "__main__":
    main()
