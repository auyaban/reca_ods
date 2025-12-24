import os
import re
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import date

import tkinter as tk
from tkinter import messagebox, ttk
import tkinter.font as tkfont

import threading
from urllib.parse import urlparse
import socket
import logging
from pathlib import Path


COLOR_PURPLE = "#7C3D96"
COLOR_TEAL = "#07B499"
_LOGO_PATH = None
_LOGO_CACHE: dict[int, tk.PhotoImage] = {}
_BACKEND_THREAD = None
_BACKEND_SERVER = None
_BACKEND_PROCESS = None
_BACKEND_STARTUP_ERROR = None
_DATE_ENTRY_CLS = None


def _get_date_entry():
    global _DATE_ENTRY_CLS
    if _DATE_ENTRY_CLS is None:
        try:
            from tkcalendar import DateEntry
        except ImportError:
            _DATE_ENTRY_CLS = False
        else:
            _DATE_ENTRY_CLS = DateEntry
    return None if _DATE_ENTRY_CLS is False else _DATE_ENTRY_CLS


def _load_logo(subsample: int = 12) -> tk.PhotoImage | None:
    global _LOGO_PATH
    if _LOGO_PATH is None:
        try:
            from app.paths import resource_path

            candidate = resource_path("logo/logo_reca.png")
            if candidate.exists():
                _LOGO_PATH = str(candidate)
            else:
                _LOGO_PATH = os.path.join(os.path.dirname(__file__), "logo", "logo_reca.png")
        except Exception:
            _LOGO_PATH = os.path.join(os.path.dirname(__file__), "logo", "logo_reca.png")
    if not os.path.exists(_LOGO_PATH):
        return None
    if subsample not in _LOGO_CACHE:
        _LOGO_CACHE[subsample] = tk.PhotoImage(file=_LOGO_PATH).subsample(subsample)
    return _LOGO_CACHE[subsample]


def _load_backend_url() -> str:
    from dotenv import load_dotenv
    env_path = None
    try:
        from app.paths import app_data_dir

        env_path = app_data_dir() / ".env"
    except Exception:
        env_path = None
    load_dotenv(dotenv_path=env_path, override=True)
    if not env_path or not env_path.exists():
        load_dotenv()
    return os.getenv("BACKEND_URL", "http://localhost:8123").rstrip("/")


def _parse_host_port(base_url: str) -> tuple[str, int]:
    parsed = urlparse(base_url)
    host = parsed.hostname or "127.0.0.1"
    if host.lower() in {"localhost", "::1"}:
        host = "127.0.0.1"
    port = parsed.port or 8000
    return host, port


def _startup_log_path() -> str:
    base = os.getenv("TEMP") or os.getenv("TMP") or os.getcwd()
    return os.path.join(base, "reca_ods_startup.log")


def _backend_subprocess_log_path() -> str:
    base = os.getenv("TEMP") or os.getenv("TMP") or os.getcwd()
    return os.path.join(base, "reca_ods_backend.log")


def _log_startup(message: str) -> None:
    try:
        with open(_startup_log_path(), "a", encoding="utf-8") as fh:
            fh.write(message.rstrip() + "\n")
    except Exception:
        pass


def _port_in_use(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _build_backend_app():
    try:
        import importlib

        backend_main = importlib.import_module("main")
        return backend_main.app
    except Exception:
        from fastapi import FastAPI, Request

        from app.routes import router
        from app.storage import ensure_appdata_files

        app = FastAPI(title="RECA ODS API")
        app.include_router(router)

        @app.on_event("startup")
        def _startup() -> None:
            ensure_appdata_files()

        log_dir = Path(__file__).resolve().parent / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "api.log"

        logger = logging.getLogger("reca_ods_api")
        if not logger.handlers:
            handler = logging.FileHandler(log_file, encoding="utf-8")
            formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.info("API logger iniciado. Archivo=%s", log_file)

        @app.middleware("http")
        async def log_requests(request: Request, call_next):
            start = time.perf_counter()
            try:
                response = await call_next(request)
            except Exception as exc:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.exception(
                    "ERROR %s %s -> 500 in %.2fms",
                    request.method,
                    request.url.path,
                    duration_ms,
                )
                raise exc

            duration_ms = (time.perf_counter() - start) * 1000
            logger.info(
                "%s %s -> %s in %.2fms",
                request.method,
                request.url.path,
                response.status_code,
                duration_ms,
            )
            return response

        return app


def _ensure_backend_running(base_url: str, status_callback=None) -> None:
    import requests
    if status_callback:
        status_callback("Verificando backend...", 10)
    try:
        requests.get(f"{base_url}/health", timeout=3)
        if status_callback:
            status_callback("Backend listo.", 100)
        return
    except requests.exceptions.RequestException:
        pass

    if status_callback:
        status_callback("Iniciando backend...", 30)
    host, port = _parse_host_port(base_url)
    _log_startup(f"Backend solicitado. base_url={base_url} host={host} port={port}")
    if _port_in_use(host, port):
        _log_startup(f"Puerto en uso antes de iniciar backend: {host}:{port}")

    if getattr(sys, "frozen", False):
        _start_backend_inprocess(host, port)
    else:
        cmd = [sys.executable, "-m", "uvicorn", "main:app", "--host", host, "--port", str(port)]
        kwargs = {"cwd": os.path.dirname(os.path.abspath(__file__))}
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        global _BACKEND_PROCESS
        _BACKEND_PROCESS = subprocess.Popen(cmd, **kwargs)

    for attempt in range(10):
        if status_callback:
            status_callback(f"Conectando al backend... ({attempt + 1}/10)", 40 + attempt * 5)
        try:
            requests.get(f"{base_url}/health", timeout=2)
            if status_callback:
                status_callback("Backend listo.", 100)
            return
        except requests.exceptions.RequestException:
            time.sleep(0.5)

    if getattr(sys, "frozen", False):
        if status_callback:
            status_callback("Reintentando backend...", 70)
        _start_backend_subprocess(host, port)
        for attempt in range(10):
            if status_callback:
                status_callback(f"Conectando al backend... ({attempt + 1}/10)", 70 + attempt * 3)
            try:
                requests.get(f"{base_url}/health", timeout=2)
                if status_callback:
                    status_callback("Backend listo.", 100)
                return
            except requests.exceptions.RequestException:
                time.sleep(0.6)
        if _BACKEND_PROCESS and _BACKEND_PROCESS.poll() is not None:
            _log_startup(
                "Backend subproceso termino con codigo "
                f"{_BACKEND_PROCESS.returncode}. Log: {_backend_subprocess_log_path()}"
            )

    if _BACKEND_STARTUP_ERROR:
        raise RuntimeError(
            "No se pudo iniciar el backend. "
            f"Detalle: {_BACKEND_STARTUP_ERROR}. "
            f"Revisa el log: {_startup_log_path()}"
        )
    raise RuntimeError(
        "No se pudo iniciar el backend. "
        f"Revisa el log: {_startup_log_path()}"
    )


def _backend_log_config() -> dict:
    return {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "default": {"class": "logging.Formatter", "format": "%(message)s"},
            "access": {"class": "logging.Formatter", "format": "%(message)s"},
        },
        "handlers": {
            "default": {"class": "logging.NullHandler", "formatter": "default"},
            "access": {"class": "logging.NullHandler", "formatter": "access"},
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "WARNING", "propagate": False},
            "uvicorn.error": {"handlers": ["default"], "level": "WARNING", "propagate": False},
            "uvicorn.access": {"handlers": ["access"], "level": "WARNING", "propagate": False},
        },
        "root": {"handlers": ["default"], "level": "WARNING"},
    }


def _start_backend_inprocess(host: str, port: int) -> None:
    import uvicorn
    global _BACKEND_THREAD, _BACKEND_SERVER
    if _BACKEND_THREAD and _BACKEND_THREAD.is_alive():
        return
    app = _build_backend_app()
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        log_level="warning",
        log_config=_backend_log_config(),
        access_log=False,
        use_colors=False,
    )
    server = uvicorn.Server(config)
    _BACKEND_SERVER = server
    thread = threading.Thread(target=_run_backend_server, args=(server,), daemon=True)
    _BACKEND_THREAD = thread
    thread.start()


def _start_backend_subprocess(host: str, port: int) -> None:
    global _BACKEND_PROCESS
    if _BACKEND_PROCESS and _BACKEND_PROCESS.poll() is None:
        return
    cmd = [sys.executable, "--backend", "--host", host, "--port", str(port)]
    kwargs = {"cwd": os.path.dirname(os.path.abspath(__file__))}
    if os.name == "nt":
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    log_path = _backend_subprocess_log_path()
    _log_startup(f"Lanzando backend subproceso. Log={log_path}")
    try:
        fh = open(log_path, "a", encoding="utf-8", buffering=1)
    except Exception:
        fh = None
    if fh:
        kwargs["stdout"] = fh
        kwargs["stderr"] = fh
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    kwargs["env"] = env
    _BACKEND_PROCESS = subprocess.Popen(cmd, **kwargs)


def _run_backend_server(server) -> None:
    global _BACKEND_STARTUP_ERROR
    try:
        _log_startup("Backend embebido iniciado.")
        server.run()
    except Exception as exc:
        _BACKEND_STARTUP_ERROR = exc
        _log_startup("ERROR backend embebido:")
        _log_startup(traceback.format_exc())


def _run_backend_only(host: str, port: int) -> None:
    import uvicorn
    _log_startup(f"Backend subproceso iniciado. host={host} port={port}")
    try:
        app = _build_backend_app()
        config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="info",
            log_config=None,
            access_log=False,
            use_colors=False,
        )
        server = uvicorn.Server(config)
        server.run()
    except Exception:
        _log_startup("ERROR backend subproceso:")
        _log_startup(traceback.format_exc())
        raise


def _stop_backend() -> None:
    global _BACKEND_PROCESS, _BACKEND_SERVER
    if _BACKEND_PROCESS and _BACKEND_PROCESS.poll() is None:
        try:
            _BACKEND_PROCESS.terminate()
        except Exception:
            pass
    if _BACKEND_SERVER:
        try:
            _BACKEND_SERVER.should_exit = True
        except Exception:
            pass

    try:
        host, port = _parse_host_port(_load_backend_url())
        if sys.platform.startswith("win"):
            cmd = f"for /f \"tokens=5\" %a in ('netstat -ano ^| findstr :{port}') do taskkill /PID %a /F"
            subprocess.Popen(
                ["cmd.exe", "/c", cmd],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
    except Exception:
        pass


def _prefetch_initial_data(api: "ApiClient", status_callback=None) -> None:
    items = [
        ("/wizard/seccion-1/orden-clausulada/opciones", None, "opciones de orden"),
        ("/wizard/seccion-1/profesionales", None, "profesionales"),
        ("/wizard/seccion-2/empresas", None, "empresas"),
        ("/wizard/seccion-3/tarifas", None, "tarifas"),
        ("/wizard/seccion-4/usuarios", None, "usuarios"),
        ("/wizard/seccion-4/discapacidades", None, "discapacidades"),
        ("/wizard/seccion-4/generos", None, "generos"),
        ("/wizard/seccion-4/contratos", None, "contratos"),
    ]
    api.prefetch(items, status_callback=status_callback)


class ApiClient:
    def __init__(self, base_url: str) -> None:
        import requests
        self.base_url = base_url.rstrip("/")
        self._session = requests.Session()
        self._cache: dict[tuple[str, str, tuple[tuple[str, str], ...] | None], dict] = {}

    def _cache_key(self, path: str, params: dict | None) -> tuple[str, str, tuple[tuple[str, str], ...] | None]:
        if params:
            normalized = tuple(sorted((str(k), str(v)) for k, v in params.items()))
        else:
            normalized = None
        return ("GET", path, normalized)

    def get(self, path: str, params: dict | None = None, use_cache: bool = False) -> dict:
        import requests
        cache_key = self._cache_key(path, params) if use_cache else None
        if cache_key and cache_key in self._cache:
            return self._cache[cache_key]
        url = f"{self.base_url}{path}"
        try:
            response = self._session.get(url, params=params, timeout=10)
            data = self._handle_response(response)
            if cache_key:
                self._cache[cache_key] = data
            return data
        except requests.exceptions.RequestException as exc:
            raise RuntimeError("No se pudo conectar al backend. Asegura que este corriendo.") from exc

    def get_cached(self, path: str, params: dict | None = None) -> dict:
        return self.get(path, params=params, use_cache=True)

    def invalidate(self, path: str, params: dict | None = None) -> None:
        key = self._cache_key(path, params)
        self._cache.pop(key, None)

    def prefetch(self, items: list[tuple[str, dict | None, str]], status_callback=None) -> None:
        total = len(items)
        for index, (path, params, label) in enumerate(items, start=1):
            if status_callback:
                progress = 60 + int((index / max(total, 1)) * 40)
                status_callback(f"Cargando {label}...", progress)
            self.get(path, params=params, use_cache=True)

    def post(self, path: str, payload: dict | None = None, timeout: int | float = 10) -> dict:
        import requests
        url = f"{self.base_url}{path}"
        try:
            response = self._session.post(url, json=payload, timeout=timeout)
            return self._handle_response(response)
        except requests.exceptions.RequestException as exc:
            raise RuntimeError("No se pudo conectar al backend. Asegura que este corriendo.") from exc

    def _handle_response(self, response) -> dict:
        try:
            data = response.json()
        except ValueError:
            response.raise_for_status()
            raise

        if response.status_code >= 400:
            detail = data.get("detail") if isinstance(data, dict) else data
            raise RuntimeError(detail)
        return data


class ScrollableFrame(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.canvas = tk.Canvas(self, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.content = ttk.Frame(self.canvas)

        self.content.bind(
            "<Configure>",
            lambda _e: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )

        self.canvas.create_window((0, 0), window=self.content, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self.canvas.bind("<Enter>", self._bind_mousewheel)
        self.canvas.bind("<Leave>", self._unbind_mousewheel)
        self.content.bind("<Enter>", self._bind_mousewheel)
        self.content.bind("<Leave>", self._unbind_mousewheel)

    def _bind_mousewheel(self, _event=None) -> None:
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel)

    def _unbind_mousewheel(self, _event=None) -> None:
        self.canvas.unbind_all("<MouseWheel>")
        self.canvas.unbind_all("<Button-4>")
        self.canvas.unbind_all("<Button-5>")

    def _on_mousewheel(self, event) -> None:
        if event.num == 4:
            self.canvas.yview_scroll(-1, "units")
        elif event.num == 5:
            self.canvas.yview_scroll(1, "units")
        else:
            delta = int(-1 * (event.delta / 120))
            self.canvas.yview_scroll(delta, "units")


class StartupSplash(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.title("SISTEMA DE GESTIÓN ODS - RECA")
        self.resizable(False, False)
        self.configure(bg="white")
        self.protocol("WM_DELETE_WINDOW", lambda: None)
        try:
            self.transient(parent)
        except tk.TclError:
            pass
        try:
            self.attributes("-topmost", True)
        except tk.TclError:
            pass

        container = tk.Frame(self, bg="white")
        container.pack(fill=tk.BOTH, expand=True, padx=24, pady=20)

        self.logo_image = _load_logo(subsample=4)
        if self.logo_image:
            tk.Label(container, image=self.logo_image, bg="white").pack(pady=(0, 8))

        tk.Label(
            container,
            text="Cargando aplicacion...",
            font=("Arial", 12, "bold"),
            bg="white",
            fg=COLOR_PURPLE,
        ).pack(pady=(0, 8))

        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure(
            "Reca.Horizontal.TProgressbar",
            background=COLOR_TEAL,
            troughcolor="#EDE7F3",
            bordercolor="#EDE7F3",
            lightcolor=COLOR_TEAL,
            darkcolor=COLOR_TEAL,
        )
        self.progress = ttk.Progressbar(
            container,
            length=280,
            mode="indeterminate",
            style="Reca.Horizontal.TProgressbar",
        )
        self.progress.pack(pady=(0, 6))
        self.progress.start(10)

        self._center_window(360, 220)
        self.lift()
        self.update_idletasks()

    def _center_window(self, width, height):
        self.update_idletasks()
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        x = int((screen_w - width) / 2)
        y = int((screen_h - height) / 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

    def set_status(self, message: str, progress: int | None = None) -> None:
        self.update_idletasks()
        self.update()

    def close(self) -> None:
        if hasattr(self, "progress"):
            self.progress.stop()
        self.destroy()


class LoadingDialog(tk.Toplevel):
    def __init__(self, parent, message: str, determinate: bool = False):
        super().__init__(parent)
        self.title("Procesando")
        self.resizable(False, False)
        self.configure(bg="white")
        self.protocol("WM_DELETE_WINDOW", lambda: None)
        try:
            self.transient(parent)
        except tk.TclError:
            pass
        try:
            self.attributes("-topmost", True)
        except tk.TclError:
            pass
        self._determinate = determinate

        container = tk.Frame(self, bg="white")
        container.pack(fill=tk.BOTH, expand=True, padx=24, pady=20)

        self.logo_image = _load_logo(subsample=7)
        if self.logo_image:
            tk.Label(container, image=self.logo_image, bg="white").pack(pady=(0, 8))

        tk.Label(
            container,
            text=message,
            font=("Arial", 11, "bold"),
            bg="white",
            fg=COLOR_PURPLE,
        ).pack(pady=(0, 10))

        self.status_label = tk.Label(
            container,
            text="Iniciando...",
            font=("Arial", 10),
            bg="white",
            fg=COLOR_TEAL,
        )
        self.status_label.pack(pady=(0, 8))

        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure(
            "Reca.Horizontal.TProgressbar",
            background=COLOR_TEAL,
            troughcolor="#EDE7F3",
            bordercolor="#EDE7F3",
            lightcolor=COLOR_TEAL,
            darkcolor=COLOR_TEAL,
        )
        self.progress = ttk.Progressbar(
            container,
            length=280,
            mode="determinate" if determinate else "indeterminate",
            maximum=100,
            style="Reca.Horizontal.TProgressbar",
        )
        self.progress.pack()
        if determinate:
            self.progress["value"] = 0
        else:
            self.progress.start(10)

        self._center_window(420, 200)
        self.lift()
        self.update_idletasks()
        self.update()

    def _center_window(self, width, height):
        self.update_idletasks()
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        x = int((screen_w - width) / 2)
        y = int((screen_h - height) / 2)
        self.geometry(f"{width}x{height}+{x}+{y}")

    def close(self) -> None:
        if not self._determinate:
            self.progress.stop()
        self.destroy()

    def set_status(self, message: str, progress: int | None = None) -> None:
        if message:
            self.status_label.config(text=message)
        if self._determinate and progress is not None:
            self.progress["value"] = max(0, min(100, progress))
        self.update_idletasks()
        self.update()


def set_widgets_state(container: tk.Widget, state: str) -> None:
    for child in container.winfo_children():
        try:
            current_state = str(child.cget("state"))
            if state == "normal" and current_state in ("readonly", "disabled"):
                pass
            else:
                child.configure(state=state)
        except tk.TclError:
            pass
        set_widgets_state(child, state)


def configure_combobox(combo: ttk.Combobox, values: list[str] | None = None) -> None:
    if values is not None:
        combo._all_values = list(values)

    def on_keyrelease(event):
        if event.keysym in {"Up", "Down", "Return", "Escape", "Tab"}:
            return
        current = combo.get().strip().lower()
        all_values = getattr(combo, "_all_values", None)
        if all_values is not None:
            filtered = [v for v in all_values if current in str(v).lower()]
            combo.configure(values=filtered)

    combo.bind("<KeyRelease>", on_keyrelease, add="+")


def format_currency(value) -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return ""
    return f"$ {amount:,.0f}".replace(",", ".")


def safe_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass
class WizardState:
    secciones: dict[str, dict] = field(default_factory=dict)
    usuarios_nuevos: list[dict] = field(default_factory=list)

    def reset_service(self, keep_id: bool = True) -> None:
        self.secciones = {}
        self.usuarios_nuevos = []


class BaseSection(ttk.Frame):
    def __init__(self, parent, title: str):
        super().__init__(parent)
        self.title_label = ttk.Label(
            self, text=title, font=("Arial", 12, "bold"), foreground=COLOR_PURPLE
        )
        self.title_label.grid(row=0, column=0, columnspan=4, sticky="w", pady=(8, 4))
        self.body = ttk.Frame(self)
        self.body.grid(row=1, column=0, columnspan=4, sticky="nsew")

    def set_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        set_widgets_state(self.body, state)


class Seccion1Frame(BaseSection):
    def __init__(self, parent, api: ApiClient, state: WizardState):
        super().__init__(parent, "Seccion 1 - Informacion basica y profesional")
        self.api = api
        self.state = state
        self.orden_var = tk.StringVar()
        self.prof_var = tk.StringVar()
        ttk.Label(self.body, text="Orden Clausulada").grid(row=0, column=0, sticky="w")
        self.orden_combo = ttk.Combobox(self.body, textvariable=self.orden_var, state="normal", width=20)
        self.orden_combo.grid(row=0, column=1, sticky="w")
        configure_combobox(self.orden_combo)

        ttk.Label(self.body, text="Profesional").grid(row=1, column=0, sticky="w")
        self.prof_combo = ttk.Combobox(self.body, textvariable=self.prof_var, state="normal", width=40)
        self.prof_combo.grid(row=1, column=1, sticky="w")
        configure_combobox(self.prof_combo)
        tk.Button(
            self.body,
            text="Agregar profesional/interprete",
            command=self._open_add_profesional,
            bg=COLOR_TEAL,
            fg="white",
            padx=8,
            pady=2,
        ).grid(row=1, column=2, sticky="w", padx=(10, 0))

    def load_data(self) -> None:
        orden_data = self.api.get_cached("/wizard/seccion-1/orden-clausulada/opciones")
        orden_labels = [item["label"] for item in orden_data["data"]]
        self.orden_combo.configure(values=orden_labels)
        self.orden_combo._all_values = orden_labels
        if orden_labels:
            self.orden_combo.set(orden_labels[0])

        prof_data = self.api.get_cached("/wizard/seccion-1/profesionales")
        prof_labels = sorted([item["nombre_profesional"] for item in prof_data["data"]])
        self.prof_combo.configure(values=prof_labels)
        self.prof_combo._all_values = prof_labels
        if prof_labels:
            self.prof_combo.set(prof_labels[0])

    def get_payload(self) -> dict:
        orden = self.orden_var.get().strip().lower()
        orden = "si" if orden.startswith("s") else "no"
        return {
            "orden_clausulada": orden,
            "nombre_profesional": self.prof_var.get().strip(),
        }

    def set_data(self, data: dict) -> None:
        orden = str(data.get("orden_clausulada", "")).strip().lower()
        self.orden_var.set("Sí" if orden.startswith("s") or orden == "true" else "No")
        self.prof_var.set(data.get("nombre_profesional", ""))

    def _open_add_profesional(self) -> None:
        dialog = tk.Toplevel(self)
        dialog.title("Agregar profesional/interprete")
        dialog.resizable(False, False)
        dialog.geometry("420x240")

        nombre_var = tk.StringVar()
        programa_var = tk.StringVar()

        ttk.Label(dialog, text="Nombre profesional").grid(row=0, column=0, sticky="w", padx=10, pady=8)
        ttk.Entry(dialog, textvariable=nombre_var, width=30).grid(row=0, column=1, padx=10, pady=8)

        ttk.Label(dialog, text="Programa").grid(row=1, column=0, sticky="w", padx=10, pady=8)
        programa_combo = ttk.Combobox(
            dialog,
            textvariable=programa_var,
            values=["Inclusión Laboral", "Interprete"],
            state="normal",
            width=20,
        )
        programa_combo.grid(row=1, column=1, padx=10, pady=8, sticky="w")
        programa_combo.set("Inclusión Laboral")

        def on_save():
            payload = {
                "nombre_profesional": nombre_var.get(),
                "programa": programa_var.get(),
            }
            try:
                data = self.api.post("/wizard/seccion-1/profesionales", payload)
            except Exception as exc:
                messagebox.showerror("Error", f"No se pudo guardar: {exc}")
                return

            if data.get("data"):
                nuevo = data["data"][0]
                nombre = nuevo.get("nombre_profesional", "").strip()
                if nombre:
                    values = list(self.prof_combo._all_values)
                    if nombre not in values:
                        values.append(nombre)
                        values.sort()
                        self.prof_combo._all_values = values
                        self.prof_combo.configure(values=values)
                    self.prof_var.set(nombre)
                self.api.invalidate("/wizard/seccion-1/profesionales")
            dialog.destroy()

        tk.Button(
            dialog,
            text="Guardar",
            command=on_save,
            bg=COLOR_TEAL,
            fg="white",
            padx=10,
            pady=4,
        ).grid(row=2, column=0, columnspan=2, pady=16)
class Seccion2Frame(BaseSection):
    def __init__(self, parent, api: ApiClient):
        super().__init__(parent, "Seccion 2 - Informacion de la empresa")
        self.api = api
        self.nit_var = tk.StringVar()
        self.nombre_var = tk.StringVar()
        self.caja_var = tk.StringVar()
        self.asesor_var = tk.StringVar()
        self.sede_var = tk.StringVar()
        self._nits: list[str] = []
        self._empresas_by_nit: dict[str, dict] = {}

        ttk.Label(self.body, text="NIT Empresa").grid(row=0, column=0, sticky="w")
        self.nit_combo = ttk.Combobox(self.body, textvariable=self.nit_var, state="normal", width=30)
        self.nit_combo.grid(row=0, column=1, sticky="w")
        self.nit_combo.bind("<<ComboboxSelected>>", self._on_nit_selected)
        self.nit_combo.bind("<KeyRelease>", self._on_nit_typed)
        self.nit_combo.bind("<Return>", self._on_nit_confirm)
        self.nit_combo.bind("<FocusOut>", self._on_nit_confirm)
        configure_combobox(self.nit_combo)

        ttk.Label(self.body, text="Nombre Empresa").grid(row=1, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.nombre_var, state="readonly", width=50).grid(
            row=1, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Caja Compensacion").grid(row=2, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.caja_var, state="readonly", width=40).grid(
            row=2, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Asesor").grid(row=3, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.asesor_var, state="readonly", width=40).grid(
            row=3, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Sede Empresa").grid(row=4, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.sede_var, state="readonly", width=40).grid(
            row=4, column=1, sticky="w"
        )

    def load_data(self) -> None:
        data = self.api.get_cached("/wizard/seccion-2/empresas")
        self._empresas_by_nit = {}
        for item in data["data"]:
            nit = item.get("nit_empresa")
            if nit is None:
                continue
            self._empresas_by_nit[str(nit)] = item
        nits = list(self._empresas_by_nit.keys())
        self._nits = sorted(nits, key=lambda value: int(re.sub(r"\D", "", value) or 0))
        self.nit_combo.configure(values=self._nits)
        self.nit_combo._all_values = self._nits
        if self._nits:
            self.nit_combo.set(self._nits[0])
            self._fetch_empresa(self._nits[0])

    def _on_nit_selected(self, _event) -> None:
        self._fetch_empresa(self.nit_var.get())

    def _on_nit_typed(self, _event) -> None:
        value = self.nit_var.get().strip()
        if not value:
            self.nit_combo.configure(values=self._nits)
            return
        filtered = [nit for nit in self._nits if value in nit]
        self.nit_combo.configure(values=filtered)

    def _on_nit_confirm(self, _event) -> None:
        nit = self.nit_var.get().strip()
        if nit:
            self._fetch_empresa(nit)

    def _fetch_empresa(self, nit: str) -> None:
        if not nit:
            return
        empresa = self._empresas_by_nit.get(nit)
        if not empresa:
            data = self.api.get_cached("/wizard/seccion-2/empresa", params={"nit": nit})
            if not data["data"]:
                self.nombre_var.set("")
                self.caja_var.set("")
                self.asesor_var.set("")
                self.sede_var.set("")
                return
            empresa = data["data"][0]
            self._empresas_by_nit[nit] = empresa
        if not empresa:
            self.nombre_var.set("")
            self.caja_var.set("")
            self.asesor_var.set("")
            self.sede_var.set("")
            return
        self.nombre_var.set(empresa.get("nombre_empresa", ""))
        self.caja_var.set(empresa.get("caja_compensacion", ""))
        self.asesor_var.set(empresa.get("asesor", ""))
        self.sede_var.set(empresa.get("sede_empresa", ""))

    def get_payload(self) -> dict:
        return {
            "nit_empresa": self.nit_var.get().strip(),
            "nombre_empresa": self.nombre_var.get().strip(),
            "caja_compensacion": self.caja_var.get().strip() or None,
            "asesor_empresa": self.asesor_var.get().strip() or None,
            "sede_empresa": self.sede_var.get().strip() or None,
        }

    def set_data(self, data: dict) -> None:
        self.nit_var.set(data.get("nit_empresa", ""))
        self.nombre_var.set(data.get("nombre_empresa", ""))
        self.caja_var.set(data.get("caja_compensacion", "") or "")
        self.asesor_var.set(data.get("asesor_empresa", "") or "")
        self.sede_var.set(data.get("sede_empresa", "") or "")


class Seccion3Frame(BaseSection):
    def __init__(self, parent, api: ApiClient):
        super().__init__(parent, "Seccion 3 - Informacion del servicio")
        self.api = api
        self.fecha_var = tk.StringVar()
        self.codigo_var = tk.StringVar()
        self.referencia_var = tk.StringVar()
        self.descripcion_var = tk.StringVar()
        self.modalidad_var = tk.StringVar()
        self.valor_base_var = tk.StringVar()
        self.valor_base_display_var = tk.StringVar()
        self.interpretacion_var = tk.BooleanVar(value=False)
        self.horas_var = tk.StringVar()
        self.minutos_var = tk.StringVar()
        self.horas_decimal_var = tk.StringVar()
        self.total_calculado_var = tk.StringVar()
        self._codigos: list[str] = []
        self._tarifas_by_codigo: dict[str, dict] = {}
        self._last_codigo: str | None = None

        ttk.Label(self.body, text="Fecha Servicio (YYYY-MM-DD)").grid(row=0, column=0, sticky="w")
        DateEntry = _get_date_entry()
        if DateEntry:
            self.fecha_widget = DateEntry(
                self.body,
                date_pattern="yyyy-mm-dd",
                textvariable=self.fecha_var,
                width=18,
            )
        else:
            self.fecha_widget = ttk.Entry(self.body, textvariable=self.fecha_var, width=20)
            self.fecha_var.set(date.today().isoformat())
        self.fecha_widget.grid(row=0, column=1, sticky="w")

        ttk.Label(self.body, text="Codigo Servicio").grid(row=1, column=0, sticky="w")
        self.codigo_combo = ttk.Combobox(self.body, textvariable=self.codigo_var, state="normal", width=20)
        self.codigo_combo.grid(row=1, column=1, sticky="w")
        self.codigo_combo.bind("<<ComboboxSelected>>", self._on_codigo_selected)
        self.codigo_combo.bind("<KeyRelease>", self._on_codigo_typed)
        self.codigo_combo.bind("<Return>", self._on_codigo_confirm)
        self.codigo_combo.bind("<FocusOut>", self._on_codigo_confirm)
        configure_combobox(self.codigo_combo)

        ttk.Label(self.body, text="Referencia").grid(row=2, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.referencia_var, state="readonly", width=40).grid(
            row=2, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Descripcion").grid(row=3, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.descripcion_var, state="readonly", width=60).grid(
            row=3, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Modalidad").grid(row=4, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.modalidad_var, state="readonly", width=25).grid(
            row=4, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Valor Base").grid(row=5, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.valor_base_display_var, state="readonly", width=25).grid(
            row=5, column=1, sticky="w"
        )

        ttk.Checkbutton(
            self.body,
            text="Servicio de interpretacion",
            variable=self.interpretacion_var,
            command=self._toggle_interprete,
        ).grid(row=6, column=0, sticky="w", pady=(6, 0))

        self.interprete_frame = ttk.Frame(self.body)
        ttk.Label(self.interprete_frame, text="Horas").grid(row=0, column=0, sticky="w")
        self.horas_entry = ttk.Entry(self.interprete_frame, textvariable=self.horas_var, width=8, state="disabled")
        self.horas_entry.grid(row=0, column=1, sticky="w")
        ttk.Label(self.interprete_frame, text="Minutos").grid(row=0, column=2, sticky="w", padx=(10, 0))
        self.minutos_entry = ttk.Entry(self.interprete_frame, textvariable=self.minutos_var, width=8, state="disabled")
        self.minutos_entry.grid(row=0, column=3, sticky="w")
        ttk.Label(self.interprete_frame, text="Horas decimales").grid(row=0, column=4, sticky="w", padx=(10, 0))
        self.horas_decimal_entry = ttk.Entry(
            self.interprete_frame, textvariable=self.horas_decimal_var, width=8, state="readonly"
        )
        self.horas_decimal_entry.grid(row=0, column=5, sticky="w")
        ttk.Label(self.interprete_frame, text="Total").grid(row=0, column=6, sticky="w", padx=(10, 0))
        self.total_interprete_entry = ttk.Entry(
            self.interprete_frame, textvariable=self.total_calculado_var, width=14, state="readonly"
        )
        self.total_interprete_entry.grid(row=0, column=7, sticky="w")
        self.calcular_button = tk.Button(
            self.interprete_frame,
            text="Calcular",
            command=self._calcular_interprete,
            bg=COLOR_TEAL,
            fg="white",
            padx=8,
            pady=2,
            state="disabled",
        )
        self.calcular_button.grid(row=0, column=8, sticky="w", padx=(10, 0))
        self.interprete_frame.grid(row=7, column=0, columnspan=2, sticky="w", pady=(4, 0))
        self.interprete_frame.grid_remove()

    def _toggle_interprete(self) -> None:
        if self.interpretacion_var.get():
            self.interprete_frame.grid()
            self.horas_entry.configure(state="normal")
            self.minutos_entry.configure(state="normal")
            self.calcular_button.configure(state="normal")
        else:
            self.interprete_frame.grid_remove()
            self.horas_entry.configure(state="disabled")
            self.minutos_entry.configure(state="disabled")
            self.calcular_button.configure(state="disabled")
            self.horas_var.set("")
            self.minutos_var.set("")
            self.horas_decimal_var.set("")
            self.total_calculado_var.set("")

    def load_data(self) -> None:
        data = self.api.get_cached("/wizard/seccion-3/tarifas")
        self._tarifas_by_codigo = {}
        for item in data["data"]:
            codigo = item.get("codigo_servicio")
            if codigo is None:
                continue
            self._tarifas_by_codigo[str(codigo)] = item
        self._codigos = list(self._tarifas_by_codigo.keys())
        self.codigo_combo.configure(values=self._codigos)
        self.codigo_combo._all_values = self._codigos
        if self._codigos:
            self.codigo_combo.set(self._codigos[0])
            self._fetch_tarifa(self._codigos[0])

    def _on_codigo_selected(self, _event) -> None:
        self._fetch_tarifa(self.codigo_var.get())

    def _on_codigo_typed(self, _event) -> None:
        value = self.codigo_var.get().strip()
        if not value:
            self.codigo_combo.configure(values=self._codigos)
            return
        filtered = [codigo for codigo in self._codigos if value in codigo]
        self.codigo_combo.configure(values=filtered)

    def _on_codigo_confirm(self, _event) -> None:
        codigo = self.codigo_var.get().strip()
        if codigo:
            self._fetch_tarifa(codigo)

    def _fetch_tarifa(self, codigo: str) -> None:
        codigo = str(codigo).strip()
        if not codigo:
            return
        tarifa = self._tarifas_by_codigo.get(codigo)
        if not tarifa:
            data = self.api.get_cached("/wizard/seccion-3/tarifa", params={"codigo": codigo})
            if not data["data"]:
                return
            tarifa = data["data"][0]
            self._tarifas_by_codigo[codigo] = tarifa
        self.referencia_var.set(tarifa.get("referencia_servicio", ""))
        self.descripcion_var.set(tarifa.get("descripcion_servicio", ""))
        self.modalidad_var.set(tarifa.get("modalidad_servicio", ""))
        valor_base = tarifa.get("valor_base", "")
        self.valor_base_var.set(str(valor_base))
        self.valor_base_display_var.set(format_currency(valor_base))
        self._last_codigo = codigo
        self.total_calculado_var.set("")

    def _calcular_interprete(self) -> None:
        base = float(self.valor_base_var.get() or 0)
        horas = safe_int(self.horas_var.get() or "") or 0
        minutos = safe_int(self.minutos_var.get() or "") or 0
        horas_decimal = horas + (minutos / 60)
        self.horas_decimal_var.set(f"{horas_decimal:.2f}")
        total = horas_decimal * base
        self.total_calculado_var.set(format_currency(total))

    def ensure_tarifa_loaded(self) -> None:
        codigo = self.codigo_var.get().strip()
        if codigo and codigo != self._last_codigo:
            self._fetch_tarifa(codigo)

    def get_payload(self) -> dict:
        payload = {
            "fecha_servicio": self.fecha_var.get().strip(),
            "codigo_servicio": self.codigo_var.get().strip(),
            "referencia_servicio": self.referencia_var.get().strip(),
            "descripcion_servicio": self.descripcion_var.get().strip(),
            "modalidad_servicio": self.modalidad_var.get().strip(),
            "valor_base": float(self.valor_base_var.get() or 0),
            "servicio_interpretacion": self.interpretacion_var.get(),
        }
        if self.interpretacion_var.get():
            payload["horas_interprete"] = int(self.horas_var.get() or 0)
            payload["minutos_interprete"] = int(self.minutos_var.get() or 0)
        return payload

    def set_data(self, data: dict) -> None:
        self.fecha_var.set(data.get("fecha_servicio", ""))
        codigo = str(data.get("codigo_servicio", "")).strip()
        self.codigo_var.set(codigo)
        self.referencia_var.set(data.get("referencia_servicio", ""))
        self.descripcion_var.set(data.get("descripcion_servicio", ""))
        self.modalidad_var.set(data.get("modalidad_servicio", ""))

        base = max(
            float(data.get("valor_virtual", 0) or 0),
            float(data.get("valor_bogota", 0) or 0),
            float(data.get("valor_otro", 0) or 0),
            float(data.get("todas_modalidades", 0) or 0),
        )
        self.valor_base_var.set(str(base))
        self.valor_base_display_var.set(format_currency(base))
        self._last_codigo = codigo

        horas_decimal = data.get("horas_interprete")
        valor_interprete = data.get("valor_interprete") or 0
        if horas_decimal:
            self.interpretacion_var.set(True)
            horas_decimal = float(horas_decimal)
            horas = int(horas_decimal)
            minutos = int(round((horas_decimal - horas) * 60))
            self._toggle_interprete()
            self.horas_var.set(str(horas))
            self.minutos_var.set(str(minutos))
            self.horas_decimal_var.set(f"{horas_decimal:.2f}")
            self.total_calculado_var.set(format_currency(valor_interprete))
        else:
            self.interpretacion_var.set(False)
            self._toggle_interprete()
class PersonaRow(ttk.Frame):
    def __init__(self, parent, cedulas, discapacidades, generos, contratos, on_search):
        super().__init__(parent)
        self.nombre_var = tk.StringVar()
        self.cedula_var = tk.StringVar()
        self.discapacidad_var = tk.StringVar()
        self.genero_var = tk.StringVar()
        self.fecha_ingreso_var = tk.StringVar()
        self.tipo_contrato_var = tk.StringVar()
        self.cargo_var = tk.StringVar()

        ttk.Label(self, text="Cedula").grid(row=0, column=0, sticky="w")
        self.cedula_combo = ttk.Combobox(self, textvariable=self.cedula_var, values=cedulas, width=14)
        self.cedula_combo.grid(row=0, column=1, sticky="w")
        configure_combobox(self.cedula_combo, cedulas)
        tk.Button(self, text="Buscar", command=lambda: on_search(self)).grid(
            row=0, column=2, sticky="w", padx=(6, 0)
        )
        tk.Button(self, text="Quitar", command=self._remove_self).grid(
            row=0, column=5, sticky="w", padx=(6, 0)
        )

        ttk.Label(self, text="Nombre").grid(row=0, column=3, sticky="w", padx=(10, 0))
        self.nombre_entry = ttk.Entry(self, textvariable=self.nombre_var, width=24, state="readonly")
        self.nombre_entry.grid(row=0, column=4, sticky="w")

        ttk.Label(self, text="Discapacidad").grid(row=1, column=0, sticky="w")
        self.discapacidad_combo = ttk.Combobox(
            self, textvariable=self.discapacidad_var, values=discapacidades, width=14, state="readonly"
        )
        self.discapacidad_combo.grid(row=1, column=1, sticky="w")
        configure_combobox(self.discapacidad_combo, discapacidades)

        ttk.Label(self, text="Genero").grid(row=1, column=3, sticky="w", padx=(10, 0))
        self.genero_combo = ttk.Combobox(
            self, textvariable=self.genero_var, values=generos, width=14, state="readonly"
        )
        self.genero_combo.grid(row=1, column=4, sticky="w")
        configure_combobox(self.genero_combo, generos)

        ttk.Label(self, text="Fecha ingreso (YYYY-MM-DD)").grid(row=2, column=0, sticky="w")
        DateEntry = _get_date_entry()
        if DateEntry:
            self.fecha_ingreso_widget = DateEntry(
                self,
                date_pattern="yyyy-mm-dd",
                textvariable=self.fecha_ingreso_var,
                width=12,
            )
        else:
            self.fecha_ingreso_widget = ttk.Entry(self, textvariable=self.fecha_ingreso_var, width=14)
        self.fecha_ingreso_widget.grid(row=2, column=1, sticky="w")

        ttk.Label(self, text="Tipo contrato").grid(row=2, column=3, sticky="w", padx=(10, 0))
        self.contrato_combo = ttk.Combobox(
            self, textvariable=self.tipo_contrato_var, values=contratos, width=14, state="normal"
        )
        self.contrato_combo.grid(row=2, column=4, sticky="w")
        configure_combobox(self.contrato_combo, contratos)

        ttk.Label(self, text="Cargo").grid(row=3, column=0, sticky="w")
        ttk.Entry(self, textvariable=self.cargo_var, width=24).grid(row=3, column=1, sticky="w")

    def _remove_self(self) -> None:
        self.destroy()

    def set_highlight(self, enabled: bool) -> None:
        color = "#FFF2CC" if enabled else "white"
        self.configure(style="Highlight.TFrame" if enabled else "TFrame")
        widgets = [
            self.nombre_entry,
            self.cedula_combo,
            self.discapacidad_combo,
            self.genero_combo,
            self.contrato_combo,
        ]
        for widget in widgets:
            try:
                widget.configure(background=color)
            except tk.TclError:
                pass

    def as_payload(self) -> dict:
        return {
            "nombre_usuario": self.nombre_var.get().strip(),
            "cedula_usuario": self.cedula_var.get().strip(),
            "discapacidad_usuario": self.discapacidad_var.get().strip(),
            "genero_usuario": self.genero_var.get().strip(),
            "fecha_ingreso": self.fecha_ingreso_var.get().strip(),
            "tipo_contrato": self.tipo_contrato_var.get().strip(),
            "cargo_servicio": self.cargo_var.get().strip(),
        }

    def set_from_user(self, user: dict) -> None:
        self.nombre_var.set(user.get("nombre_usuario", ""))
        self.cedula_var.set(user.get("cedula_usuario", ""))
        self.discapacidad_var.set(user.get("discapacidad_usuario", ""))
        self.genero_var.set(user.get("genero_usuario", ""))

    def set_from_record(self, record: dict) -> None:
        self.nombre_var.set(record.get("nombre_usuario", ""))
        self.cedula_var.set(record.get("cedula_usuario", ""))
        self.discapacidad_var.set(record.get("discapacidad_usuario", ""))
        self.genero_var.set(record.get("genero_usuario", ""))
        self.fecha_ingreso_var.set(record.get("fecha_ingreso", ""))
        self.tipo_contrato_var.set(record.get("tipo_contrato", ""))
        self.cargo_var.set(record.get("cargo_servicio", ""))


class Seccion4Frame(BaseSection):
    def __init__(self, parent, api: ApiClient, state: WizardState):
        super().__init__(parent, "Seccion 4 - Oferentes")
        self.api = api
        self.state = state
        self.cedulas = []
        self.discapacidades = []
        self.generos = []
        self.contratos = []
        self.rows: list[PersonaRow] = []
        self.usuarios_by_cedula: dict[str, dict] = {}

        self.rows_frame = ttk.Frame(self.body)
        self.rows_frame.grid(row=0, column=0, columnspan=4, sticky="w")

        controls = ttk.Frame(self.body)
        controls.grid(row=1, column=0, columnspan=4, sticky="w", pady=(8, 0))
        tk.Button(
            controls,
            text="Agregar Usuario",
            command=self._add_row,
            bg=COLOR_TEAL,
            fg="white",
            padx=10,
        ).pack(side="left", padx=(0, 8))
        tk.Button(
            controls,
            text="Crear Usuario",
            command=self._open_create_user,
            bg=COLOR_PURPLE,
            fg="white",
            padx=10,
        ).pack(side="left")

    def load_data(self) -> None:
        cedulas_data = self.api.get_cached("/wizard/seccion-4/usuarios")
        self.usuarios_by_cedula = {}
        for item in cedulas_data["data"]:
            cedula = item.get("cedula_usuario")
            if not cedula:
                continue
            self.usuarios_by_cedula[str(cedula)] = item
        self.cedulas = list(self.usuarios_by_cedula.keys())

        disc_data = self.api.get_cached("/wizard/seccion-4/discapacidades")
        self.discapacidades = [item["label"] for item in disc_data["data"]]

        genero_data = self.api.get_cached("/wizard/seccion-4/generos")
        self.generos = [item["label"] for item in genero_data["data"]]

        contratos_data = self.api.get_cached("/wizard/seccion-4/contratos")
        self.contratos = [item["label"] for item in contratos_data["data"]]

        self._add_row()

    def _add_row(self) -> None:
        row = PersonaRow(
            self.rows_frame,
            self.cedulas,
            self.discapacidades,
            self.generos,
            self.contratos,
            self._search_user,
        )
        row.grid(row=len(self.rows), column=0, sticky="w", pady=(4, 8))
        row.cedula_combo.bind("<<ComboboxSelected>>", lambda _e, r=row: self._fill_user(r))
        row.cedula_combo.bind("<KeyRelease>", lambda _e, r=row: self._filter_cedulas(r))
        row.cedula_combo.bind("<Return>", lambda _e, r=row: self._search_user(r))
        self.rows.append(row)

    def clear_rows(self) -> None:
        for row in self.rows:
            row.destroy()
        self.rows = []

    def set_data(self, data: dict) -> None:
        def split_field(value: str) -> list[str]:
            if not value:
                return []
            return [item.strip() for item in str(value).split(";") if item.strip()]

        nombres = split_field(data.get("nombre_usuario", ""))
        cedulas = split_field(data.get("cedula_usuario", ""))
        discapacidades = split_field(data.get("discapacidad_usuario", ""))
        generos = split_field(data.get("genero_usuario", ""))
        fechas = split_field(data.get("fecha_ingreso", ""))
        contratos = split_field(data.get("tipo_contrato", ""))
        cargos = split_field(data.get("cargo_servicio", ""))

        total = max(
            len(nombres),
            len(cedulas),
            len(discapacidades),
            len(generos),
            len(fechas),
            len(contratos),
            len(cargos),
            1,
        )

        self.clear_rows()
        for idx in range(total):
            self._add_row()
            row = self.rows[-1]
            record = {
                "nombre_usuario": nombres[idx] if idx < len(nombres) else "",
                "cedula_usuario": cedulas[idx] if idx < len(cedulas) else "",
                "discapacidad_usuario": discapacidades[idx] if idx < len(discapacidades) else "",
                "genero_usuario": generos[idx] if idx < len(generos) else "",
                "fecha_ingreso": fechas[idx] if idx < len(fechas) else "",
                "tipo_contrato": contratos[idx] if idx < len(contratos) else "",
                "cargo_servicio": cargos[idx] if idx < len(cargos) else "",
            }
            row.set_from_record(record)

    def _filter_cedulas(self, row: PersonaRow) -> None:
        value = row.cedula_var.get().strip()
        if not value:
            row.cedula_combo.configure(values=self.cedulas)
            return
        filtered = [cedula for cedula in self.cedulas if value in cedula]
        row.cedula_combo.configure(values=filtered)

    def _fill_user(self, row: PersonaRow) -> None:
        cedula = row.cedula_var.get().strip()
        if not cedula:
            return
        user = self.usuarios_by_cedula.get(cedula)
        if not user:
            data = self.api.get_cached("/wizard/seccion-4/usuario", params={"cedula": cedula})
            if not data["data"]:
                return
            user = data["data"][0]
            self.usuarios_by_cedula[cedula] = user
            if cedula not in self.cedulas:
                self.cedulas.append(cedula)
        row.set_from_user(user)

    def _search_user(self, row: PersonaRow) -> None:
        cedula = row.cedula_var.get().strip()
        if not cedula:
            return
        if cedula in self.usuarios_by_cedula:
            self._fill_user(row)
            return
        confirm = messagebox.askyesno(
            "Usuario no encontrado",
            "No existe un usuario con esa cedula. Deseas crearlo?",
        )
        if confirm:
            self._open_create_user(prefill_cedula=cedula, target_row=row)

    def _open_create_user(self, prefill_cedula: str | None = None, target_row: PersonaRow | None = None) -> None:
        dialog = tk.Toplevel(self)
        dialog.title("Crear usuario")
        dialog.geometry("420x360")
        dialog.resizable(False, False)

        nombre_var = tk.StringVar()
        cedula_var = tk.StringVar()
        discapacidad_var = tk.StringVar()
        genero_var = tk.StringVar()

        ttk.Label(dialog, text="Nombre").grid(row=0, column=0, sticky="w", padx=10, pady=6)
        ttk.Entry(dialog, textvariable=nombre_var, width=30).grid(row=0, column=1, padx=10, pady=6)
        ttk.Label(dialog, text="Cedula").grid(row=1, column=0, sticky="w", padx=10, pady=6)
        if prefill_cedula:
            cedula_var.set(prefill_cedula)
        ttk.Entry(dialog, textvariable=cedula_var, width=20).grid(row=1, column=1, padx=10, pady=6)
        ttk.Label(dialog, text="Discapacidad").grid(row=2, column=0, sticky="w", padx=10, pady=6)
        discapacidad_combo = ttk.Combobox(
            dialog, textvariable=discapacidad_var, values=self.discapacidades, state="normal", width=18
        )
        discapacidad_combo.grid(row=2, column=1, padx=10, pady=6)
        configure_combobox(discapacidad_combo, self.discapacidades)
        ttk.Label(dialog, text="Genero").grid(row=3, column=0, sticky="w", padx=10, pady=6)
        genero_combo = ttk.Combobox(
            dialog, textvariable=genero_var, values=self.generos, state="normal", width=18
        )
        genero_combo.grid(row=3, column=1, padx=10, pady=6)
        configure_combobox(genero_combo, self.generos)

        def on_save():
            nombre = nombre_var.get().strip().title()
            cedula = re.sub(r"\D", "", cedula_var.get().strip())
            payload = {
                "nombre_usuario": nombre,
                "cedula_usuario": cedula,
                "discapacidad_usuario": discapacidad_var.get().strip(),
                "genero_usuario": genero_var.get().strip(),
            }
            if not payload["nombre_usuario"] or not payload["cedula_usuario"]:
                messagebox.showerror("Error", "Nombre y cedula son obligatorios")
                return
            if payload["cedula_usuario"] in self.usuarios_by_cedula:
                messagebox.showerror(
                    "Error",
                    "La cedula ya existe en la base de datos. Seleccionala desde la lista.",
                )
                return
            try:
                data = self.api.post("/wizard/seccion-4/usuarios", payload)
            except Exception as exc:
                messagebox.showerror("Error", f"No se pudo crear el usuario: {exc}")
                return

            user = data["data"]
            self.state.usuarios_nuevos.append(user)
            if user["cedula_usuario"] not in self.cedulas:
                self.cedulas.append(user["cedula_usuario"])
                self.usuarios_by_cedula[user["cedula_usuario"]] = user
                self.rows = [r for r in self.rows if r.winfo_exists()]
                for row in self.rows:
                    row.cedula_combo.configure(values=self.cedulas)

            self.api.invalidate("/wizard/seccion-4/usuarios")

            row = target_row if target_row and target_row.winfo_exists() else None
            if row is None:
                row = self.rows[-1] if self.rows else None
                if row is None or row.cedula_var.get().strip():
                    self._add_row()
                    row = self.rows[-1]
            row.set_from_user(user)
            messagebox.showinfo("Exito", "Usuario guardado correctamente.")
            dialog.destroy()

        tk.Button(
            dialog,
            text="Guardar usuario",
            command=on_save,
            bg=COLOR_TEAL,
            fg="white",
            padx=10,
            pady=6,
        ).grid(row=4, column=0, columnspan=2, pady=16)

    def get_payload(self) -> dict:
        self.rows = [row for row in self.rows if row.winfo_exists()]
        personas = [row.as_payload() for row in self.rows if row.cedula_var.get().strip()]
        cedulas = [p["cedula_usuario"] for p in personas]
        duplicadas = {c for c in cedulas if cedulas.count(c) > 1}
        for row in self.rows:
            row.set_highlight(row.cedula_var.get().strip() in duplicadas)
        if duplicadas:
            listado = ", ".join(sorted(duplicadas))
            raise RuntimeError(
                f"Hay cedulas repetidas ({listado}). Elimina duplicados antes de continuar."
            )
        return {"personas": personas}


class Seccion5Frame(BaseSection):
    def __init__(self, parent, api: ApiClient):
        super().__init__(parent, "Seccion 5 - Observaciones")
        self.api = api
        self.fecha_servicio = ""
        self.obs_text = tk.Text(self.body, width=70, height=3)
        self.obs_agencia_text = tk.Text(self.body, width=70, height=3)
        self.seguimiento_text = tk.Text(self.body, width=70, height=3)

        ttk.Label(self.body, text="Observaciones").grid(row=0, column=0, sticky="w")
        self.obs_text.grid(row=1, column=0, sticky="w")
        ttk.Label(self.body, text="Observacion agencia").grid(row=2, column=0, sticky="w")
        self.obs_agencia_text.grid(row=3, column=0, sticky="w")
        ttk.Label(self.body, text="Seguimiento").grid(row=4, column=0, sticky="w")
        self.seguimiento_text.grid(row=5, column=0, sticky="w")

    def set_fecha_servicio(self, value: str) -> None:
        self.fecha_servicio = value

    def get_payload(self) -> dict:
        return {
            "fecha_servicio": self.fecha_servicio,
            "observaciones": self.obs_text.get("1.0", tk.END).strip(),
            "observacion_agencia": self.obs_agencia_text.get("1.0", tk.END).strip(),
            "seguimiento_servicio": self.seguimiento_text.get("1.0", tk.END).strip(),
        }

    def set_data(self, data: dict) -> None:
        self.obs_text.delete("1.0", tk.END)
        self.obs_text.insert("1.0", data.get("observaciones", "") or "")
        self.obs_agencia_text.delete("1.0", tk.END)
        self.obs_agencia_text.insert("1.0", data.get("observacion_agencia", "") or "")
        self.seguimiento_text.delete("1.0", tk.END)
        self.seguimiento_text.insert("1.0", data.get("seguimiento_servicio", "") or "")


class ResumenFrame(ttk.Frame):
    def __init__(self, parent, on_terminar, on_retry_queue=None, show_terminar: bool = True):
        super().__init__(parent)
        self.on_terminar = on_terminar
        self.vars = {
            "fecha_servicio": tk.StringVar(),
            "nombre_profesional": tk.StringVar(),
            "nombre_empresa": tk.StringVar(),
            "codigo_servicio": tk.StringVar(),
            "valor_total": tk.StringVar(),
        }

        ttk.Label(self, text="Resumen del servicio", font=("Arial", 12, "bold")).grid(
            row=0, column=0, columnspan=2, sticky="w", pady=(8, 4)
        )
        row = 1
        for label, key in [
            ("Fecha", "fecha_servicio"),
            ("Profesional", "nombre_profesional"),
            ("Empresa", "nombre_empresa"),
            ("Codigo", "codigo_servicio"),
            ("Valor total", "valor_total"),
        ]:
            ttk.Label(self, text=label).grid(row=row, column=0, sticky="w")
            ttk.Label(self, textvariable=self.vars[key]).grid(row=row, column=1, sticky="w")
            row += 1

        self._terminar_btn = tk.Button(
            self,
            text="Confirmar informacion y terminar",
            command=self.on_terminar,
            bg=COLOR_PURPLE,
            fg="white",
            font=("Arial", 10, "bold"),
            padx=12,
            pady=6,
        )
        if show_terminar:
            self._terminar_btn.grid(row=row, column=0, pady=(10, 8), sticky="w")

        row += 1
        self.queue_label = ttk.Label(self, text="", foreground=COLOR_TEAL)
        self.queue_label.grid(row=row, column=0, columnspan=2, sticky="w")
        row += 1
        if on_retry_queue:
            tk.Button(
                self,
                text="Reintentar cola Excel",
                command=on_retry_queue,
                bg=COLOR_TEAL,
                fg="white",
                padx=10,
                pady=4,
            ).grid(row=row, column=0, pady=(6, 0), sticky="w")

    def update_summary(self, data: dict) -> None:
        for key, var in self.vars.items():
            value = data.get(key, "")
            if key == "valor_total":
                if isinstance(value, str) and value.strip().startswith("$"):
                    pass
                else:
                    value = format_currency(value)
            var.set(value)
class WizardApp:
    def __init__(self, root: tk.Tk, api: ApiClient):
        self.root = root
        self.api = api
        self.state = WizardState()
        self._summary_after_id: str | None = None
        self.edit_entry_id: str | None = None
        self.edit_original_entry: dict | None = None

        self.root.title("SISTEMA DE GESTIÓN ODS - RECA")
        self._set_window_size()

        self.header = tk.Frame(self.root, bg=COLOR_PURPLE, height=70)
        self.header.pack(fill=tk.X)
        self.header.grid_columnconfigure(0, weight=0)
        self.header.grid_columnconfigure(1, weight=1)
        self.header.grid_columnconfigure(2, weight=0)
        self.header_logo = _load_logo(subsample=8)
        if self.header_logo:
            tk.Label(self.header, image=self.header_logo, bg=COLOR_PURPLE).grid(
                row=0, column=0, padx=(16, 6), pady=8, sticky="w"
            )
        tk.Label(
            self.header,
            text="SISTEMA DE GESTIÓN ODS - RECA",
            font=("Arial", 22, "bold"),
            bg=COLOR_PURPLE,
            fg="white",
        ).grid(row=0, column=1, pady=10)
        tk.Frame(self.header, bg=COLOR_PURPLE).grid(row=0, column=2, padx=16)

        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=16)

        self.show_initial_screen()

    def show_initial_screen(self) -> None:
        for child in self.main_frame.winfo_children():
            child.destroy()

        ttk.Label(
            self.main_frame,
            text="Seleccione una opcion para iniciar",
            font=("Arial", 14, "bold"),
            foreground=COLOR_PURPLE,
        ).pack(pady=20)

        tk.Button(
            self.main_frame,
            text="Crear nueva entrada",
            command=self.start_new_service,
            bg=COLOR_TEAL,
            fg="white",
            font=("Arial", 12, "bold"),
            padx=16,
            pady=8,
        ).pack(pady=8)

        tk.Button(
            self.main_frame,
            text="Editar entrada existente",
            command=self.show_edit_search_screen,
            bg=COLOR_PURPLE,
            fg="white",
            font=("Arial", 12, "bold"),
            padx=16,
            pady=8,
        ).pack(pady=8)

        tk.Button(
            self.main_frame,
            text="Reconstruir Excel desde Supabase",
            command=self._rebuild_excel_from_supabase,
            bg="#C0392B",
            fg="white",
            font=("Arial", 12, "bold"),
            padx=16,
            pady=8,
        ).pack(pady=8)

    def _not_ready(self) -> None:
        messagebox.showinfo("Info", "Funcion de edicion aun no implementada")

    def _rebuild_excel_from_supabase(self) -> None:
        confirm = messagebox.askyesno(
            "Reconstruir Excel",
            "¿Estas seguro de que deseas reconstruir el Excel desde Supabase?",
        )
        if not confirm:
            return
        confirm2 = messagebox.askyesno(
            "Confirmacion final",
            "Este proceso BORRARA el contenido actual del Excel y lo recreara desde Supabase.\n"
            "Si el Excel tenia cambios no guardados, se perderan.\n"
            "¿Deseas continuar?",
        )
        if not confirm2:
            return
        loading = LoadingDialog(self.root, "Reconstruyendo Excel...")
        self.root.update_idletasks()
        try:
            response = self.api.post("/wizard/editar/excel/rebuild", {}, timeout=300)
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo reconstruir el Excel: {exc}")
            return
        loading.close()
        data = response.get("data", {})
        backup = data.get("backup") or "No aplica"
        messagebox.showinfo(
            "Reconstruccion completa",
            f"Filas recreadas: {data.get('rows', 0)}\nBackup: {backup}",
        )
        self._update_queue_status()

    def _set_window_size(self) -> None:
        self.root.update_idletasks()
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        width = max(1100, int(screen_w * 0.9))
        height = max(800, int(screen_h * 0.9))
        self.root.geometry(f"{width}x{height}+0+0")
        if sys.platform.startswith("win"):
            try:
                self.root.state("zoomed")
            except tk.TclError:
                pass

    def show_edit_search_screen(self) -> None:
        for child in self.main_frame.winfo_children():
            child.destroy()

        header = ttk.Label(
            self.main_frame,
            text="Buscar entrada existente",
            font=("Arial", 14, "bold"),
            foreground=COLOR_PURPLE,
        )
        header.pack(pady=12)

        search_frame = ttk.Frame(self.main_frame)
        search_frame.pack(fill=tk.X, padx=12, pady=8)

        self.edit_filters = {
            "nombre_profesional": tk.StringVar(),
            "nit_empresa": tk.StringVar(),
            "fecha_servicio": tk.StringVar(),
            "codigo_servicio": tk.StringVar(),
        }

        ttk.Label(search_frame, text="Profesional").grid(row=0, column=0, sticky="w")
        ttk.Entry(search_frame, textvariable=self.edit_filters["nombre_profesional"], width=24).grid(
            row=0, column=1, sticky="w", padx=(0, 10)
        )
        ttk.Label(search_frame, text="NIT").grid(row=0, column=2, sticky="w")
        ttk.Entry(search_frame, textvariable=self.edit_filters["nit_empresa"], width=16).grid(
            row=0, column=3, sticky="w", padx=(0, 10)
        )

        ttk.Label(search_frame, text="Fecha servicio (YYYY-MM-DD)").grid(row=1, column=0, sticky="w")
        ttk.Entry(search_frame, textvariable=self.edit_filters["fecha_servicio"], width=16).grid(
            row=1, column=1, sticky="w", padx=(0, 10)
        )
        ttk.Label(search_frame, text="Codigo servicio").grid(row=1, column=2, sticky="w")
        ttk.Entry(search_frame, textvariable=self.edit_filters["codigo_servicio"], width=16).grid(
            row=1, column=3, sticky="w", padx=(0, 10)
        )

        tk.Button(
            search_frame,
            text="Buscar",
            command=self._buscar_entradas,
            bg=COLOR_TEAL,
            fg="white",
            padx=12,
            pady=4,
        ).grid(row=1, column=5, sticky="w")

        tk.Button(
            search_frame,
            text="Volver",
            command=self.show_initial_screen,
            bg=COLOR_PURPLE,
            fg="white",
            padx=12,
            pady=4,
        ).grid(row=1, column=6, sticky="w", padx=(10, 0))

        self.results_container = ttk.Frame(self.main_frame)
        self.results_container.pack(fill=tk.BOTH, expand=True, padx=12, pady=8)

        self._edit_results_map: dict[str, dict] = {}
        self._edit_tree: ttk.Treeview | None = None

        self.selected_entry_label = ttk.Label(
            self.main_frame,
            text="",
            font=("Arial", 10, "bold"),
            foreground=COLOR_PURPLE,
        )
        self.selected_entry_label.pack(pady=(4, 2))
        self.queue_status_label = ttk.Label(
            self.main_frame,
            text="",
            font=("Arial", 9),
            foreground=COLOR_TEAL,
        )
        self.queue_status_label.pack(pady=(0, 2))
        self.confirm_button = tk.Button(
            self.main_frame,
            text="Cargar entrada seleccionada",
            command=self._confirmar_carga_edicion,
            bg=COLOR_PURPLE,
            fg="white",
            padx=14,
            pady=6,
            state="disabled",
        )
        self.confirm_button.pack(pady=(0, 8))

        tk.Button(
            self.main_frame,
            text="Reintentar cola Excel",
            command=self._flush_excel_queue,
            bg=COLOR_TEAL,
            fg="white",
            padx=12,
            pady=4,
        ).pack(pady=(0, 8))

        self._flush_excel_queue()

    def _buscar_entradas(self) -> None:
        params = {}
        for key, var in self.edit_filters.items():
            value = var.get().strip()
            if value:
                params[key] = value
        if not params:
            messagebox.showerror("Error", "Debes llenar al menos un filtro")
            return

        loading = LoadingDialog(self.root, "Buscando entradas...", determinate=True)
        self.root.update_idletasks()
        loading.set_status("Preparando filtros...", 25)
        try:
            loading.set_status("Consultando ODS...", 70)
            data = self.api.get("/wizard/editar/buscar", params=params)
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo buscar: {exc}")
            return
        loading.set_status("Renderizando resultados...", 90)
        loading.set_status("Listo...", 100)
        loading.close()
        self._render_results(data.get("data", []))

    def _render_results(self, results: list[dict]) -> None:
        for child in self.results_container.winfo_children():
            child.destroy()

        if not results:
            ttk.Label(self.results_container, text="No se encontraron resultados.").pack()
            self.confirm_button.configure(state="disabled")
            self.selected_entry_label.config(text="")
            self.edit_entry_id = None
            self._edit_results_map = {}
            return

        columns = (
            "id",
            "fecha",
            "profesional",
            "empresa",
            "codigo",
            "nit",
            "accion",
        )
        tree = ttk.Treeview(self.results_container, columns=columns, show="headings", height=10)
        tree.heading("id", text="ID")
        tree.heading("fecha", text="Fecha")
        tree.heading("profesional", text="Profesional")
        tree.heading("empresa", text="Empresa")
        tree.heading("codigo", text="Codigo")
        tree.heading("nit", text="NIT")
        tree.heading("accion", text="")

        tree.column("id", width=60, anchor="w")
        tree.column("fecha", width=110, anchor="w")
        tree.column("profesional", width=170, anchor="w")
        tree.column("empresa", width=170, anchor="w")
        tree.column("codigo", width=90, anchor="w")
        tree.column("nit", width=120, anchor="w")
        tree.column("accion", width=110, anchor="center")

        scrollbar = ttk.Scrollbar(self.results_container, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)
        tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self._edit_results_map = {}
        for row in results:
            values = (
                row.get("id", ""),
                row.get("fecha_servicio", ""),
                row.get("nombre_profesional", ""),
                row.get("nombre_empresa", ""),
                row.get("codigo_servicio", ""),
                row.get("nit_empresa", ""),
                "Seleccionar",
            )
            item_id = tree.insert("", "end", values=values)
            self._edit_results_map[item_id] = row

        def on_click(event):
            item = tree.identify_row(event.y)
            column = tree.identify_column(event.x)
            if not item:
                return
            if column == f"#{len(columns)}":
                self._seleccionar_resultado(self._edit_results_map.get(item, {}))

        def on_double_click(_event):
            selection = tree.selection()
            if selection:
                self._seleccionar_resultado(self._edit_results_map.get(selection[0], {}))

        tree.bind("<ButtonRelease-1>", on_click)
        tree.bind("<Double-1>", on_double_click)
        self._edit_tree = tree

    def _seleccionar_resultado(self, row: dict) -> None:
        self.edit_entry_id = row.get("id")
        self.edit_original_entry = None
        resumen = (
            f"Seleccionado ID {row.get('id')} | "
            f"{row.get('fecha_servicio')} | "
            f"{row.get('nombre_profesional')} | "
            f"{row.get('codigo_servicio')}"
        )
        self.selected_entry_label.config(text=resumen)
        self.confirm_button.configure(state="normal")
        self._confirmar_carga_edicion()

    def _confirmar_carga_edicion(self) -> None:
        if not self.edit_entry_id:
            return
        confirm = messagebox.askyesno(
            "Confirmar",
            f"Deseas cargar la entrada ID {self.edit_entry_id}?",
        )
        if not confirm:
            return

        loading = LoadingDialog(self.root, "Cargando entrada...", determinate=True)
        self.root.update_idletasks()
        loading.set_status("Consultando ODS...", 60)
        try:
            data = self.api.get("/wizard/editar/entrada", params={"id": self.edit_entry_id})
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo cargar la entrada: {exc}")
            return
        loading.set_status("Aplicando datos al formulario...", 90)
        loading.set_status("Preparando formulario...", 95)
        loading.close()
        entry = data.get("data", {})
        self.edit_original_entry = entry
        self._start_edit_service(entry)

    def start_new_service(self) -> None:
        try:
            for child in self.main_frame.winfo_children():
                child.destroy()

            self.scroll = ScrollableFrame(self.main_frame)
            self.scroll.pack(fill=tk.BOTH, expand=True)
            self.scroll.content.grid_columnconfigure(0, weight=1)
            self.scroll.content.grid_columnconfigure(1, weight=1)

            left_col = ttk.Frame(self.scroll.content)
            left_col.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            right_col = ttk.Frame(self.scroll.content)
            right_col.grid(row=0, column=1, sticky="nsew")
            left_col.grid_columnconfigure(0, weight=1)
            right_col.grid_columnconfigure(0, weight=1)

            self.seccion1 = Seccion1Frame(left_col, self.api, self.state)
            self.seccion1.grid(row=0, column=0, sticky="ew", pady=8)

            self.seccion2 = Seccion2Frame(left_col, self.api)
            self.seccion2.grid(row=1, column=0, sticky="ew", pady=8)

            self.seccion3 = Seccion3Frame(left_col, self.api)
            self.seccion3.grid(row=2, column=0, sticky="ew", pady=8)

            self.seccion4 = Seccion4Frame(right_col, self.api, self.state)
            self.seccion4.grid(row=0, column=0, sticky="ew", pady=8)

            self.seccion5 = Seccion5Frame(right_col, self.api)
            self.seccion5.grid(row=1, column=0, sticky="ew", pady=8)

            self.resumen = ResumenFrame(right_col, self.terminar_servicio, self._flush_excel_queue)
            self.resumen.grid(row=2, column=0, sticky="ew", pady=8)

            self._load_section_data()
            self._lock_sections()
            self._bind_summary_updates()
            self._refresh_summary()
            self._update_queue_status()
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo iniciar el formulario: {exc}")

    def _start_edit_service(self, entry: dict) -> None:
        try:
            for child in self.main_frame.winfo_children():
                child.destroy()

            self.scroll = ScrollableFrame(self.main_frame)
            self.scroll.pack(fill=tk.BOTH, expand=True)
            self.scroll.content.grid_columnconfigure(0, weight=1)
            self.scroll.content.grid_columnconfigure(1, weight=1)

            left_col = ttk.Frame(self.scroll.content)
            left_col.grid(row=0, column=0, sticky="nsew", padx=(0, 12))
            right_col = ttk.Frame(self.scroll.content)
            right_col.grid(row=0, column=1, sticky="nsew")
            left_col.grid_columnconfigure(0, weight=1)
            right_col.grid_columnconfigure(0, weight=1)

            self.seccion1 = Seccion1Frame(left_col, self.api, self.state)
            self.seccion1.grid(row=0, column=0, sticky="ew", pady=8)

            self.seccion2 = Seccion2Frame(left_col, self.api)
            self.seccion2.grid(row=1, column=0, sticky="ew", pady=8)

            self.seccion3 = Seccion3Frame(left_col, self.api)
            self.seccion3.grid(row=2, column=0, sticky="ew", pady=8)

            self.seccion4 = Seccion4Frame(right_col, self.api, self.state)
            self.seccion4.grid(row=0, column=0, sticky="ew", pady=8)

            self.seccion5 = Seccion5Frame(right_col, self.api)
            self.seccion5.grid(row=1, column=0, sticky="ew", pady=8)

            self.resumen = ResumenFrame(right_col, self.terminar_servicio, self._flush_excel_queue, show_terminar=False)
            self.resumen.grid(row=2, column=0, sticky="ew", pady=8)

            self.edit_actions = ttk.Frame(self.scroll.content)
            self.edit_actions.grid(row=1, column=0, columnspan=2, sticky="w", pady=8)
            tk.Button(
                self.edit_actions,
                text="Actualizar entrada",
                command=self.actualizar_entrada,
                bg=COLOR_TEAL,
                fg="white",
                padx=12,
                pady=4,
            ).pack(side="left", padx=(0, 10))
            tk.Button(
                self.edit_actions,
                text="Eliminar entrada",
                command=self.eliminar_entrada,
                bg="#C62828",
                fg="white",
                padx=12,
                pady=4,
            ).pack(side="left", padx=(0, 10))
            tk.Button(
                self.edit_actions,
                text="Volver",
                command=self.show_initial_screen,
                bg=COLOR_PURPLE,
                fg="white",
                padx=12,
                pady=4,
            ).pack(side="left")

            self._load_section_data()
            self._lock_sections()
            self._bind_summary_updates()

            self.seccion1.set_data(entry)
            self.seccion2.set_data(entry)
            self.seccion3.set_data(entry)
            self.seccion4.set_data(entry)
            self.seccion5.set_data(entry)
            self.seccion5.set_fecha_servicio(entry.get("fecha_servicio", ""))
            self._refresh_summary()
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo cargar el formulario: {exc}")

    def _flush_excel_queue(self) -> None:
        loading = LoadingDialog(self.root, "Reintentando cola Excel...", determinate=True)
        self.root.update_idletasks()
        loading.set_status("Procesando pendientes...", 60)
        try:
            response = self.api.post("/wizard/editar/excel/flush", {})
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo reintentar la cola: {exc}")
            return
        loading.set_status("Actualizando estado...", 90)
        loading.set_status("Finalizando...", 100)
        loading.close()
        data = response.get("data", {})
        if hasattr(self, "queue_status_label"):
            self.queue_status_label.config(
                text=f"Cola Excel: procesados {data.get('procesados', 0)} / pendientes {data.get('pendientes', 0)}"
            )
        self._update_queue_status()

    def _update_queue_status(self) -> None:
        try:
            data = self.api.get("/wizard/editar/excel/status").get("data", {})
        except Exception:
            return
        pendientes = int(data.get("pendientes", 0) or 0)
        locked = bool(data.get("locked"))
        if self.resumen and getattr(self.resumen, "queue_label", None):
            status = f"Cola Excel: {pendientes} pendiente(s)"
            if locked:
                status += " (Excel en uso)"
            self.resumen.queue_label.config(text=status)

    def _load_section_data(self) -> None:
        self.seccion1.load_data()
        self.seccion2.load_data()
        self.seccion3.load_data()
        self.seccion4.load_data()

    def _lock_sections(self) -> None:
        self.seccion1.set_enabled(True)
        self.seccion2.set_enabled(True)
        self.seccion3.set_enabled(True)
        self.seccion4.set_enabled(True)
        self.seccion5.set_enabled(True)
        self.resumen.grid()

    def _bind_summary_updates(self) -> None:
        def update(*_args):
            if self._summary_after_id is not None:
                self.root.after_cancel(self._summary_after_id)
            self._summary_after_id = self.root.after(300, self._refresh_summary)

        self.seccion1.prof_var.trace_add("write", update)
        self.seccion2.nombre_var.trace_add("write", update)
        self.seccion3.fecha_var.trace_add("write", update)
        self.seccion3.codigo_var.trace_add("write", update)
        self.seccion3.horas_var.trace_add("write", update)
        self.seccion3.minutos_var.trace_add("write", update)
        self.seccion3.interpretacion_var.trace_add("write", update)
        self.seccion3.total_calculado_var.trace_add("write", update)

    def _refresh_summary(self) -> None:
        self._summary_after_id = None
        self.seccion3.ensure_tarifa_loaded()
        data = {
            "fecha_servicio": self.seccion3.fecha_var.get().strip(),
            "nombre_profesional": self.seccion1.prof_var.get().strip(),
            "nombre_empresa": self.seccion2.nombre_var.get().strip(),
            "codigo_servicio": self.seccion3.codigo_var.get().strip(),
            "valor_total": "",
        }
        if self.seccion3.interpretacion_var.get():
            data["valor_total"] = self.seccion3.total_calculado_var.get()
        else:
            base = float(self.seccion3.valor_base_var.get() or 0)
            data["valor_total"] = base if base else ""
        self.resumen.update_summary(data)
        self._update_queue_status()

    def _build_ods_payload(self) -> dict:
        ods = {}
        for key in ["seccion1", "seccion2", "seccion3", "seccion4", "seccion5"]:
            ods.update(self.state.secciones.get(key, {}))
        return ods

    def _validar_secciones(self) -> dict:
        self.seccion3.ensure_tarifa_loaded()
        seccion1 = self.api.post("/wizard/seccion-1/confirmar", self.seccion1.get_payload())["data"]
        seccion2 = self.api.post("/wizard/seccion-2/confirmar", self.seccion2.get_payload())["data"]
        seccion3_payload = self.seccion3.get_payload()
        modalidad_txt = seccion3_payload.get("modalidad_servicio", "").lower()
        if "toda" in modalidad_txt and "modalidad" in modalidad_txt:
            self.seccion3.modalidad_var.set("Todas las modalidades")
            seccion3_payload = self.seccion3.get_payload()
        elif "fuera" in modalidad_txt or "otro" in modalidad_txt:
            self.seccion3.modalidad_var.set("Fuera de Bogotá")
            seccion3_payload = self.seccion3.get_payload()
        elif "bogota" in modalidad_txt:
            self.seccion3.modalidad_var.set("Bogotá")
            seccion3_payload = self.seccion3.get_payload()
        elif "virtual" in modalidad_txt:
            self.seccion3.modalidad_var.set("Virtual")
            seccion3_payload = self.seccion3.get_payload()
        if (
            seccion3_payload.get("servicio_interpretacion")
            and not seccion3_payload.get("modalidad_servicio")
        ):
            self.seccion3.modalidad_var.set("Todas las modalidades")
            seccion3_payload = self.seccion3.get_payload()
        if not seccion3_payload.get("modalidad_servicio") and seccion3_payload.get("codigo_servicio"):
            tarifa = self.api.get(
                "/wizard/seccion-3/tarifa",
                params={"codigo": seccion3_payload["codigo_servicio"]},
            )
            if tarifa["data"]:
                item = tarifa["data"][0]
                self.seccion3.referencia_var.set(item.get("referencia_servicio", ""))
                self.seccion3.descripcion_var.set(item.get("descripcion_servicio", ""))
                self.seccion3.modalidad_var.set(item.get("modalidad_servicio", ""))
                valor_base = item.get("valor_base", "")
                self.seccion3.valor_base_var.set(str(valor_base))
                self.seccion3.valor_base_display_var.set(format_currency(valor_base))
                seccion3_payload = self.seccion3.get_payload()
        seccion3 = self.api.post("/wizard/seccion-3/confirmar", seccion3_payload)["data"]
        seccion4 = self.api.post("/wizard/seccion-4/confirmar", self.seccion4.get_payload())["data"]
        seccion5_payload = self.seccion5.get_payload()
        seccion5_payload["fecha_servicio"] = seccion3_payload["fecha_servicio"]
        seccion5 = self.api.post("/wizard/seccion-5/confirmar", seccion5_payload)["data"]

        self.state.secciones = {
            "seccion1": seccion1,
            "seccion2": seccion2,
            "seccion3": seccion3,
            "seccion4": seccion4,
            "seccion5": seccion5,
        }
        return self._build_ods_payload()

    def terminar_servicio(self) -> None:
        try:
            status = self.api.get("/wizard/editar/excel/status").get("data", {})
        except Exception:
            status = {}
        if int(status.get("pendientes", 0) or 0) > 0:
            messagebox.showwarning(
                "Pendientes en Excel",
                "Hay registros pendientes por escribir en Excel. "
                "Usa 'Reintentar cola Excel' y espera a que termine antes de guardar otro servicio.",
            )
            return
        if status.get("locked"):
            messagebox.showwarning(
                "Excel en uso",
                "El archivo Excel esta abierto. Cierralo para continuar con el guardado.",
            )
            return
        loading = LoadingDialog(self.root, "Preparando servicio...")
        self.root.update_idletasks()
        try:
            ods = self._validar_secciones()
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo validar la informacion: {exc}")
            return

        try:
            resumen = self.api.post("/wizard/resumen-final", {"ods": ods}, timeout=30)["data"]
            self.resumen.update_summary(resumen)
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo generar el resumen: {exc}")
            return
        loading.close()

        continuar = messagebox.askyesno(
            "Confirmar servicio",
            "Resumen generado. Deseas terminar y guardar el servicio?",
        )
        if not continuar:
            return

        loading = LoadingDialog(self.root, "Guardando servicio en la base de datos...")
        self.root.update_idletasks()
        loading.set_status("Guardando servicio...", None)
        payload = {"ods": ods, "usuarios_nuevos": self.state.usuarios_nuevos}
        try:
            response = self.api.post("/wizard/terminar-servicio", payload, timeout=120)
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo terminar el servicio: {exc}")
            return
        loading.close()
        if response.get("excel_status") == "background":
            messagebox.showinfo(
                "Servicio guardado",
                "Servicio guardado. El Excel y la factura se actualizan en segundo plano.",
            )
        elif response.get("excel_status") == "pendiente":
            messagebox.showwarning(
                "Aviso",
                "El archivo Excel estaba abierto. Se guardo la fila en cola.",
            )
        elif response.get("excel_status") == "error":
            messagebox.showwarning(
                "Aviso",
                f"No se pudo actualizar el Excel: {response.get('excel_error')}",
            )

        add_another = messagebox.askyesno(
            "Servicio terminado",
            "Servicio guardado. Deseas agregar otro servicio?",
        )
        if add_another:
            self.start_new_service()
        else:
            self.show_initial_screen()

    def actualizar_entrada(self) -> None:
        if not self.edit_entry_id:
            messagebox.showerror("Error", "No hay una entrada seleccionada.")
            return
        loading = LoadingDialog(self.root, "Preparando actualizacion...")
        self.root.update_idletasks()
        try:
            ods = self._validar_secciones()
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo validar la informacion: {exc}")
            return
        cambios = []
        if self.edit_original_entry:
            for key, value in ods.items():
                original_value = self.edit_original_entry.get(key)
                if isinstance(value, float):
                    value = round(value, 4)
                if isinstance(original_value, float):
                    original_value = round(original_value, 4)
                if str(value) != str(original_value):
                    cambios.append(key)
        loading.close()
        if cambios:
            detalle = ", ".join(sorted(cambios))
            confirmar = messagebox.askyesno(
                "Confirmar cambios",
                f"Se actualizaran {len(cambios)} campos:\n{detalle}\n\nDeseas continuar?",
            )
            if not confirmar:
                return
        else:
            messagebox.showinfo("Sin cambios", "No hay cambios detectados.")
            return
        loading = LoadingDialog(self.root, "Actualizando entrada...")
        self.root.update_idletasks()
        payload = {"filtro": {"id": self.edit_entry_id}, "datos": ods}
        if self.edit_original_entry:
            payload["original"] = self.edit_original_entry
        try:
            response = self.api.post("/wizard/editar/actualizar", payload, timeout=120)
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo actualizar: {exc}")
            return
        loading.close()
        cambios = response.get("cambios", [])
        messagebox.showinfo(
            "Actualizado",
            "Entrada actualizada. Campos cambiados: " + (", ".join(cambios) if cambios else "ninguno"),
        )
        if response.get("excel_status") == "background":
            messagebox.showinfo(
                "Actualizado",
                "Entrada actualizada. El Excel y la factura se actualizan en segundo plano.",
            )
        elif response.get("excel_status") == "pendiente":
            messagebox.showwarning(
                "Aviso",
                "El archivo Excel estaba abierto. Se guardo la actualizacion en cola.",
            )
        elif response.get("excel_status") == "error":
            messagebox.showwarning(
                "Aviso",
                f"No se pudo actualizar el Excel: {response.get('excel_error')}",
            )

    def eliminar_entrada(self) -> None:
        if not self.edit_entry_id:
            messagebox.showerror("Error", "No hay una entrada seleccionada.")
            return
        confirm = messagebox.askyesno(
            "Eliminar",
            f"Seguro que deseas eliminar la entrada ID {self.edit_entry_id}?",
        )
        if not confirm:
            return
        loading = LoadingDialog(self.root, "Eliminando entrada...")
        self.root.update_idletasks()
        payload = {"filtro": {"id": self.edit_entry_id}}
        if self.edit_original_entry:
            payload["original"] = self.edit_original_entry
        try:
            response = self.api.post("/wizard/editar/eliminar", payload, timeout=120)
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo eliminar: {exc}")
            return
        loading.close()
        messagebox.showinfo("Eliminado", "Entrada eliminada correctamente.")
        if response.get("excel_status") == "background":
            messagebox.showinfo(
                "Eliminado",
                "Entrada eliminada. El Excel y la factura se actualizan en segundo plano.",
            )
        elif response.get("excel_status") == "pendiente":
            messagebox.showwarning(
                "Aviso",
                "El archivo Excel estaba abierto. Se guardo la eliminacion en cola.",
            )
        elif response.get("excel_status") == "error":
            messagebox.showwarning(
                "Aviso",
                f"No se pudo actualizar el Excel: {response.get('excel_error')}",
            )
        self.show_initial_screen()


def main() -> None:
    if "--backend" in sys.argv:
        host = "127.0.0.1"
        port = "8123"
        if "--host" in sys.argv:
            idx = sys.argv.index("--host")
            if idx + 1 < len(sys.argv):
                host = sys.argv[idx + 1]
        if "--port" in sys.argv:
            idx = sys.argv.index("--port")
            if idx + 1 < len(sys.argv):
                port = sys.argv[idx + 1]
        _run_backend_only(host, int(port))
        return
    root = tk.Tk()
    root.withdraw()
    splash = StartupSplash(root)
    try:
        splash.deiconify()
    except tk.TclError:
        pass
    splash.lift()
    splash.update_idletasks()
    splash.update()
    time.sleep(0.2)
    backend_url = _load_backend_url()
    start_time = time.time()
    try:
        _ensure_backend_running(backend_url, splash.set_status)
    except RuntimeError as exc:
        splash.close()
        messagebox.showerror("Error", str(exc))
        root.destroy()
        return
    api = ApiClient(backend_url)
    try:
        splash.set_status("Cargando datos iniciales...", 55)
        _prefetch_initial_data(api, splash.set_status)
    except Exception as exc:
        splash.close()
        messagebox.showerror("Error", f"No se pudieron cargar datos iniciales: {exc}")
        root.destroy()
        return
    elapsed = time.time() - start_time
    if elapsed < 1.0:
        time.sleep(1.0 - elapsed)
    splash.close()
    root.deiconify()
    default_font = tkfont.nametofont("TkDefaultFont")
    default_font.configure(size=13)
    root.option_add("*Font", default_font)
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        pass
    style.configure("TFrame", background="white")
    style.configure("TLabel", background="white")
    style.configure("TCombobox", padding=2)
    style.map("TEntry", fieldbackground=[("readonly", "white")], foreground=[("readonly", "black")])
    style.map("TCombobox", fieldbackground=[("readonly", "white")], foreground=[("readonly", "black")])
    style.configure("Highlight.TFrame", background="#FFF2CC")
    root.configure(bg="white")
    app = WizardApp(root, api)

    def _on_close():
        _stop_backend()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)

    def _run_update():
        try:
            from app.updater import check_and_update

            check_and_update()
        except Exception:
            pass

    threading.Thread(target=_run_update, daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    main()
