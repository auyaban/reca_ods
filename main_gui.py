import os
import queue
import re
import subprocess
import sys
import time
import webbrowser
import difflib
from collections import OrderedDict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import tkinter.font as tkfont

import threading

from app.domain.service_calculation import CalculoServicioInput, calcular_servicio
from app.logging_utils import LOGGER_GUI, get_file_logger, get_logger
from app.services.errors import ServiceError
from app.supabase_client import classify_supabase_error
from app.utils.text import normalize_search_text, normalize_text

COLOR_PURPLE = "#7C3D96"
COLOR_TEAL = "#07B499"
_LOGGER = get_logger(LOGGER_GUI)
_LOGO_PATH = None
_LOGO_CACHE: dict[int, tk.PhotoImage] = {}
_MAX_LOGO_CACHE_ENTRIES = 8
_MAX_API_CACHE_ENTRIES = 256
_DATE_ENTRY_CLS = None
_GLOBAL_STATE_LOCK = threading.RLock()


def _desktop_dir() -> Path:
    one_drive = os.getenv("OneDrive")
    if one_drive:
        for folder in ("Desktop", "Escritorio"):
            candidate = Path(one_drive) / folder
            if candidate.exists():
                return candidate
    for folder in ("Desktop", "Escritorio"):
        candidate = Path.home() / folder
        if candidate.exists():
            return candidate
    return Path.home()


_ODS_FLOW_LOG_FILE = _desktop_dir() / "log ods.log"
_ODS_FLOW_LOGGER = get_file_logger("reca.ods.flow", _ODS_FLOW_LOG_FILE, announce=True)


def log_and_show_error(exc: Exception, context: str, title: str = "Error") -> None:
    error_code = f"E-{int(time.time() * 1000) % 1_000_000:06d}"
    _LOGGER.exception("[%s] %s: %s", error_code, context, exc)
    category = classify_supabase_error(exc)
    if category == "auth":
        user_message = "No se pudo autenticar con la base de datos."
    elif category == "permission":
        user_message = "La aplicacion no tiene permisos para realizar esta operacion."
    elif category == "connectivity":
        user_message = "No se pudo conectar con Supabase en este momento."
    else:
        user_message = context
    messagebox.showerror(
        title,
        f"{user_message}\nIntenta nuevamente o contacta soporte.\nCodigo: {error_code}",
    )


def _get_date_entry() -> object | None:
    global _DATE_ENTRY_CLS
    with _GLOBAL_STATE_LOCK:
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
    with _GLOBAL_STATE_LOCK:
        if _LOGO_PATH is None:
            try:
                from app.paths import resource_path

                candidate = resource_path("logo/logo_reca.png")
                if candidate.exists():
                    _LOGO_PATH = str(candidate)
                else:
                    _LOGO_PATH = os.path.join(os.path.dirname(__file__), "logo", "logo_reca.png")
            except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
                _LOGGER.warning("No se pudo resolver ruta de logo via resource_path: %s", exc)
                _LOGO_PATH = os.path.join(os.path.dirname(__file__), "logo", "logo_reca.png")
        if not os.path.exists(_LOGO_PATH):
            return None
        if subsample not in _LOGO_CACHE:
            if len(_LOGO_CACHE) >= _MAX_LOGO_CACHE_ENTRIES:
                oldest_key = next(iter(_LOGO_CACHE))
                _LOGO_CACHE.pop(oldest_key, None)
            _LOGO_CACHE[subsample] = tk.PhotoImage(file=_LOGO_PATH).subsample(subsample)
        return _LOGO_CACHE[subsample]




INITIAL_PREFETCH_ITEMS: list[tuple[str, dict | None, str]] = [
    ("/wizard/seccion-1/orden-clausulada/opciones", None, "opciones de orden"),
    ("/wizard/seccion-1/profesionales", None, "profesionales"),
    ("/wizard/seccion-2/empresas", None, "empresas"),
    ("/wizard/seccion-3/tarifas", None, "tarifas"),
    ("/wizard/seccion-4/usuarios", None, "usuarios"),
    ("/wizard/seccion-4/discapacidades", None, "discapacidades"),
    ("/wizard/seccion-4/generos", None, "generos"),
    ("/wizard/seccion-4/contratos", None, "contratos"),
]


def _prefetch_initial_data(api: "ApiClient", status_callback=None) -> None:
    api.prefetch(INITIAL_PREFETCH_ITEMS, status_callback=status_callback)


class ApiClient:
    def __init__(self, _base_url: str | None = None) -> None:
        from app.services import wizard_service

        self._svc = wizard_service
        self._cache: OrderedDict[tuple[str, str, tuple[tuple[str, str], ...] | None], dict] = OrderedDict()

    def _cache_key(self, path: str, params: dict | None) -> tuple[str, str, tuple[tuple[str, str], ...] | None]:
        if params:
            normalized = tuple(sorted((str(k), str(v)) for k, v in params.items()))
        else:
            normalized = None
        return ("GET", path, normalized)

    def get(self, path: str, params: dict | None = None, use_cache: bool = False) -> dict:
        cache_key = self._cache_key(path, params) if use_cache else None
        if cache_key and cache_key in self._cache:
            self._cache.move_to_end(cache_key)
            return self._cache[cache_key]
        try:
            data = self._dispatch_get(path, params or {})
            if cache_key:
                self._cache[cache_key] = data
                self._cache.move_to_end(cache_key)
                while len(self._cache) > _MAX_API_CACHE_ENTRIES:
                    self._cache.popitem(last=False)
            return data
        except (
            RuntimeError,
            ValueError,
            TypeError,
            OSError,
            KeyError,
            IndexError,
            AttributeError,
            tk.TclError,
            ServiceError,
        ) as exc:
            detail = getattr(exc, "detail", None) or str(exc)
            raise RuntimeError(detail) from exc

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

    def build_cache(self, items: list[tuple[str, dict | None, str]], status_callback=None) -> dict:
        total = len(items)
        new_cache: OrderedDict[tuple[str, str, tuple[tuple[str, str], ...] | None], dict] = OrderedDict()
        for index, (path, params, label) in enumerate(items, start=1):
            if status_callback:
                progress = int((index / max(total, 1)) * 100)
                status_callback(f"Cargando {label}...", progress)
            data = self._dispatch_get(path, params or {})
            key = self._cache_key(path, params)
            new_cache[key] = data
            new_cache.move_to_end(key)
            while len(new_cache) > _MAX_API_CACHE_ENTRIES:
                new_cache.popitem(last=False)
        return new_cache

    def replace_cache(self, new_cache: dict) -> None:
        bounded: OrderedDict[tuple[str, str, tuple[tuple[str, str], ...] | None], dict] = OrderedDict()
        for key, value in new_cache.items():
            bounded[key] = value
            bounded.move_to_end(key)
            while len(bounded) > _MAX_API_CACHE_ENTRIES:
                bounded.popitem(last=False)
        self._cache = bounded

    def reset_runtime_caches(self) -> None:
        self._svc.reset_runtime_caches()

    def post(self, path: str, payload: dict | None = None, timeout: int | float = 10) -> dict:
        try:
            return self._dispatch_post(path, payload or {})
        except (
            RuntimeError,
            ValueError,
            TypeError,
            OSError,
            KeyError,
            IndexError,
            AttributeError,
            tk.TclError,
            ServiceError,
        ) as exc:
            detail = getattr(exc, "detail", None) or str(exc)
            raise RuntimeError(detail) from exc

    def _dispatch_get(self, path: str, params: dict) -> dict:
        if path == "/health":
            return {"status": "ok"}
        if path == "/wizard/seccion-1/orden-clausulada/opciones":
            return self._svc.get_orden_clausulada_opciones()
        if path == "/wizard/seccion-1/profesionales":
            return self._svc.get_profesionales(programa=params.get("programa"))
        if path == "/wizard/seccion-2/empresas":
            return self._svc.get_empresas()
        if path == "/wizard/seccion-2/empresa":
            return self._svc.get_empresa_por_nit(params.get("nit", ""))
        if path == "/wizard/seccion-3/tarifas":
            return self._svc.get_codigos_servicio()
        if path == "/wizard/seccion-3/tarifa":
            return self._svc.get_tarifa_por_codigo(params.get("codigo", ""))
        if path == "/wizard/seccion-4/usuarios":
            return self._svc.get_usuarios_reca()
        if path == "/wizard/seccion-4/usuario":
            return self._svc.get_usuario_por_cedula(params.get("cedula", ""))
        if path == "/wizard/seccion-4/usuarios/existe":
            return self._svc.verificar_usuario_existe(params.get("cedula", ""))
        if path == "/wizard/seccion-4/discapacidades":
            return self._svc.get_discapacidades()
        if path == "/wizard/seccion-4/generos":
            return self._svc.get_generos()
        if path == "/wizard/seccion-4/contratos":
            return self._svc.get_tipos_contrato()
        if path == "/wizard/actas-finalizadas":
            return self._svc.listar_actas_finalizadas(params)
        if path == "/wizard/actas-finalizadas/status":
            return self._svc.estado_actas_finalizadas()
        if path == "/wizard/google-drive/status":
            return self._svc.google_drive_status()
        raise RuntimeError(f"Endpoint no soportado: GET {path}")

    def _dispatch_post(self, path: str, payload: dict) -> dict:
        if path == "/wizard/seccion-1/confirmar":
            return self._svc.confirmar_seccion_1(payload)
        if path == "/wizard/seccion-1/profesionales":
            return self._svc.crear_profesional(payload)
        if path == "/wizard/seccion-2/confirmar":
            return self._svc.confirmar_seccion_2(payload)
        if path == "/wizard/seccion-3/confirmar":
            return self._svc.confirmar_seccion_3(payload)
        if path == "/wizard/seccion-4/confirmar":
            return self._svc.confirmar_seccion_4(payload)
        if path == "/wizard/seccion-4/usuarios":
            return self._svc.crear_usuario(payload)
        if path == "/wizard/seccion-5/confirmar":
            return self._svc.confirmar_seccion_5(payload)
        if path == "/wizard/resumen-final":
            return self._svc.resumen_final_servicio(payload)
        if path == "/wizard/terminar-servicio":
            return self._svc.terminar_servicio(payload)
        if path == "/wizard/google-drive/flush":
            return self._svc.google_drive_flush()
        if path == "/wizard/google-sheet-sync/preview":
            return self._svc.preview_google_sheet_supabase_sync(payload)
        if path == "/wizard/google-sheet-sync/apply":
            return self._svc.apply_google_sheet_supabase_sync(payload)
        if path == "/wizard/actas-finalizadas/revisado":
            return self._svc.actualizar_acta_revisado(payload)
        raise RuntimeError(f"Endpoint no soportado: POST {path}")


class ScrollableFrame(ttk.Frame):
    def __init__(self, parent) -> None:
        super().__init__(parent)
        self.canvas = tk.Canvas(self, highlightthickness=0, bg="white")
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.content = ttk.Frame(self.canvas)

        self.content.bind(
            "<Configure>",
            lambda _e: self.canvas.configure(scrollregion=self.canvas.bbox("all")),
        )

        self._content_window = self.canvas.create_window((0, 0), window=self.content, anchor="nw")
        self.canvas.configure(yscrollcommand=scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.canvas.bind("<Configure>", self._on_canvas_configure)

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
        try:
            if not self.canvas.winfo_exists():
                return
        except tk.TclError:
            _LOGGER.debug("Canvas ya no existe al procesar rueda del mouse.")
            return
        try:
            if event.num == 4:
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                self.canvas.yview_scroll(1, "units")
            else:
                delta = int(-1 * (event.delta / 120))
                self.canvas.yview_scroll(delta, "units")
        except tk.TclError:
            _LOGGER.debug("Error Tcl en desplazamiento de canvas; se ignora por widget destruido.")
            return

    def _on_canvas_configure(self, event) -> None:
        try:
            if self.canvas.winfo_exists():
                self.canvas.itemconfigure(self._content_window, width=event.width)
        except tk.TclError:
            _LOGGER.debug("Error Tcl al redimensionar canvas scrollable.")
            return


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
            _LOGGER.debug("No se pudo establecer splash como transient.")
        try:
            self.attributes("-topmost", True)
        except tk.TclError:
            _LOGGER.debug("No se pudo establecer splash topmost.")

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
            _LOGGER.debug("No se pudo aplicar tema 'clam' en StartupSplash.")
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
            _LOGGER.debug("No se pudo establecer loading dialog como transient.")
        try:
            self.attributes("-topmost", True)
        except tk.TclError:
            _LOGGER.debug("No se pudo establecer loading dialog topmost.")
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
            _LOGGER.debug("No se pudo aplicar tema 'clam' en LoadingDialog.")
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

    def set_mode(self, determinate: bool) -> None:
        if determinate == self._determinate:
            return
        if determinate:
            self.progress.stop()
            self.progress.config(mode="determinate", maximum=100)
            self.progress["value"] = 0
        else:
            self.progress.config(mode="indeterminate")
            self.progress.start(10)
        self._determinate = determinate


def set_widgets_state(container: tk.Widget, state: str) -> None:
    for child in container.winfo_children():
        try:
            current_state = str(child.cget("state"))
            if state == "normal" and current_state in ("readonly", "disabled"):
                pass
            else:
                child.configure(state=state)
        except tk.TclError:
            _LOGGER.debug("No se pudo cambiar estado de un widget (TclError).")
        set_widgets_state(child, state)


def configure_combobox(
    combo: ttk.Combobox, values: list[str] | None = None, allow_typing: bool = False
) -> None:
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

    if allow_typing:
        combo.bind("<KeyRelease>", on_keyrelease, add="+")

    combo.bind("<MouseWheel>", lambda _e: "break", add="+")
    combo.bind("<Button-4>", lambda _e: "break", add="+")
    combo.bind("<Button-5>", lambda _e: "break", add="+")


def format_currency(value) -> str:
    amount = safe_decimal(value)
    if amount == Decimal("0") and value not in (0, "0", 0.0, Decimal("0")):
        return ""
    rounded = int(amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    return f"$ {rounded:,}".replace(",", ".")


def safe_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_decimal(value) -> Decimal:
    try:
        if isinstance(value, str):
            clean = value.strip()
            if not clean:
                return Decimal("0")
            clean = clean.replace("$", "").replace(" ", "")
            if "." in clean and "," in clean:
                if clean.rfind(",") > clean.rfind("."):
                    clean = clean.replace(".", "").replace(",", ".")
                else:
                    clean = clean.replace(",", "")
            elif "." in clean:
                if clean.count(".") > 1 or re.search(r"\.\d{3}$", clean):
                    clean = clean.replace(".", "")
            elif "," in clean:
                if clean.count(",") > 1 or re.search(r",\d{3}$", clean):
                    clean = clean.replace(",", "")
                else:
                    clean = clean.replace(",", ".")
            return Decimal(clean)
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


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
        self._profesionales_meta_by_nombre: dict[str, dict] = {}
        ttk.Label(self.body, text="Orden Clausulada").grid(row=0, column=0, sticky="w")
        self.orden_combo = ttk.Combobox(self.body, textvariable=self.orden_var, state="readonly", width=20)
        self.orden_combo.grid(row=0, column=1, sticky="w")
        configure_combobox(self.orden_combo)

        ttk.Label(self.body, text="Profesional").grid(row=1, column=0, sticky="w")
        self.prof_combo = ttk.Combobox(self.body, textvariable=self.prof_var, state="readonly", width=40)
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

        prof_data = self.api.get_cached("/wizard/seccion-1/profesionales")
        self._apply_profesionales_data(prof_data["data"])

    def _apply_profesionales_data(self, rows: list[dict]) -> None:
        self._profesionales_meta_by_nombre = {}
        prof_labels: list[str] = []
        for item in rows or []:
            nombre = (item.get("nombre_profesional") or "").strip()
            if not nombre:
                continue
            prof_labels.append(nombre)
            self._profesionales_meta_by_nombre[nombre] = dict(item)
        self.prof_combo.configure(values=prof_labels)
        self.prof_combo._all_values = prof_labels

    def selected_profesional_is_interprete(self) -> bool:
        nombre = self.prof_var.get().strip()
        meta = self._profesionales_meta_by_nombre.get(nombre) or {}
        return bool(meta.get("es_interprete"))

    def reset_for_new_entry(self) -> None:
        self.orden_var.set("")
        self.prof_var.set("")

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
            state="readonly",
            width=20,
        )
        programa_combo.grid(row=1, column=1, padx=10, pady=8, sticky="w")
        programa_combo.set("Inclusión Laboral")

        def on_save():
            nombre_limpio = " ".join(nombre_var.get().split())
            programa_limpio = " ".join(programa_var.get().split())
            if not nombre_limpio:
                messagebox.showerror("Error", "Nombre profesional es obligatorio.")
                return
            if not programa_limpio:
                messagebox.showerror("Error", "Programa es obligatorio.")
                return

            loading = LoadingDialog(dialog, "Guardando profesional...", determinate=True)
            loading.set_status("Enviando datos...", 40)
            payload = {
                "nombre_profesional": nombre_limpio,
                "programa": programa_limpio,
            }
            try:
                data = self.api.post("/wizard/seccion-1/profesionales", payload)
                loading.set_status("Actualizando lista...", 80)
            except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
                loading.close()
                log_and_show_error(exc, "No se pudo guardar el profesional", title="Error")
                return

            nuevo = data.get("data")
            nombre = ""
            if isinstance(nuevo, dict):
                nombre = nuevo.get("nombre_profesional", "").strip()
            elif isinstance(nuevo, list) and nuevo:
                nombre = (nuevo[0].get("nombre_profesional") or "").strip()
            try:
                self.api.invalidate("/wizard/seccion-1/profesionales")
                prof_data = self.api.get("/wizard/seccion-1/profesionales")
                self._apply_profesionales_data(prof_data["data"])
            except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
                loading.close()
                log_and_show_error(exc, "El profesional se guardo, pero no se pudo refrescar la lista", title="Aviso")
                return
            finally:
                if loading.winfo_exists():
                    loading.close()

            if nombre:
                self.prof_var.set(nombre)
            messagebox.showinfo("Éxito", "Profesional guardado correctamente.")
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
        self._nombres: list[str] = []
        self._empresas_by_nit: dict[str, dict] = {}
        self._empresas_by_nombre: dict[str, list[dict]] = {}
        self._empresas: list[dict] = []
        self._is_updating = False

        ttk.Label(self.body, text="NIT Empresa").grid(row=0, column=0, sticky="w")
        self.nit_combo = ttk.Combobox(self.body, textvariable=self.nit_var, state="normal", width=30)
        self.nit_combo.grid(row=0, column=1, sticky="w")
        self.nit_combo.bind("<<ComboboxSelected>>", self._on_nit_selected)
        self.nit_combo.bind("<KeyRelease>", self._on_nit_typed, add="+")
        self.nit_combo.bind("<Return>", self._on_nit_confirm, add="+")
        self.nit_combo.bind("<FocusOut>", self._on_nit_confirm, add="+")
        configure_combobox(self.nit_combo, allow_typing=True)

        ttk.Label(self.body, text="Nombre Empresa").grid(row=1, column=0, sticky="w")
        self.nombre_combo = ttk.Combobox(self.body, textvariable=self.nombre_var, state="normal", width=50)
        self.nombre_combo.grid(row=1, column=1, sticky="w")
        self.nombre_combo.bind("<<ComboboxSelected>>", self._on_nombre_selected)
        self.nombre_combo.bind("<KeyRelease>", self._on_nombre_typed, add="+")
        self.nombre_combo.bind("<Return>", self._on_nombre_confirm, add="+")
        self.nombre_combo.bind("<FocusOut>", self._on_nombre_confirm, add="+")
        configure_combobox(self.nombre_combo, allow_typing=True)

        ttk.Label(self.body, text="Caja Compensacion").grid(row=2, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.caja_var, state="readonly", width=40).grid(
            row=2, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Asesor").grid(row=3, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.asesor_var, state="readonly", width=40).grid(
            row=3, column=1, sticky="w"
        )

        ttk.Label(self.body, text="Zona Compensar").grid(row=4, column=0, sticky="w")
        ttk.Entry(self.body, textvariable=self.sede_var, state="readonly", width=40).grid(
            row=4, column=1, sticky="w"
        )

    def load_data(self) -> None:
        data = self.api.get_cached("/wizard/seccion-2/empresas")
        self._empresas_by_nit = {}
        self._empresas_by_nombre = {}
        self._empresas = list(data["data"] or [])
        for item in self._empresas:
            nit = item.get("nit_empresa")
            if nit is None:
                continue
            nit_key = str(nit)
            self._empresas_by_nit[nit_key] = item
            nombre = (item.get("nombre_empresa") or "").strip()
            if nombre:
                key = self._normalize_nombre(nombre)
                self._empresas_by_nombre.setdefault(key, []).append(item)
        nits = list(self._empresas_by_nit.keys())
        self._nits = sorted(nits, key=lambda value: int(re.sub(r"\D", "", value) or 0))
        self.nit_combo.configure(values=self._nits)
        self.nit_combo._all_values = self._nits
        nombres = [item.get("nombre_empresa", "").strip() for item in self._empresas if item.get("nombre_empresa")]
        self._nombres = sorted(nombres, key=lambda value: value.lower())
        self.nombre_combo.configure(values=self._nombres)
        self.nombre_combo._all_values = self._nombres

    def reset_for_new_entry(self) -> None:
        self.nit_var.set("")
        self.nombre_var.set("")
        self.caja_var.set("")
        self.asesor_var.set("")
        self.sede_var.set("")
        self.nit_combo.configure(values=self._nits)
        self.nombre_combo.configure(values=self._nombres)

    def _normalize_nombre(self, nombre: str) -> str:
        return normalize_text(nombre, lowercase=True)

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
        if not nit:
            self.nit_combo.configure(values=self._nits)
            return
        selected_nit = nit if nit in self._empresas_by_nit else ""
        if not selected_nit:
            candidates = [item for item in self._nits if nit in item]
            if candidates:
                selected_nit = candidates[0]
        if not selected_nit:
            self.nit_var.set("")
            self.nombre_var.set("")
            self.caja_var.set("")
            self.asesor_var.set("")
            self.sede_var.set("")
            self.nit_combo.configure(values=self._nits)
            return
        self.nit_var.set(selected_nit)
        self._fetch_empresa(selected_nit)

    def _on_nombre_selected(self, _event) -> None:
        self._fetch_empresa_por_nombre(self.nombre_var.get())

    def _on_nombre_typed(self, _event) -> None:
        value = self._normalize_nombre(self.nombre_var.get())
        if not value:
            self.nombre_combo.configure(values=self._nombres)
            return
        filtered = [nombre for nombre in self._nombres if value in self._normalize_nombre(nombre)]
        self.nombre_combo.configure(values=filtered)

    def _on_nombre_confirm(self, _event) -> None:
        nombre = self.nombre_var.get().strip()
        if not nombre:
            self.nombre_combo.configure(values=self._nombres)
            return
        key = self._normalize_nombre(nombre)
        matched = next((item for item in self._nombres if self._normalize_nombre(item) == key), "")
        if not matched:
            candidates = [item for item in self._nombres if key in self._normalize_nombre(item)]
            if candidates:
                matched = candidates[0]
        if not matched:
            self.nombre_var.set("")
            self.nit_var.set("")
            self.caja_var.set("")
            self.asesor_var.set("")
            self.sede_var.set("")
            self.nombre_combo.configure(values=self._nombres)
            return
        self.nombre_var.set(matched)
        self._fetch_empresa_por_nombre(matched)

    def _fetch_empresa(self, nit: str) -> None:
        if not nit:
            return
        if self._is_updating:
            return
        self._is_updating = True
        empresa = self._empresas_by_nit.get(nit)
        if not empresa:
            data = self.api.get_cached("/wizard/seccion-2/empresa", params={"nit": nit})
            if not data["data"]:
                self.nombre_var.set("")
                self.caja_var.set("")
                self.asesor_var.set("")
                self.sede_var.set("")
                self._is_updating = False
                return
            empresa = data["data"][0]
            self._empresas_by_nit[nit] = empresa
            nombre = (empresa.get("nombre_empresa") or "").strip()
            if nombre:
                key = self._normalize_nombre(nombre)
                self._empresas_by_nombre.setdefault(key, []).append(empresa)
        if not empresa:
            self.nombre_var.set("")
            self.caja_var.set("")
            self.asesor_var.set("")
            self.sede_var.set("")
            self._is_updating = False
            return
        self.nombre_var.set(empresa.get("nombre_empresa", ""))
        self.caja_var.set(empresa.get("caja_compensacion", ""))
        self.asesor_var.set(empresa.get("asesor", ""))
        self.sede_var.set(empresa.get("zona_empresa", ""))
        nit_value = str(empresa.get("nit_empresa", "")).strip()
        if nit_value:
            self.nit_var.set(nit_value)
        self._is_updating = False

    def _fetch_empresa_por_nombre(self, nombre: str) -> None:
        if not nombre:
            return
        if self._is_updating:
            return
        self._is_updating = True
        key = self._normalize_nombre(nombre)
        matches = self._empresas_by_nombre.get(key) or []
        empresa = matches[0] if matches else None
        if not empresa:
            for item in self._empresas:
                if self._normalize_nombre(item.get("nombre_empresa", "")) == key:
                    empresa = item
                    break
        if not empresa:
            self.nombre_var.set("")
            self.caja_var.set("")
            self.asesor_var.set("")
            self.sede_var.set("")
            self._is_updating = False
            return
        self.nombre_var.set(empresa.get("nombre_empresa", ""))
        self.caja_var.set(empresa.get("caja_compensacion", ""))
        self.asesor_var.set(empresa.get("asesor", ""))
        self.sede_var.set(empresa.get("zona_empresa", ""))
        nit_value = str(empresa.get("nit_empresa", "")).strip()
        if nit_value:
            self.nit_var.set(nit_value)
        self._is_updating = False

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
    _MESES = [
        "Enero",
        "Febrero",
        "Marzo",
        "Abril",
        "Mayo",
        "Junio",
        "Julio",
        "Agosto",
        "Septiembre",
        "Octubre",
        "Noviembre",
        "Diciembre",
    ]
    _MES_TO_NUM = {mes: index for index, mes in enumerate(_MESES, start=1)}
    _ANIOS = ["2025", "2026", "2027"]

    def __init__(self, parent, api: ApiClient):
        super().__init__(parent, "Seccion 3 - Informacion del servicio")
        self.api = api
        self.fecha_dia_var = tk.StringVar()
        self.fecha_mes_var = tk.StringVar()
        self.fecha_ano_var = tk.StringVar()
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
        self._interprete_required = False
        self._codigos: list[str] = []
        self._tarifas_by_codigo: dict[str, dict] = {}
        self._last_codigo: str | None = None
        self._codigos_popup: tk.Toplevel | None = None

        ttk.Label(self.body, text="Fecha Servicio").grid(row=0, column=0, sticky="w")
        fecha_frame = ttk.Frame(self.body)
        fecha_frame.grid(row=0, column=1, sticky="w")
        ttk.Entry(fecha_frame, textvariable=self.fecha_dia_var, width=6).grid(row=0, column=0, sticky="w")
        self.fecha_mes_combo = ttk.Combobox(
            fecha_frame,
            textvariable=self.fecha_mes_var,
            values=self._MESES,
            state="readonly",
            width=14,
        )
        self.fecha_mes_combo.grid(row=0, column=1, sticky="w", padx=(6, 0))
        configure_combobox(self.fecha_mes_combo, self._MESES)
        self.fecha_ano_combo = ttk.Combobox(
            fecha_frame,
            textvariable=self.fecha_ano_var,
            values=self._ANIOS,
            state="readonly",
            width=8,
        )
        self.fecha_ano_combo.grid(row=0, column=2, sticky="w", padx=(6, 0))
        configure_combobox(self.fecha_ano_combo, self._ANIOS)

        ttk.Label(self.body, text="Codigo Servicio").grid(row=1, column=0, sticky="w")
        self.codigo_combo = ttk.Combobox(self.body, textvariable=self.codigo_var, state="normal", width=20)
        self.codigo_combo.grid(row=1, column=1, sticky="w")
        self.codigo_combo.bind("<<ComboboxSelected>>", self._on_codigo_selected)
        self.codigo_combo.bind("<KeyRelease>", self._on_codigo_typed, add="+")
        self.codigo_combo.bind("<Return>", self._on_codigo_confirm, add="+")
        self.codigo_combo.bind("<FocusOut>", self._on_codigo_confirm, add="+")
        configure_combobox(self.codigo_combo, allow_typing=True)
        tk.Button(
            self.body,
            text="Lista de codigos",
            command=self._open_codigos_popup,
            bg="#2E86C1",
            fg="white",
            padx=8,
            pady=2,
        ).grid(row=1, column=2, sticky="w", padx=(8, 0))

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

        self.interpretacion_check = ttk.Checkbutton(
            self.body,
            text="Servicio de interpretacion",
            variable=self.interpretacion_var,
            command=self._toggle_interprete,
        )
        self.interpretacion_check.grid(row=6, column=0, sticky="w", pady=(6, 0))

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

    @staticmethod
    def _decimal_to_float(value: Decimal, field_name: str) -> float:
        if not value.is_finite():
            raise ValueError(f"{field_name}: valor no finito")
        return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def get_fecha_servicio(self, *, validate: bool = False) -> str:
        dia_txt = self.fecha_dia_var.get().strip()
        mes_txt = self.fecha_mes_var.get().strip()
        ano_txt = self.fecha_ano_var.get().strip()
        if not dia_txt and not mes_txt and not ano_txt:
            return ""
        if not dia_txt or not mes_txt or not ano_txt:
            if validate:
                raise ValueError("Fecha servicio incompleta. Selecciona dia, mes y ano.")
            return ""
        if not dia_txt.isdigit():
            if validate:
                raise ValueError("Dia invalido. Debe ser un numero entre 1 y 31.")
            return ""
        dia = int(dia_txt)
        if dia < 1 or dia > 31:
            if validate:
                raise ValueError("Dia invalido. Debe estar entre 1 y 31.")
            return ""
        mes_num = self._MES_TO_NUM.get(mes_txt)
        if mes_num is None:
            if validate:
                raise ValueError("Mes invalido. Selecciona un mes de la lista.")
            return ""
        if ano_txt not in self._ANIOS:
            if validate:
                raise ValueError("Ano invalido. Selecciona 2025, 2026 o 2027.")
            return ""
        ano = int(ano_txt)
        try:
            fecha = date(ano, mes_num, dia)
        except ValueError as exc:
            if validate:
                raise ValueError("Fecha invalida. Verifica dia, mes y ano.") from exc
            return ""
        return fecha.isoformat()

    def set_fecha_servicio(self, value: str) -> None:
        raw = (value or "").strip()
        if not raw:
            self.fecha_dia_var.set("")
            self.fecha_mes_var.set("")
            self.fecha_ano_var.set("")
            return
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            self.fecha_dia_var.set("")
            self.fecha_mes_var.set("")
            self.fecha_ano_var.set("")
            return
        self.fecha_dia_var.set(str(parsed.day))
        self.fecha_mes_var.set(self._MESES[parsed.month - 1])
        if str(parsed.year) in self._ANIOS:
            self.fecha_ano_var.set(str(parsed.year))
        else:
            self.fecha_ano_var.set("")

    def _build_calculo_input(self) -> CalculoServicioInput:
        horas_raw = self.horas_var.get().strip()
        minutos_raw = self.minutos_var.get().strip()
        return CalculoServicioInput(
            fecha_servicio=self.get_fecha_servicio(validate=False),
            codigo_servicio=self.codigo_var.get().strip(),
            modalidad_servicio=self.modalidad_var.get().strip(),
            valor_base=self.valor_base_var.get().strip() or "0",
            servicio_interpretacion=self.interpretacion_var.get(),
            horas_interprete=safe_int(horas_raw) if horas_raw else 0,
            minutos_interprete=safe_int(minutos_raw) if minutos_raw else 0,
        )

    def _toggle_interprete(self) -> None:
        if self._interprete_required and not self.interpretacion_var.get():
            self.interpretacion_var.set(True)
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

    def set_interprete_required(self, required: bool) -> None:
        self._interprete_required = bool(required)
        if self._interprete_required:
            self.interpretacion_var.set(True)
            try:
                self.interpretacion_check.configure(state="disabled")
            except tk.TclError:
                _LOGGER.debug("No se pudo bloquear check de interpretacion.")
            self._toggle_interprete()
        else:
            try:
                self.interpretacion_check.configure(state="normal")
            except tk.TclError:
                _LOGGER.debug("No se pudo desbloquear check de interpretacion.")
            self._toggle_interprete()

    def _validate_interpretacion_required(self, calculo_input: CalculoServicioInput) -> None:
        if not self.interpretacion_var.get():
            return
        horas = int(calculo_input.horas_interprete or 0)
        minutos = int(calculo_input.minutos_interprete or 0)
        if horas <= 0 and minutos <= 0:
            raise RuntimeError("Debes ingresar horas o minutos de interpretacion.")

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

    def reset_for_new_entry(self) -> None:
        self._interprete_required = False
        try:
            self.interpretacion_check.configure(state="normal")
        except tk.TclError:
            _LOGGER.debug("No se pudo restaurar check de interpretacion.")
        self.fecha_dia_var.set("")
        self.fecha_mes_var.set("")
        self.fecha_ano_var.set("")
        self.codigo_var.set("")
        self.codigo_combo.configure(values=self._codigos)
        self.referencia_var.set("")
        self.descripcion_var.set("")
        self.modalidad_var.set("")
        self.valor_base_var.set("")
        self.valor_base_display_var.set("")
        self.interpretacion_var.set(False)
        self._toggle_interprete()
        self.horas_var.set("")
        self.minutos_var.set("")
        self.horas_decimal_var.set("")
        self.total_calculado_var.set("")
        self._last_codigo = None

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
        if not codigo:
            self.codigo_combo.configure(values=self._codigos)
            return
        selected_codigo = codigo if codigo in self._tarifas_by_codigo else ""
        if not selected_codigo:
            candidates = [item for item in self._codigos if codigo in item]
            if candidates:
                selected_codigo = candidates[0]
        if not selected_codigo:
            self.codigo_var.set("")
            self.referencia_var.set("")
            self.descripcion_var.set("")
            self.modalidad_var.set("")
            self.valor_base_var.set("")
            self.valor_base_display_var.set("")
            self._last_codigo = None
            self.codigo_combo.configure(values=self._codigos)
            return
        self.codigo_var.set(selected_codigo)
        self._fetch_tarifa(selected_codigo)

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

    def _open_codigos_popup(self) -> None:
        if self._codigos_popup and self._codigos_popup.winfo_exists():
            self._codigos_popup.lift()
            self._codigos_popup.focus_force()
            return

        rows = []
        for codigo, item in self._tarifas_by_codigo.items():
            rows.append((str(codigo), str(item.get("descripcion_servicio", "") or "")))
        def _codigo_sort_key(value: str) -> tuple[int, int | str, str]:
            txt = str(value or "").strip()
            nums = re.findall(r"\d+", txt)
            if nums:
                return (0, int(nums[0]), txt.lower())
            return (1, txt.lower())

        rows.sort(key=lambda r: _codigo_sort_key(r[0]))

        popup = tk.Toplevel(self)
        popup.title("Lista de codigos")
        popup.geometry("760x440")
        popup.transient(self.winfo_toplevel())
        popup.configure(bg="white")
        self._codigos_popup = popup
        popup.protocol("WM_DELETE_WINDOW", self._close_codigos_popup)

        wrap = ttk.Frame(popup, padding=10)
        wrap.pack(fill=tk.BOTH, expand=True)
        ttk.Label(wrap, text="Tarifas disponibles", font=("Arial", 11, "bold")).pack(anchor="w", pady=(0, 8))

        table_wrap = ttk.Frame(wrap)
        table_wrap.pack(fill=tk.BOTH, expand=True)
        table_wrap.grid_rowconfigure(0, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

        tree = ttk.Treeview(table_wrap, columns=("codigo", "descripcion"), show="headings")
        tree.heading("codigo", text="Codigo servicio")
        tree.heading("descripcion", text="Descripcion servicio")
        tree.column("codigo", width=170, anchor="w")
        tree.column("descripcion", width=520, anchor="w")

        yscroll = ttk.Scrollbar(table_wrap, orient="vertical", command=tree.yview)
        xscroll = ttk.Scrollbar(table_wrap, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        for codigo, descripcion in rows:
            tree.insert("", "end", values=(codigo, descripcion))

    def _close_codigos_popup(self) -> None:
        if self._codigos_popup and self._codigos_popup.winfo_exists():
            self._codigos_popup.destroy()
        self._codigos_popup = None

    def _calcular_interprete(self) -> None:
        try:
            result = calcular_servicio(self._build_calculo_input())
        except ValueError as exc:
            messagebox.showerror("Seccion 3", str(exc))
            return
        horas_dec = result.horas_interprete or Decimal("0")
        self.horas_decimal_var.set(f"{horas_dec:.2f}")
        self.total_calculado_var.set(format_currency(result.valor_total))

    def ensure_tarifa_loaded(self) -> None:
        codigo = self.codigo_var.get().strip()
        if codigo and codigo != self._last_codigo:
            self._fetch_tarifa(codigo)

    def get_payload(self) -> dict:
        calculo_input = self._build_calculo_input()
        self._validate_interpretacion_required(calculo_input)
        try:
            result = calcular_servicio(calculo_input)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc

        try:
            fecha_servicio = self.get_fecha_servicio(validate=True)
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc

        valor_base = safe_decimal(self.valor_base_var.get()).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        payload = {
            "fecha_servicio": fecha_servicio,
            "codigo_servicio": self.codigo_var.get().strip(),
            "referencia_servicio": self.referencia_var.get().strip(),
            "descripcion_servicio": self.descripcion_var.get().strip(),
            "modalidad_servicio": self.modalidad_var.get().strip(),
            "valor_base": self._decimal_to_float(valor_base, "valor_base"),
            "servicio_interpretacion": self.interpretacion_var.get(),
        }
        if self.interpretacion_var.get():
            payload["horas_interprete"] = int(calculo_input.horas_interprete or 0)
            payload["minutos_interprete"] = int(calculo_input.minutos_interprete or 0)
            self.horas_decimal_var.set(f"{(result.horas_interprete or Decimal('0')):.2f}")
            self.total_calculado_var.set(format_currency(result.valor_total))
        return payload

    def set_data(self, data: dict) -> None:
        self.set_fecha_servicio(data.get("fecha_servicio", ""))
        codigo = str(data.get("codigo_servicio", "")).strip()
        self.codigo_var.set(codigo)
        self.referencia_var.set(data.get("referencia_servicio", ""))
        self.descripcion_var.set(data.get("descripcion_servicio", ""))
        self.modalidad_var.set(data.get("modalidad_servicio", ""))

        base = max(
            safe_decimal(data.get("valor_virtual", 0)),
            safe_decimal(data.get("valor_bogota", 0)),
            safe_decimal(data.get("valor_otro", 0)),
            safe_decimal(data.get("todas_modalidades", 0)),
        ).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        self.valor_base_var.set(f"{base:.2f}")
        self.valor_base_display_var.set(format_currency(base))
        self._last_codigo = codigo

        horas_decimal = data.get("horas_interprete")
        valor_interprete = data.get("valor_interprete") or 0
        if horas_decimal:
            self.interpretacion_var.set(True)
            horas_decimal_dec = safe_decimal(horas_decimal).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            horas = int(horas_decimal_dec)
            minutos_dec = (horas_decimal_dec - Decimal(horas)) * Decimal("60")
            minutos = int(minutos_dec.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            if minutos == 60:
                horas += 1
                minutos = 0
            self._toggle_interprete()
            self.horas_var.set(str(horas))
            self.minutos_var.set(str(minutos))
            self.horas_decimal_var.set(f"{horas_decimal_dec:.2f}")
            self.total_calculado_var.set(format_currency(valor_interprete))
        else:
            self.interpretacion_var.set(False)
            self._toggle_interprete()
class PersonaRow(ttk.Frame):
    def __init__(self, parent, cedulas, discapacidades, generos, contratos, on_search) -> None:
        super().__init__(parent)
        self.nombre_var = tk.StringVar()
        self.cedula_var = tk.StringVar()
        self.discapacidad_var = tk.StringVar()
        self.genero_var = tk.StringVar()
        self.fecha_ingreso_var = tk.StringVar()
        self.tipo_contrato_var = tk.StringVar()
        self.cargo_var = tk.StringVar()

        self._cedulas = [str(c) for c in (cedulas or [])]

        ttk.Label(self, text="Cedula").grid(row=0, column=0, sticky="w")
        self.cedula_combo = ttk.Combobox(
            self, textvariable=self.cedula_var, values=cedulas, width=14, state="normal"
        )
        self.cedula_combo.grid(row=0, column=1, sticky="w")
        configure_combobox(self.cedula_combo, cedulas, allow_typing=True)
        self.cedula_combo.bind("<Return>", self._on_cedula_confirm, add="+")
        self.cedula_combo.bind("<FocusOut>", self._on_cedula_confirm, add="+")
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
                width=18,
                font=("Arial", 10),
            )
        else:
            self.fecha_ingreso_widget = ttk.Entry(self, textvariable=self.fecha_ingreso_var, width=18)
        self.fecha_ingreso_widget.grid(row=2, column=1, sticky="w", ipady=2)
        self.fecha_ingreso_var.set("")
        if hasattr(self.fecha_ingreso_widget, "delete"):
            try:
                self.fecha_ingreso_widget.delete(0, "end")
            except tk.TclError:
                _LOGGER.debug("No se pudo limpiar fecha de ingreso en fila de persona.")

        ttk.Label(self, text="Tipo contrato").grid(row=2, column=3, sticky="w", padx=(10, 0))
        self.contrato_combo = ttk.Combobox(
            self, textvariable=self.tipo_contrato_var, values=contratos, width=14, state="readonly"
        )
        self.contrato_combo.grid(row=2, column=4, sticky="w")
        configure_combobox(self.contrato_combo, contratos)

        ttk.Label(self, text="Cargo").grid(row=3, column=0, sticky="w")
        ttk.Entry(self, textvariable=self.cargo_var, width=24).grid(row=3, column=1, sticky="w")

    def _remove_self(self) -> None:
        self.destroy()

    def _on_cedula_confirm(self, _event) -> None:
        value = self.cedula_var.get().strip()
        if not value:
            self.cedula_combo.configure(values=self._cedulas)
            return
        selected = value if value in self._cedulas else ""
        if not selected:
            candidates = [item for item in self._cedulas if value in item]
            if candidates:
                selected = candidates[0]
        if not selected:
            self.cedula_var.set("")
            self.cedula_combo.configure(values=self._cedulas)
            return
        self.cedula_var.set(selected)

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
                _LOGGER.debug("No se pudo aplicar color de validacion a widget de persona.")

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
        self.rows.append(row)

    def clear_rows(self) -> None:
        for row in self.rows:
            row.destroy()
        self.rows = []

    def reset_for_new_entry(self) -> None:
        self.clear_rows()
        self._add_row()

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
        if not hasattr(self, "rows") or not isinstance(self.rows, list):
            self.rows = []
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
            dialog, textvariable=discapacidad_var, values=self.discapacidades, state="readonly", width=18
        )
        discapacidad_combo.grid(row=2, column=1, padx=10, pady=6)
        configure_combobox(discapacidad_combo, self.discapacidades)
        ttk.Label(dialog, text="Genero").grid(row=3, column=0, sticky="w", padx=10, pady=6)
        genero_combo = ttk.Combobox(
            dialog, textvariable=genero_var, values=self.generos, state="readonly", width=18
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
            except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
                log_and_show_error(exc, "No se pudo crear el usuario", title="Error")
                return

            user = data["data"]
            self.state.usuarios_nuevos.append(user)
            if user["cedula_usuario"] not in self.cedulas:
                self.cedulas.append(user["cedula_usuario"])
                self.usuarios_by_cedula[user["cedula_usuario"]] = user
                self.rows = [r for r in self.rows if r.winfo_exists()]
                for row in self.rows:
                    row.cedula_combo.configure(values=self.cedulas)
                    row._cedulas = [str(c) for c in self.cedulas]

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
        personas = []
        for row in self.rows:
            payload = row.as_payload()
            if any(value for value in payload.values()):
                personas.append(payload)
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

    def reset_for_new_entry(self) -> None:
        self.fecha_servicio = ""
        self.obs_text.delete("1.0", tk.END)
        self.obs_agencia_text.delete("1.0", tk.END)
        self.seguimiento_text.delete("1.0", tk.END)


class ResumenFrame(ttk.Frame):
    def __init__(
        self,
        parent,
        on_terminar,
        on_retry_queue=None,
        show_terminar: bool = True,
        show_excel_queue: bool = True,
    ):
        super().__init__(parent)
        self.on_terminar = on_terminar
        self.queue_label = None
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
        if show_excel_queue:
            self.queue_label = ttk.Label(self, text="", foreground=COLOR_TEAL)
            self.queue_label.grid(row=row, column=0, columnspan=2, sticky="w")
            row += 1
            if on_retry_queue:
                tk.Button(
                    self,
                    text="Reintentar cola Drive",
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

class ActasTerminadasPanel(tk.Toplevel):
    def __init__(self, root: tk.Tk, api: ApiClient, on_status_change=None, on_import_acta=None) -> None:
        super().__init__(root)
        self.root = root
        self.api = api
        self.on_status_change = on_status_change
        self.on_import_acta = on_import_acta
        self.title("Actas Terminadas")
        self.geometry("1180x620")
        self.configure(bg="white")
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._rows_by_item: dict[str, dict] = {}

        top = ttk.Frame(self, padding=(10, 8))
        top.pack(fill=tk.X)

        self.pending_var = tk.StringVar(value="Pendientes por revisar: 0")
        ttk.Label(top, textvariable=self.pending_var, foreground=COLOR_PURPLE, font=("Arial", 10, "bold")).pack(
            side=tk.LEFT
        )
        ttk.Label(
            top,
            text="Doble clic en 'Revisado' para cambiar estado. Doble clic en 'Ruta' para abrir.",
            foreground="#555555",
        ).pack(side=tk.LEFT, padx=(12, 0))
        tk.Button(top, text="Refrescar", command=self.refresh, bg="#2E86C1", fg="white", padx=8, pady=3).pack(
            side=tk.RIGHT
        )

        wrap = ttk.Frame(self)
        wrap.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        wrap.grid_rowconfigure(0, weight=1)
        wrap.grid_columnconfigure(0, weight=1)

        cols = ("fecha", "profesional", "empresa", "formato", "ruta", "revisado")
        self.tree = ttk.Treeview(wrap, columns=cols, show="headings")
        self.tree.heading("fecha", text="Fecha y Hora")
        self.tree.heading("profesional", text="Profesional")
        self.tree.heading("empresa", text="Empresa")
        self.tree.heading("formato", text="Formato")
        self.tree.heading("ruta", text="Ruta")
        self.tree.heading("revisado", text="Revisado")
        self.tree.column("fecha", width=170, anchor="w")
        self.tree.column("profesional", width=210, anchor="w")
        self.tree.column("empresa", width=220, anchor="w")
        self.tree.column("formato", width=180, anchor="w")
        self.tree.column("ruta", width=300, anchor="w")
        self.tree.column("revisado", width=90, anchor="center")

        yscroll = ttk.Scrollbar(wrap, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(wrap, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        self.tree.bind("<Double-1>", self._on_double_click)
        self.tree.tag_configure("pending", background="#FFF8E1")

        self.refresh()

    def _on_close(self) -> None:
        self.destroy()

    def _format_fecha(self, row: dict) -> str:
        text = str(
            row.get("finalizado_at_colombia")
            or row.get("finalizado_at_iso")
            or row.get("created_at")
            or ""
        ).strip()
        if not text:
            return ""
        try:
            raw = text
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            local = dt.astimezone(timezone(timedelta(hours=-5)))
            return local.strftime("%Y-%m-%d %H:%M:%S")
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError):
            return text

    def _set_pending(self, pendientes: int) -> None:
        self.pending_var.set(f"Pendientes por revisar: {pendientes}")
        if self.on_status_change:
            try:
                self.on_status_change(pendientes)
            except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
                _LOGGER.warning("No se pudo notificar cambio de pendientes en actas: %s", exc)

    @staticmethod
    def _is_revisado(value) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "t", "si", "s", "yes", "y", "x"}
        return bool(value)

    def refresh(self) -> None:
        payload = self.api.get("/wizard/actas-finalizadas", params={"limit": 1000})
        rows = list(payload.get("data", []) or [])
        pendientes = int(payload.get("pendientes", 0) or 0)
        self._set_pending(pendientes)

        for item in self.tree.get_children():
            self.tree.delete(item)
        self._rows_by_item.clear()

        for idx, row in enumerate(rows):
            iid = f"row_{idx}"
            revisado = self._is_revisado(row.get("revisado"))
            tag = "" if revisado else "pending"
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    self._format_fecha(row),
                    row.get("nombre_usuario", "") or "",
                    row.get("nombre_empresa", "") or "",
                    row.get("nombre_formato", "") or "",
                    self._display_path(row.get("path_formato", "") or ""),
                    "Si" if revisado else "No",
                ),
                tags=(tag,) if tag else (),
            )
            self._rows_by_item[iid] = row

    @staticmethod
    def _display_path(path_value: str) -> str:
        path = str(path_value or "").strip()
        if not path:
            return ""
        if re.match(r"^https?://", path, re.IGNORECASE):
            if "docs.google.com/spreadsheets" in path.lower():
                return "Abrir Google Sheet"
            if "drive.google.com" in path.lower():
                return "Abrir archivo en Drive"
            return "Abrir enlace"
        return path

    def _on_double_click(self, event) -> None:
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        iid = self.tree.identify_row(event.y)
        column = self.tree.identify_column(event.x)
        if not iid or iid not in self._rows_by_item:
            return
        row = self._rows_by_item[iid]

        # #5 = ruta, #6 = revisado
        if column == "#5":
            self._open_path(row.get("path_formato", ""))
        elif column == "#6":
            self._toggle_revisado(row)

    def _open_path(self, path_value: str) -> None:
        path = str(path_value or "").strip()
        if not path:
            messagebox.showinfo("Actas Terminadas", "No hay ruta para este registro.")
            return
        allowed_doc_exts = {".pdf", ".xlsx", ".xlsm", ".xls", ".doc", ".docx", ".png", ".jpg", ".jpeg"}
        blocked_exts = {".exe", ".bat", ".cmd", ".ps1", ".vbs", ".js", ".msi", ".com", ".scr", ".pif"}
        try:
            if re.match(r"^https?://", path, re.IGNORECASE):
                webbrowser.open(path)
                return
            normalized = os.path.expanduser(os.path.expandvars(path))
            ext = os.path.splitext(normalized)[1].lower()
            if ext in blocked_exts:
                messagebox.showwarning(
                    "Actas Terminadas",
                    "La ruta fue bloqueada por seguridad (tipo de archivo no permitido).",
                )
                return
            if ext and ext not in allowed_doc_exts:
                messagebox.showwarning(
                    "Actas Terminadas",
                    "Tipo de archivo no permitido para apertura desde la aplicacion.",
                )
                return
            if os.path.exists(normalized):
                os.startfile(normalized)
                return
            messagebox.showwarning("Actas Terminadas", "La ruta configurada no existe o no es accesible.")
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            log_and_show_error(exc, "No se pudo abrir la ruta del acta", title="Actas Terminadas")

    def _toggle_revisado(self, row: dict) -> None:
        nuevo = not self._is_revisado(row.get("revisado"))
        payload = {
            "registro_id": row.get("registro_id"),
            "session_id": row.get("session_id"),
            "revisado": nuevo,
        }
        try:
            response = self.api.post("/wizard/actas-finalizadas/revisado", payload, timeout=30)
            pendientes = int(response.get("pendientes", 0) or 0)
            self._set_pending(pendientes)
            self.refresh()
            if nuevo and self.on_import_acta:
                try:
                    self.on_import_acta(row)
                except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
                    log_and_show_error(
                        exc,
                        "El acta se marco como revisada, pero no se pudo iniciar la importacion",
                        title="Actas Terminadas",
                    )
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            log_and_show_error(exc, "No se pudo actualizar estado de revision", title="Actas Terminadas")


class GoogleSheetSupabaseSyncReportDialog(tk.Toplevel):
    def __init__(self, owner: "WizardApp", report: dict[str, object], mes: int, ano: int) -> None:
        super().__init__(owner.root)
        self.owner = owner
        self.api = owner.api
        self.report = report
        self.mes = mes
        self.ano = ano
        self.title("Actualizar Supabase")
        self.configure(bg="white")
        self.geometry("1180x720")
        self.transient(owner.root)
        self.grab_set()

        container = ttk.Frame(self, padding=12)
        container.pack(fill=tk.BOTH, expand=True)
        container.grid_columnconfigure(0, weight=1)
        container.grid_rowconfigure(1, weight=1)

        header = ttk.Frame(container)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.grid_columnconfigure(0, weight=1)

        spreadsheet_name = str(report.get("spreadsheet_name") or "-")
        target_name = str(report.get("spreadsheet_target") or spreadsheet_name)
        sheet_name = str(report.get("sheet_name") or "ODS_CALCULADA")
        changed_count = int(report.get("changed_record_count", 0) or 0)
        only_in_sheet_count = int(report.get("only_in_sheet_count", 0) or 0)
        only_in_supabase_count = int(report.get("only_in_supabase_count", 0) or 0)
        invalid_count = len(report.get("invalid_rows", []) or [])
        ignored_count = len(report.get("ignored_rows_without_id", []) or [])
        common_count = int(report.get("common_id_count", 0) or 0)

        summary_lines = [
            f"Spreadsheet: {spreadsheet_name}",
            f"Objetivo mensual: {target_name}",
            f"Hoja fuente: {sheet_name}",
            (
                f"IDs comunes: {common_count} | Filas con cambios: {changed_count} | "
                f"Solo en sheet: {only_in_sheet_count} | Solo en Supabase: {only_in_supabase_count}"
            ),
            f"Filas ignoradas sin ID: {ignored_count} | Filas invalidas: {invalid_count}",
        ]
        ttk.Label(
            header,
            text="\n".join(summary_lines),
            justify="left",
            font=("Arial", 10),
        ).grid(row=0, column=0, sticky="w")

        button_row = ttk.Frame(header)
        button_row.grid(row=0, column=1, sticky="e")
        self.apply_btn = tk.Button(
            button_row,
            text="Aplicar cambios a Supabase",
            command=self._apply_changes,
            bg=COLOR_TEAL,
            fg="white",
            padx=12,
            pady=5,
        )
        self.apply_btn.pack(side=tk.RIGHT, padx=(8, 0))
        tk.Button(
            button_row,
            text="Cerrar",
            command=self.destroy,
            bg=COLOR_PURPLE,
            fg="white",
            padx=12,
            pady=5,
        ).pack(side=tk.RIGHT)
        if changed_count <= 0:
            self.apply_btn.configure(state="disabled")

        notebook = ttk.Notebook(container)
        notebook.grid(row=1, column=0, sticky="nsew")

        changed_tab = ttk.Frame(notebook, padding=8)
        changed_tab.pack(fill=tk.BOTH, expand=True)
        changed_tab.grid_columnconfigure(0, weight=1)
        changed_tab.grid_rowconfigure(0, weight=2)
        changed_tab.grid_rowconfigure(1, weight=1)
        notebook.add(changed_tab, text="Cambios")

        changed_wrap = ttk.Frame(changed_tab)
        changed_wrap.grid(row=0, column=0, sticky="nsew", pady=(0, 8))
        changed_wrap.grid_columnconfigure(0, weight=1)
        changed_wrap.grid_rowconfigure(0, weight=1)
        changed_cols = ("id", "sheet_row", "diff_count", "updated_fields")
        self.changed_tree = ttk.Treeview(changed_wrap, columns=changed_cols, show="headings")
        changed_scroll = ttk.Scrollbar(changed_wrap, orient="vertical", command=self.changed_tree.yview)
        self.changed_tree.configure(yscrollcommand=changed_scroll.set)
        self.changed_tree.grid(row=0, column=0, sticky="nsew")
        changed_scroll.grid(row=0, column=1, sticky="ns")
        self.changed_tree.heading("id", text="ID")
        self.changed_tree.heading("sheet_row", text="Fila Sheet")
        self.changed_tree.heading("diff_count", text="Cambios")
        self.changed_tree.heading("updated_fields", text="Campos")
        self.changed_tree.column("id", width=220, anchor="w")
        self.changed_tree.column("sheet_row", width=90, anchor="center")
        self.changed_tree.column("diff_count", width=90, anchor="center")
        self.changed_tree.column("updated_fields", width=700, anchor="w")

        details_wrap = ttk.Frame(changed_tab)
        details_wrap.grid(row=1, column=0, sticky="nsew")
        details_wrap.grid_columnconfigure(0, weight=1)
        details_wrap.grid_rowconfigure(1, weight=1)
        ttk.Label(details_wrap, text="Diferencias de la fila seleccionada", font=("Arial", 10, "bold")).grid(
            row=0, column=0, sticky="w", pady=(0, 4)
        )
        diff_cols = ("field", "sheet_value", "supabase_value")
        self.diff_tree = ttk.Treeview(details_wrap, columns=diff_cols, show="headings")
        diff_scroll = ttk.Scrollbar(details_wrap, orient="vertical", command=self.diff_tree.yview)
        self.diff_tree.configure(yscrollcommand=diff_scroll.set)
        self.diff_tree.grid(row=1, column=0, sticky="nsew")
        diff_scroll.grid(row=1, column=1, sticky="ns")
        self.diff_tree.heading("field", text="Campo")
        self.diff_tree.heading("sheet_value", text="Valor en Google Sheet")
        self.diff_tree.heading("supabase_value", text="Valor en Supabase")
        self.diff_tree.column("field", width=220, anchor="w")
        self.diff_tree.column("sheet_value", width=420, anchor="w")
        self.diff_tree.column("supabase_value", width=420, anchor="w")

        self._create_list_tab(notebook, "Solo en Sheet", "only_in_sheet", ("id", "sheet_row"))
        self._create_list_tab(notebook, "Solo en Supabase", "only_in_supabase", ("id",))
        self._create_list_tab(
            notebook,
            "Invalidas",
            "invalid_rows",
            ("sheet_row", "id", "reason"),
        )
        self._create_list_tab(
            notebook,
            "Sin ID",
            "ignored_rows_without_id",
            ("sheet_row", "reason", "preview"),
        )

        self._changed_records_by_id: dict[str, dict[str, object]] = {}
        self._populate_report()
        self.changed_tree.bind("<<TreeviewSelect>>", self._on_changed_selected)
        self.owner._center_dialog(self, 1180, 720)

    def _create_list_tab(self, notebook: ttk.Notebook, title: str, key: str, columns: tuple[str, ...]) -> None:
        tab = ttk.Frame(notebook, padding=8)
        tab.pack(fill=tk.BOTH, expand=True)
        tab.grid_columnconfigure(0, weight=1)
        tab.grid_rowconfigure(0, weight=1)
        tree = ttk.Treeview(tab, columns=columns, show="headings")
        scroll = ttk.Scrollbar(tab, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=scroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        scroll.grid(row=0, column=1, sticky="ns")
        for column in columns:
            tree.heading(column, text=column.replace("_", " ").title())
            width = 180 if column != "preview" else 520
            tree.column(column, width=width, anchor="w")
        notebook.add(tab, text=title)
        setattr(self, f"{key}_tree", tree)

    def _populate_report(self) -> None:
        changed_records = list(self.report.get("changed_records", []) or [])
        for record in changed_records:
            record_id = str(record.get("id") or "")
            self._changed_records_by_id[record_id] = record
            self.changed_tree.insert(
                "",
                tk.END,
                iid=record_id,
                values=(
                    record_id,
                    record.get("sheet_row"),
                    record.get("diff_count"),
                    ", ".join(record.get("updated_fields", []) or []),
                ),
            )
        for key in ("only_in_sheet", "only_in_supabase", "invalid_rows", "ignored_rows_without_id"):
            tree = getattr(self, f"{key}_tree")
            for index, item in enumerate(self.report.get(key, []) or []):
                row_id = f"{key}-{index}"
                if key == "only_in_sheet":
                    values = (item.get("id"), item.get("sheet_row"))
                elif key == "only_in_supabase":
                    values = (item.get("id"),)
                elif key == "invalid_rows":
                    values = (item.get("sheet_row"), item.get("id"), item.get("reason"))
                else:
                    preview = item.get("preview", [])
                    values = (item.get("sheet_row"), item.get("reason"), ", ".join(preview))
                tree.insert("", tk.END, iid=row_id, values=values)
        children = self.changed_tree.get_children()
        if children:
            first = children[0]
            self.changed_tree.selection_set(first)
            self._show_diff_for_record(first)

    def _on_changed_selected(self, _event=None) -> None:
        selection = self.changed_tree.selection()
        if not selection:
            return
        self._show_diff_for_record(selection[0])

    def _show_diff_for_record(self, record_id: str) -> None:
        for item in self.diff_tree.get_children():
            self.diff_tree.delete(item)
        record = self._changed_records_by_id.get(record_id) or {}
        for index, diff in enumerate(record.get("diffs", []) or []):
            self.diff_tree.insert(
                "",
                tk.END,
                iid=f"diff-{record_id}-{index}",
                values=(
                    diff.get("field"),
                    diff.get("sheet_value"),
                    diff.get("supabase_value"),
                ),
            )

    def _apply_changes(self) -> None:
        changed_ids = [str(item.get("id") or "") for item in (self.report.get("changed_records", []) or []) if item.get("id")]
        if not changed_ids:
            messagebox.showinfo("Actualizar Supabase", "No hay cambios para aplicar.")
            return
        confirm = messagebox.askyesno(
            "Confirmar actualizacion",
            (
                f"Se aplicaran {len(changed_ids)} filas con diferencias a Supabase.\n"
                "Esta operacion sobreescribira los campos distintos detectados en ODS_CALCULADA.\n"
                "Deseas continuar?"
            ),
        )
        if not confirm:
            return

        loading = LoadingDialog(self, "Aplicando cambios a Supabase...")
        self.update_idletasks()

        def _worker() -> dict:
            return self.api.post(
                "/wizard/google-sheet-sync/apply",
                {"mes": self.mes, "ano": self.ano, "selected_ids": changed_ids},
                timeout=300,
            )

        def _on_success(response: dict) -> None:
            loading.close()
            data = response.get("data", {})
            failed_records = list(data.get("failed_records", []) or [])
            message_lines = [
                f"Filas actualizadas: {data.get('applied_record_count', 0)}",
                f"Campos actualizados: {data.get('applied_field_count', 0)}",
                f"Filas con error: {data.get('failed_record_count', 0)}",
            ]
            if failed_records:
                preview = ", ".join(
                    f"{item.get('id')} (fila {item.get('sheet_row')})" for item in failed_records[:5]
                )
                message_lines.append(f"Errores: {preview}")
            messagebox.showinfo("Actualizar Supabase", "\n".join(message_lines))
            try:
                if int(data.get("failed_record_count", 0) or 0) == 0:
                    self.apply_btn.configure(state="disabled")
            except tk.TclError:
                pass

        def _on_error(exc: Exception) -> None:
            loading.close()
            self.owner._report_error("No se pudo actualizar Supabase desde Google Sheets", exc)

        self.owner._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=300,
            timeout_message="La actualizacion de Supabase excedio el tiempo esperado.",
            operation_name="apply_google_sheet_supabase_sync",
        )


class WizardApp:
    def __init__(self, root: tk.Tk, api: ApiClient) -> None:
        self.root = root
        self.api = api
        self.state = WizardState()
        self._summary_after_id: str | None = None
        self._version_var = tk.StringVar()
        self.actas_panel: ActasTerminadasPanel | None = None
        self._actas_alert_button: tk.Button | None = None
        self._actas_pending = 0
        self._screen_w = self.root.winfo_screenwidth()
        self._screen_h = self.root.winfo_screenheight()
        self._ui_scale = max(0.78, min(1.0, self._screen_h / 900.0))
        self._is_small_screen = self._screen_h <= 800 or self._screen_w <= 1366
        self._main_padx = 10 if self._is_small_screen else 16
        self._main_pady = 10 if self._is_small_screen else 16
        self._menu_button_width = 24 if self._is_small_screen else 28
        self._menu_button_pady = 6 if self._is_small_screen else 8
        self._op_in_progress = False
        self._op_lock_owner: str | None = None
        self._main_action_buttons: list[tk.Button] = []
        self._startup_status_var = tk.StringVar(value="Cargando datos iniciales...")
        self._initial_data_ready = False
        self._initial_data_loading = False
        self._creation_trace_id: str | None = None
        self._creation_started_at: float | None = None

        self.root.title("SISTEMA DE GESTIÓN ODS - RECA")
        self._set_window_size()

        self.header = tk.Frame(self.root, bg=COLOR_PURPLE, height=82 if self._is_small_screen else 96)
        self.header.pack(fill=tk.X)
        self.header_logo = _load_logo(subsample=9 if self._is_small_screen else 8)
        if self.header_logo:
            tk.Label(self.header, image=self.header_logo, bg=COLOR_PURPLE).place(
                x=10 if self._is_small_screen else 16, rely=0.5, anchor="w"
            )
        title_frame = tk.Frame(self.header, bg=COLOR_PURPLE)
        title_frame.place(relx=0.5, rely=0.5, anchor="center")
        tk.Label(
            title_frame,
            text="SISTEMA DE GESTION ODS",
            font=("Arial", self._scaled_font(20, 16), "bold"),
            bg=COLOR_PURPLE,
            fg="white",
        ).pack()
        tk.Label(
            title_frame,
            text="RECA",
            font=("Arial", self._scaled_font(20, 16), "bold"),
            bg=COLOR_PURPLE,
            fg="white",
        ).pack()

        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=self._main_padx, pady=self._main_pady)

        self.footer = ttk.Frame(self.root)
        self.footer.pack(fill=tk.X, padx=max(8, self._main_padx - 2), pady=(0, 6), side=tk.BOTTOM)
        self._version_var.set("Version local: - | GitHub: -")
        version_box = ttk.Frame(self.footer)
        version_box.pack(side=tk.LEFT, anchor="w")
        ttk.Label(
            version_box,
            textvariable=self._version_var,
            font=("Arial", 9),
            foreground="#666666",
        ).pack(anchor="w")

        self.show_initial_screen()
        self.root.after(100, lambda: self._prefetch_initial_data_async(silent=True))
        self.root.after(350, self._flush_google_drive_queue_silent)

    def set_version_info(self, local_version: str, remote_version: str | None) -> None:
        remote_label = remote_version or "-"
        self._version_var.set(f"Version local: {local_version} | GitHub: {remote_label}")

    def show_initial_screen(self) -> None:
        for child in self.main_frame.winfo_children():
            child.destroy()

        scroll = ScrollableFrame(self.main_frame)
        scroll.pack(fill=tk.BOTH, expand=True)

        content = ttk.Frame(scroll.content)
        content.pack(fill=tk.BOTH, expand=True)
        content.grid_columnconfigure(0, weight=1)

        button_col = ttk.Frame(content)
        button_col.grid(row=0, column=0, pady=(4, 8))

        top_actions = ttk.Frame(button_col)
        top_actions.pack(fill=tk.X, pady=(0, max(4, int(6 * self._ui_scale))))
        actions_right = ttk.Frame(top_actions)
        actions_right.pack(anchor="e")
        update_btn = tk.Button(
            actions_right,
            text="Actualizar Version de la Aplicacion",
            command=self._open_update_page,
            bg="#4B8BBE",
            fg="white",
            font=("Arial", self._scaled_font(9, 8), "bold"),
            padx=7 if self._is_small_screen else 8,
            pady=2 if self._is_small_screen else 3,
        )
        update_btn.pack(side=tk.LEFT, padx=(0, 6))
        self._actas_alert_button = tk.Button(
            actions_right,
            text="Actas Terminadas (0)",
            command=self._open_actas_terminadas,
            bg="#5D6D7E",
            fg="white",
            font=("Arial", self._scaled_font(9, 8), "bold"),
            padx=7 if self._is_small_screen else 8,
            pady=2 if self._is_small_screen else 3,
        )
        self._actas_alert_button.pack(side=tk.LEFT)

        startup_status = ttk.Frame(button_col)
        startup_status.pack(fill=tk.X, pady=(0, max(4, int(6 * self._ui_scale))))
        ttk.Label(
            startup_status,
            textvariable=self._startup_status_var,
            foreground="#666666",
            font=("Arial", self._scaled_font(9, 8)),
        ).pack(side=tk.LEFT)
        retry_btn = tk.Button(
            startup_status,
            text="Reintentar carga",
            command=lambda: self._prefetch_initial_data_async(silent=False),
            bg="#5D6D7E",
            fg="white",
            font=("Arial", self._scaled_font(8, 8), "bold"),
            padx=6,
            pady=1,
        )
        retry_btn.pack(side=tk.RIGHT)

        ttk.Label(
            button_col,
            text="Seleccione una opcion para iniciar",
            font=("Arial", self._scaled_font(14, 11), "bold"),
            foreground=COLOR_PURPLE,
        ).pack(pady=(max(12, int(16 * self._ui_scale)), max(8, int(12 * self._ui_scale))))

        new_entry_btn = tk.Button(
            button_col,
            text="Crear nueva entrada",
            command=self.start_new_service,
            bg=COLOR_TEAL,
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        )
        new_entry_btn.pack(pady=self._menu_button_pady)

        retry_drive_btn = tk.Button(
            button_col,
            text="Reintentar sincronizacion Drive",
            command=self._flush_google_drive_queue,
            bg="#117A65",
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        )
        retry_drive_btn.pack(pady=self._menu_button_pady)

        refresh_cache_btn = tk.Button(
            button_col,
            text="Actualizar Base de Datos - Gestión",
            command=self._refresh_cache_from_supabase,
            bg="#2E86C1",
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        )
        refresh_cache_btn.pack(pady=self._menu_button_pady)

        update_supabase_btn = tk.Button(
            button_col,
            text="Actualizar Supabase",
            command=self._open_google_sheet_supabase_dialog,
            bg="#1F618D",
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        )
        update_supabase_btn.pack(pady=self._menu_button_pady)

        self._main_action_buttons = [
            update_btn,
            self._actas_alert_button,
            retry_btn,
            new_entry_btn,
            retry_drive_btn,
            refresh_cache_btn,
            update_supabase_btn,
        ]
        if self._initial_data_loading:
            self._set_main_actions_enabled(False)
        elif not self._initial_data_ready:
            # Permitimos acciones auxiliares, pero forzamos recarga de datos para nueva entrada.
            self._set_main_actions_enabled(True)
        self._refresh_actas_alert_async(silent=True)

    def _scaled_font(self, base_size: int, min_size: int = 9) -> int:
        return max(min_size, int(round(base_size * self._ui_scale)))

    def _set_main_actions_enabled(self, enabled: bool) -> None:
        state = "normal" if enabled else "disabled"
        for button in self._main_action_buttons:
            try:
                if button.winfo_exists():
                    button.configure(state=state)
            except tk.TclError:
                _LOGGER.debug("No se pudo actualizar estado de boton de menu principal.")

    def _begin_ui_operation(self, name: str, disable_main_actions: bool = True) -> bool:
        if self._op_in_progress:
            messagebox.showinfo("Operacion en curso", "Espera a que finalice la operacion actual.")
            return False
        self._op_in_progress = True
        self._op_lock_owner = name
        if disable_main_actions:
            self._set_main_actions_enabled(False)
        return True

    def _end_ui_operation(self, name: str) -> None:
        if not self._op_in_progress:
            return
        if self._op_lock_owner and name != self._op_lock_owner:
            return
        self._op_in_progress = False
        self._op_lock_owner = None
        self._set_main_actions_enabled(True)

    def _report_error(self, context: str, exc: Exception, title: str = "Error") -> None:
        log_and_show_error(exc, context, title=title)

    def _ensure_creation_trace(self) -> str:
        if not self._creation_trace_id:
            self._creation_trace_id = f"ods-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"
        if self._creation_started_at is None:
            self._creation_started_at = time.time()
        return self._creation_trace_id

    def _log_creation_flow(self, event: str, **details) -> None:
        trace_id = self._ensure_creation_trace()
        parts = [f"trace_id={trace_id}", f"event={event}"]
        if self._creation_started_at is not None:
            elapsed = time.time() - self._creation_started_at
            parts.append(f"elapsed_s={elapsed:.3f}")
        for key, value in details.items():
            if value is None:
                continue
            parts.append(f"{key}={value!r}")
        _ODS_FLOW_LOGGER.info(" | ".join(parts))

    def _finish_creation_trace(self, final_event: str, **details) -> None:
        self._log_creation_flow(final_event, **details)
        self._creation_trace_id = None
        self._creation_started_at = None

    def _run_background_task(
        self,
        worker,
        on_success,
        on_error=None,
        *,
        timeout_sec: int | float | None = None,
        timeout_message: str | None = None,
        poll_ms: int = 250,
        operation_name: str | None = None,
        disable_main_actions: bool = True,
    ) -> None:
        operation_started = False
        if operation_name:
            operation_started = self._begin_ui_operation(operation_name, disable_main_actions=disable_main_actions)
            if not operation_started:
                return

        events: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)

        def _runner() -> None:
            try:
                result = worker()
            except (
                RuntimeError,
                ValueError,
                TypeError,
                OSError,
                KeyError,
                IndexError,
                AttributeError,
                tk.TclError,
                ServiceError,
            ) as exc:
                events.put(("error", exc))
                return
            events.put(("ok", result))

        thread = threading.Thread(target=_runner, daemon=True)
        thread.start()
        started = time.time()

        def _poll() -> None:
            try:
                status, payload = events.get_nowait()
            except queue.Empty:
                if timeout_sec is not None and (time.time() - started) >= timeout_sec:
                    error = TimeoutError(timeout_message or "La operacion excedio el tiempo limite.")
                    if operation_started and operation_name:
                        self._end_ui_operation(operation_name)
                    if on_error:
                        on_error(error)
                    else:
                        self._report_error("Operacion en segundo plano", error)
                    return
                self.root.after(poll_ms, _poll)
                return

            if status == "ok":
                if operation_started and operation_name:
                    self._end_ui_operation(operation_name)
                on_success(payload)
                return

            error = payload if isinstance(payload, Exception) else RuntimeError(str(payload))
            if operation_started and operation_name:
                self._end_ui_operation(operation_name)
            if on_error:
                on_error(error)
            else:
                self._report_error("Operacion en segundo plano", error)

        self.root.after(poll_ms, _poll)

    def _return_to_menu_from_form(self) -> None:
        self._finish_creation_trace("cancelado_volver_menu")
        self.show_initial_screen()

    def _set_actas_pending(self, pendientes: int) -> None:
        self._actas_pending = max(0, int(pendientes or 0))
        btn = self._actas_alert_button
        if not btn:
            return
        try:
            if not btn.winfo_exists():
                return
            if self._actas_pending > 0:
                btn.configure(
                    text=f"Actas Terminadas ({self._actas_pending})",
                    bg="#C0392B",
                )
            else:
                btn.configure(text="Actas Terminadas (0)", bg="#5D6D7E")
        except tk.TclError:
            _LOGGER.debug("No se pudo actualizar badge de actas pendientes (widget no disponible).")
            return

    def _refresh_actas_alert(self, silent: bool = False) -> None:
        try:
            payload = self.api.get("/wizard/actas-finalizadas/status")
            data = payload.get("data", {})
            pendientes = int(data.get("pendientes", 0) or 0)
            self._set_actas_pending(pendientes)
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            if not silent:
                log_and_show_error(exc, "No se pudo cargar el estado de actas", title="Actas Terminadas")

    def _refresh_actas_alert_async(self, silent: bool = False) -> None:
        def _worker() -> int:
            payload = self.api.get("/wizard/actas-finalizadas/status")
            data = payload.get("data", {})
            return int(data.get("pendientes", 0) or 0)

        def _on_success(pendientes: int) -> None:
            self._set_actas_pending(pendientes)

        def _on_error(exc: Exception) -> None:
            if not silent:
                self._report_error("No se pudo cargar el estado de actas", exc, title="Actas Terminadas")

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=30,
            timeout_message="No se pudo consultar el estado de actas a tiempo.",
            poll_ms=200,
            operation_name=None,
            disable_main_actions=False,
        )

    def _prefetch_initial_data_async(self, silent: bool = False) -> None:
        if self._initial_data_loading:
            return
        if not self._begin_ui_operation("prefetch_inicial"):
            return
        self._initial_data_loading = True
        self._startup_status_var.set("Cargando datos iniciales...")

        def _worker() -> bool:
            self.api.prefetch(INITIAL_PREFETCH_ITEMS)
            return True

        def _on_success(_result: bool) -> None:
            self._initial_data_loading = False
            self._initial_data_ready = True
            self._startup_status_var.set("Datos iniciales cargados.")
            self._end_ui_operation("prefetch_inicial")

        def _on_error(exc: Exception) -> None:
            self._initial_data_loading = False
            self._initial_data_ready = False
            self._startup_status_var.set("Sin conexion con Supabase. Usa 'Reintentar carga'.")
            self._end_ui_operation("prefetch_inicial")
            if not silent:
                self._report_error("No se pudieron cargar datos iniciales", exc, title="Inicio")

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=60,
            timeout_message="Supabase no respondio durante la carga inicial.",
            poll_ms=200,
            operation_name=None,
        )

    def _open_actas_terminadas(self) -> None:
        if self.actas_panel and self.actas_panel.winfo_exists():
            self.actas_panel.lift()
            self.actas_panel.focus_force()
            return
        self.actas_panel = ActasTerminadasPanel(
            self.root,
            self.api,
            on_status_change=self._set_actas_pending,
            on_import_acta=self._importar_acta_revisada,
        )

    def _open_google_sheet_supabase_dialog(self) -> None:
        dialog = tk.Toplevel(self.root)
        dialog.title("Actualizar Supabase")
        dialog.resizable(False, False)
        dialog.configure(bg="white")
        dialog.transient(self.root)
        dialog.grab_set()

        container = ttk.Frame(dialog, padding=16)
        container.pack(fill=tk.BOTH, expand=True)

        meses = [
            "Enero",
            "Febrero",
            "Marzo",
            "Abril",
            "Mayo",
            "Junio",
            "Julio",
            "Agosto",
            "Septiembre",
            "Octubre",
            "Noviembre",
            "Diciembre",
        ]
        mes_var = tk.StringVar()
        ano_var = tk.StringVar()

        hoy = date.today()
        mes_var.set(meses[max(0, min(len(meses) - 1, hoy.month - 1))])
        ano_var.set(str(hoy.year))

        ttk.Label(container, text="Mes").grid(row=0, column=0, sticky="w", pady=4)
        mes_combo = ttk.Combobox(container, textvariable=mes_var, values=meses, width=20, state="readonly")
        mes_combo.grid(row=0, column=1, sticky="w", pady=4)
        configure_combobox(mes_combo, meses)

        ttk.Label(container, text="Año").grid(row=1, column=0, sticky="w", pady=4)
        anos = [str(year) for year in range(2020, 2036)]
        ano_combo = ttk.Combobox(container, textvariable=ano_var, values=anos, width=10, state="readonly")
        ano_combo.grid(row=1, column=1, sticky="w", pady=4)
        configure_combobox(ano_combo, anos)

        ttk.Label(
            container,
            text="Se usará la hoja ODS_CALCULADA del spreadsheet mensual y solo se actualizarán IDs coincidentes.",
            wraplength=420,
            justify="left",
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(8, 0))

        button_row = ttk.Frame(container)
        button_row.grid(row=3, column=0, columnspan=2, pady=(12, 0), sticky="e")

        def _confirmar() -> None:
            mes_nombre = mes_var.get().strip()
            ano_raw = ano_var.get().strip()
            if mes_nombre not in meses:
                messagebox.showerror("Actualizar Supabase", "Selecciona un mes valido.")
                return
            try:
                ano = int(ano_raw)
            except ValueError:
                messagebox.showerror("Actualizar Supabase", "Selecciona un año valido.")
                return
            mes = meses.index(mes_nombre) + 1
            dialog.destroy()
            self._preview_google_sheet_supabase_sync(mes, ano)

        tk.Button(
            button_row,
            text="Cancelar",
            command=dialog.destroy,
            bg=COLOR_PURPLE,
            fg="white",
            padx=10,
            pady=4,
        ).pack(side=tk.RIGHT, padx=(8, 0))
        tk.Button(
            button_row,
            text="Generar reporte",
            command=_confirmar,
            bg=COLOR_TEAL,
            fg="white",
            padx=10,
            pady=4,
        ).pack(side=tk.RIGHT)

        self._center_dialog(dialog, 520, 230)

    def _preview_google_sheet_supabase_sync(self, mes: int, ano: int) -> None:
        loading = LoadingDialog(self.root, "Comparando Google Sheet con Supabase...")
        self.root.update_idletasks()

        def _worker() -> dict:
            return self.api.post(
                "/wizard/google-sheet-sync/preview",
                {"mes": mes, "ano": ano},
                timeout=300,
            )

        def _on_success(response: dict) -> None:
            loading.close()
            report = response.get("data", {})
            GoogleSheetSupabaseSyncReportDialog(self, report, mes, ano)

        def _on_error(exc: Exception) -> None:
            loading.close()
            self._report_error("No se pudo generar el reporte de sincronizacion", exc)

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=300,
            timeout_message="La comparacion entre Google Sheets y Supabase excedio el tiempo esperado.",
            operation_name="preview_google_sheet_supabase_sync",
        )

    def _set_window_size(self) -> None:
        self.root.update_idletasks()
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        # Keep window inside the visible work area for low-resolution displays.
        min_w = 760 if self._is_small_screen else 900
        min_h = 520 if self._is_small_screen else 560
        width = min(max(min_w, int(screen_w * 0.9)), max(min_w, screen_w - 20))
        height = min(max(min_h, int(screen_h * 0.9)), max(min_h, screen_h - 80))
        x = max(0, int((screen_w - width) / 2))
        y = max(0, int((screen_h - height) / 2))
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        self.root.minsize(min_w, min_h)

    def start_new_service(self, after_ready=None) -> None:
        try:
            self._creation_trace_id = f"ods-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"
            self._creation_started_at = time.time()
            self._log_creation_flow("inicio_nueva_entrada")
            self.state.reset_service()
            for child in self.main_frame.winfo_children():
                child.destroy()

            self.scroll = ScrollableFrame(self.main_frame)
            self.scroll.pack(fill=tk.BOTH, expand=True)
            self.scroll.content.grid_columnconfigure(0, weight=1)

            main_col = ttk.Frame(self.scroll.content)
            main_col.grid(row=0, column=0, sticky="nsew")
            main_col.grid_columnconfigure(0, weight=1)

            nav = ttk.Frame(main_col)
            nav.grid(row=0, column=0, sticky="ew", pady=(0, 6))
            tk.Button(
                nav,
                text="Volver",
                command=self._return_to_menu_from_form,
                bg=COLOR_PURPLE,
                fg="white",
                padx=12,
                pady=4,
            ).pack(side="left")
            tk.Button(
                nav,
                text="Importar acta Excel",
                command=self._importar_acta_excel,
                bg="#2E86C1",
                fg="white",
                padx=12,
                pady=4,
            ).pack(side="left", padx=(8, 0))

            self.seccion1 = Seccion1Frame(main_col, self.api, self.state)
            self.seccion1.grid(row=1, column=0, sticky="ew", pady=8)

            self.seccion2 = Seccion2Frame(main_col, self.api)
            self.seccion2.grid(row=2, column=0, sticky="ew", pady=8)

            self.seccion3 = Seccion3Frame(main_col, self.api)
            self.seccion3.grid(row=3, column=0, sticky="ew", pady=8)

            self.seccion4 = Seccion4Frame(main_col, self.api, self.state)
            self.seccion4.grid(row=4, column=0, sticky="ew", pady=8)

            self.seccion5 = Seccion5Frame(main_col, self.api)
            self.seccion5.grid(row=5, column=0, sticky="ew", pady=8)

            self.resumen = ResumenFrame(
                main_col,
                self.terminar_servicio,
                None,
                show_excel_queue=False,
            )
            self.resumen.grid(row=6, column=0, sticky="ew", pady=8)

            loading = LoadingDialog(self.root, "Cargando datos del formulario...", determinate=True)
            loading.set_status("Sincronizando cache local...", 10)

            def _worker() -> dict:
                return self.api.build_cache(INITIAL_PREFETCH_ITEMS)

            def _on_success(new_cache: dict) -> None:
                loading.set_status("Aplicando datos...", 80)
                self.api.replace_cache(new_cache)
                self._initial_data_ready = True
                self._startup_status_var.set("Datos iniciales cargados.")
                self._load_section_data()
                self._reset_for_new_entry()
                self._lock_sections()
                self._bind_summary_updates()
                self._refresh_summary()
                self._log_creation_flow("formulario_cargado")
                loading.set_status("Listo", 100)
                loading.close()
                if after_ready:
                    def _run_after_ready() -> None:
                        try:
                            after_ready()
                        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
                            self._report_error("No se pudo aplicar la importacion del acta", exc, title="Importar acta")

                    self.root.after(0, _run_after_ready)

            def _on_error(exc: Exception) -> None:
                loading.close()
                self._initial_data_ready = False
                self._startup_status_var.set("Sin conexion con Supabase. Usa 'Reintentar carga'.")
                self.show_initial_screen()
                self._finish_creation_trace("error_inicio_formulario", error=str(exc))
                self._report_error("No se pudo iniciar el formulario", exc)

            self._run_background_task(
                _worker,
                _on_success,
                _on_error,
                timeout_sec=60,
                timeout_message="Supabase no respondio al cargar el formulario.",
                poll_ms=200,
                operation_name="start_new_service_load",
            )
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            self._finish_creation_trace("error_inicio_formulario", error=str(exc))
            self._report_error("No se pudo iniciar el formulario", exc)

    def _importar_acta_excel(self) -> None:
        file_path = filedialog.askopenfilename(
            title="Seleccionar acta en Excel",
            filetypes=[
                ("Archivos Excel", "*.xlsx *.xlsm"),
                ("Todos los archivos", "*.*"),
            ],
        )
        if not file_path:
            return
        self._importar_acta_desde_fuente(file_path, source_label=Path(file_path).name)

    def _importar_acta_revisada(self, row: dict) -> None:
        source = str(row.get("path_formato") or "").strip()
        if not source:
            messagebox.showinfo(
                "Actas Terminadas",
                "El acta se marco como revisada, pero no tiene ruta configurada para importar.",
            )
            return
        source_label = str(row.get("nombre_formato") or "").strip() or "Acta revisada"
        self._importar_acta_desde_fuente(source, source_label=source_label, start_form_if_needed=True)

    def _importar_acta_desde_fuente(
        self,
        source: str,
        *,
        source_label: str = "acta",
        start_form_if_needed: bool = False,
    ) -> None:
        source_text = str(source or "").strip()
        if not source_text:
            messagebox.showerror("Importar acta", "No se encontro una ruta o URL valida para el acta.")
            return

        is_remote = bool(re.match(r"^https?://", source_text, re.IGNORECASE))
        loading = LoadingDialog(
            self.root,
            "Leyendo acta desde Google Drive..." if is_remote else "Leyendo acta de Excel...",
        )
        self.root.update_idletasks()

        def _worker() -> dict:
            from app.services.excel_acta_import import parse_acta_source

            parsed = parse_acta_source(source_text)
            prepared_parsed, participantes = self._preparar_importacion_acta(parsed)
            return {
                "parsed": prepared_parsed,
                "participantes": participantes,
            }

        def _on_success(result: dict) -> None:
            loading.close()
            parsed = dict(result.get("parsed") or {})
            participantes = list(result.get("participantes") or [])

            def _apply_import() -> None:
                self._procesar_importacion_acta(parsed, participantes, source_label=source_label)

            if start_form_if_needed:
                self.start_new_service(after_ready=_apply_import)
                return
            _apply_import()

        def _on_error(exc: Exception) -> None:
            loading.close()
            self._report_error("No se pudo leer el acta", exc, title="Importar acta")

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=180 if is_remote else 60,
            timeout_message=(
                "La importacion del acta desde Google Drive excedio el tiempo esperado."
                if is_remote
                else "La lectura del acta excedio el tiempo esperado."
            ),
            poll_ms=200,
            operation_name="importar_acta_fuente",
        )

    def _buscar_empresas_por_nit_import(self, nit: str) -> list[dict]:
        nit_clean = str(nit or "").strip()
        if not nit_clean:
            return []
        try:
            empresas_payload = self.api.get_cached("/wizard/seccion-2/empresas")
            empresas = list(empresas_payload.get("data") or [])
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError):
            empresas = []

        matches = [
            item
            for item in empresas
            if str(item.get("nit_empresa") or "").strip() == nit_clean
        ]
        if matches:
            return matches

        empresa_lookup = self.api.get("/wizard/seccion-2/empresa", params={"nit": nit_clean})
        return list(empresa_lookup.get("data") or [])

    def _preparar_importacion_acta(self, parsed: dict) -> tuple[dict, list[dict]]:
        prepared = dict(parsed)
        nit = (parsed.get("nit_empresa") or "").strip()
        if not nit:
            raise RuntimeError("No se detecto NIT en el archivo. Verifica la plantilla.")

        empresas_encontradas = self._buscar_empresas_por_nit_import(nit)
        if not empresas_encontradas:
            raise RuntimeError(f"El NIT {nit} no existe en la base de datos. Verifica el formulario.")

        prepared["_nit_validado_bd"] = True
        prepared["_empresa_bd_nombre"] = str(empresas_encontradas[0].get("nombre_empresa") or "").strip()

        participantes_raw = list(parsed.get("participantes") or [])
        cedulas_bd = set()
        try:
            usuarios_data = self.api.get_cached("/wizard/seccion-4/usuarios")
            for item in list(usuarios_data.get("data") or []):
                ced = str(item.get("cedula_usuario") or "").strip()
                if ced:
                    cedulas_bd.add(ced)
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError):
            cedulas_bd = set()

        participantes: list[dict] = []
        descartados: list[str] = []
        for persona in participantes_raw:
            ced = str(persona.get("cedula_usuario") or "").strip()
            if not ced:
                continue
            if ced in cedulas_bd:
                participantes.append({"cedula_usuario": ced})
            else:
                descartados.append(ced)
        if descartados:
            prepared.setdefault("warnings", []).append(
                f"Se descartaron {len(descartados)} cedula(s) que no existen en BD."
            )
            prepared["_cedulas_descartadas"] = descartados

        return prepared, participantes

    def _procesar_importacion_acta(
        self,
        parsed: dict,
        participantes: list[dict],
        *,
        source_label: str = "acta",
    ) -> None:
        nit = (parsed.get("nit_empresa") or "").strip()

        if len(participantes) > 1:
            seleccion = self._seleccionar_participantes_import(participantes)
            if seleccion is None:
                return
            participantes = seleccion

        if not self._preview_importacion_acta(parsed, participantes):
            return

        self._aplicar_importacion_acta(parsed, participantes)

        warnings = parsed.get("warnings") or []
        summary = [
            f"Fuente: {source_label}",
            f"NIT detectado: {nit}",
            f"Cedulas cargadas: {len(participantes)}",
        ]
        fecha_servicio = (parsed.get("fecha_servicio") or "").strip()
        if fecha_servicio:
            summary.append(f"Fecha detectada: {fecha_servicio}")
        if warnings:
            summary.append("")
            summary.append("Avisos:")
            summary.extend(f"- {item}" for item in warnings)
        messagebox.showinfo("Importar acta", "\n".join(summary))

    def _wait_dialog_with_timeout(
        self,
        dialog: tk.Toplevel,
        *,
        timeout_ms: int = 120_000,
        timeout_message: str = "La ventana tardo demasiado en responder.",
    ) -> bool:
        timed_out = {"value": False}

        def _on_timeout() -> None:
            timed_out["value"] = True
            try:
                if dialog.winfo_exists():
                    dialog.grab_release()
                    dialog.destroy()
            except tk.TclError:
                _LOGGER.debug("No se pudo cerrar dialogo por timeout.")

        after_id = dialog.after(timeout_ms, _on_timeout)
        try:
            dialog.wait_window()
        finally:
            try:
                dialog.after_cancel(after_id)
            except tk.TclError:
                _LOGGER.debug("No se pudo cancelar timeout de dialogo (ya destruido).")

        if timed_out["value"]:
            messagebox.showwarning("Tiempo de espera agotado", timeout_message)
            return False
        return True

    def _seleccionar_participantes_import(self, participantes: list[dict]) -> list[dict] | None:
        dialog = tk.Toplevel(self.root)
        dialog.title("Seleccionar participantes")
        dialog.resizable(True, True)
        dialog.geometry("760x420")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(
            dialog,
            text="Se detectaron varias cedulas validas en BD. Elige cuales importar:",
            font=("Arial", 10, "bold"),
        ).pack(anchor="w", padx=12, pady=(10, 8))

        table_wrap = ttk.Frame(dialog)
        table_wrap.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 10))
        table_wrap.grid_rowconfigure(0, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

        tree = ttk.Treeview(table_wrap, columns=("idx", "cedula"), show="headings", selectmode="extended")
        tree.heading("idx", text="#")
        tree.heading("cedula", text="Cedula")
        tree.column("idx", width=60, anchor="center")
        tree.column("cedula", width=260, anchor="w")
        yscroll = ttk.Scrollbar(table_wrap, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=yscroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")

        row_map: dict[str, dict] = {}
        for idx, persona in enumerate(participantes, start=1):
            ced = (persona.get("cedula_usuario") or "").strip()
            item_id = tree.insert("", "end", values=(idx, ced))
            row_map[item_id] = {"cedula_usuario": ced}

        result: dict[str, list[dict] | None] = {"value": None}

        def _importar_todos() -> None:
            result["value"] = list(row_map.values())
            dialog.destroy()

        def _importar_seleccion() -> None:
            selected = tree.selection()
            if not selected:
                messagebox.showwarning("Seleccion", "Selecciona al menos un participante.")
                return
            result["value"] = [row_map[item_id] for item_id in selected if item_id in row_map]
            dialog.destroy()

        def _cancelar() -> None:
            result["value"] = None
            dialog.destroy()

        btns = ttk.Frame(dialog)
        btns.pack(fill=tk.X, padx=12, pady=(0, 12))
        tk.Button(btns, text="Importar todos", command=_importar_todos, bg=COLOR_TEAL, fg="white", padx=10, pady=4).pack(
            side=tk.LEFT
        )
        tk.Button(
            btns,
            text="Importar seleccionados",
            command=_importar_seleccion,
            bg="#2E86C1",
            fg="white",
            padx=10,
            pady=4,
        ).pack(side=tk.LEFT, padx=(8, 0))
        tk.Button(btns, text="Cancelar", command=_cancelar, bg=COLOR_PURPLE, fg="white", padx=10, pady=4).pack(
            side=tk.RIGHT
        )

        if not self._wait_dialog_with_timeout(
            dialog,
            timeout_ms=120_000,
            timeout_message="La seleccion de participantes supero el tiempo de espera.",
        ):
            return None
        return result["value"]

    def _resolve_profesional_import(self, profesional: str, candidatos: list[str] | None = None) -> str:
        """Busca el profesional RECA que mejor coincide con los candidatos del acta.

        Compara todos los nombres encontrados en la sección de asistentes contra la
        tabla de profesionales y retorna el de mayor puntaje de similitud.
        """
        prof_values = list(getattr(self.seccion1.prof_combo, "_all_values", []) or [])

        # Construir lista de fuentes a comparar: primero los candidatos del acta,
        # luego el nombre_profesional como fallback.
        sources: list[str] = []
        for c in (candidatos or []):
            c = (c or "").strip()
            if c:
                sources.append(c)
        fallback = (profesional or "").strip()
        if fallback and fallback not in sources:
            sources.append(fallback)

        if not sources:
            return ""

        best_item = ""
        best_score = 0.0
        for src_raw in sources:
            if src_raw in prof_values:
                return src_raw
            src = normalize_search_text(src_raw)
            if not src:
                continue
            src_tokens = set(src.split())
            for item in prof_values:
                norm_item = normalize_search_text(item)
                if not norm_item:
                    continue
                if src in norm_item or norm_item in src:
                    return item
                item_tokens = set(norm_item.split())
                overlap = len(src_tokens & item_tokens) / max(len(src_tokens), 1)
                ratio = difflib.SequenceMatcher(None, src, norm_item).ratio()
                score = max(overlap, ratio)
                if score > best_score:
                    best_score = score
                    best_item = item

        if best_score >= 0.55:
            return best_item
        return ""

    def _preview_importacion_acta(self, parsed: dict, participantes: list[dict]) -> bool:
        dialog = tk.Toplevel(self.root)
        dialog.title("Vista previa de importacion")
        dialog.resizable(True, True)
        dialog.geometry("900x560")
        dialog.transient(self.root)
        dialog.grab_set()

        ttk.Label(
            dialog,
            text="Revisa el mapeo antes de aplicar al formulario",
            font=("Arial", 10, "bold"),
        ).pack(anchor="w", padx=12, pady=(10, 8))

        mapping_wrap = ttk.LabelFrame(dialog, text="Campos detectados", padding=(8, 8))
        mapping_wrap.pack(fill=tk.X, padx=12, pady=(0, 8))
        mapping_wrap.grid_columnconfigure(0, weight=1)

        mapping_tree = ttk.Treeview(mapping_wrap, columns=("campo", "valor", "destino"), show="headings", height=8)
        mapping_tree.heading("campo", text="Campo interno")
        mapping_tree.heading("valor", text="Valor detectado")
        mapping_tree.heading("destino", text="Destino")
        mapping_tree.column("campo", width=220, anchor="w")
        mapping_tree.column("valor", width=340, anchor="w")
        mapping_tree.column("destino", width=260, anchor="w")
        map_scroll = ttk.Scrollbar(mapping_wrap, orient="vertical", command=mapping_tree.yview)
        mapping_tree.configure(yscrollcommand=map_scroll.set)
        mapping_tree.grid(row=0, column=0, sticky="nsew")
        map_scroll.grid(row=0, column=1, sticky="ns")

        raw_prof = (parsed.get("nombre_profesional") or "").strip()
        candidatos_prof = parsed.get("candidatos_profesional") or []
        resolved_prof = str(parsed.get("_resolved_profesional") or "").strip()
        if not resolved_prof:
            resolved_prof = self._resolve_profesional_import(raw_prof, candidatos_prof)
            parsed["_resolved_profesional"] = resolved_prof
        modalidad = (parsed.get("modalidad_servicio") or "").strip()
        nombre_empresa = (parsed.get("nombre_empresa") or "").strip()
        empresa_bd_nombre = (parsed.get("_empresa_bd_nombre") or "").strip()
        nit = (parsed.get("nit_empresa") or "").strip()
        fecha = (parsed.get("fecha_servicio") or "").strip()
        nit_validado_bd = bool(parsed.get("_nit_validado_bd"))

        rows = [
            ("seccion2.nit_empresa", nit or "-", "NIT de empresa"),
            ("validacion.nit_bd", "Si" if nit_validado_bd else "No", "Validacion en BD"),
            (
                "seccion2.empresa_bd",
                empresa_bd_nombre or "-",
                "Empresa encontrada en BD",
            ),
            (
                "seccion2.nombre_empresa",
                nombre_empresa or "(se resuelve por NIT en BD)",
                "Nombre empresa",
            ),
            ("seccion3.fecha_servicio", fecha or "-", "Fecha servicio"),
            (
                "seccion1.nombre_profesional",
                (resolved_prof or raw_prof or "-"),
                "Profesional (match en lista)",
            ),
            ("seccion3.modalidad_servicio", modalidad or "-", "Modalidad (si aplica)"),
            ("seccion4.total_participantes", str(len(participantes)), "Oferentes"),
        ]
        for row in rows:
            mapping_tree.insert("", "end", values=row)

        parts_wrap = ttk.LabelFrame(dialog, text="Participantes a importar", padding=(8, 8))
        parts_wrap.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 8))
        parts_wrap.grid_rowconfigure(0, weight=1)
        parts_wrap.grid_columnconfigure(0, weight=1)

        parts_tree = ttk.Treeview(parts_wrap, columns=("idx", "cedula"), show="headings")
        parts_tree.heading("idx", text="#")
        parts_tree.heading("cedula", text="Cedula")
        parts_tree.column("idx", width=50, anchor="center")
        parts_tree.column("cedula", width=260, anchor="w")
        parts_scroll = ttk.Scrollbar(parts_wrap, orient="vertical", command=parts_tree.yview)
        parts_tree.configure(yscrollcommand=parts_scroll.set)
        parts_tree.grid(row=0, column=0, sticky="nsew")
        parts_scroll.grid(row=0, column=1, sticky="ns")

        if participantes:
            for idx, persona in enumerate(participantes, start=1):
                parts_tree.insert(
                    "",
                    "end",
                    values=(
                        idx,
                        (persona.get("cedula_usuario") or "").strip(),
                    ),
                )
        else:
            parts_tree.insert("", "end", values=("-", "Sin cedulas detectadas"))

        warnings = list(parsed.get("warnings") or [])
        if warnings:
            warn_wrap = ttk.LabelFrame(dialog, text="Avisos", padding=(8, 6))
            warn_wrap.pack(fill=tk.X, padx=12, pady=(0, 8))
            for item in warnings[:6]:
                ttk.Label(warn_wrap, text=f"- {item}", foreground="#8A6D3B").pack(anchor="w")

        result = {"apply": False}

        def _apply() -> None:
            result["apply"] = True
            dialog.destroy()

        def _cancel() -> None:
            dialog.destroy()

        btns = ttk.Frame(dialog)
        btns.pack(fill=tk.X, padx=12, pady=(0, 12))
        tk.Button(btns, text="Cancelar", command=_cancel, bg=COLOR_PURPLE, fg="white", padx=10, pady=4).pack(
            side=tk.RIGHT
        )
        tk.Button(btns, text="Aplicar al formulario", command=_apply, bg=COLOR_TEAL, fg="white", padx=12, pady=4).pack(
            side=tk.RIGHT,
            padx=(0, 8),
        )

        if not self._wait_dialog_with_timeout(
            dialog,
            timeout_ms=120_000,
            timeout_message="La vista previa de importacion supero el tiempo de espera.",
        ):
            return False
        return bool(result["apply"])

    def _aplicar_importacion_acta(self, parsed: dict, participantes: list[dict]) -> None:
        nit = (parsed.get("nit_empresa") or "").strip()
        if nit:
            self.seccion2.nit_var.set(nit)
            self.seccion2._fetch_empresa(nit)
            if not self.seccion2.nombre_var.get().strip():
                self.seccion2.nombre_var.set((parsed.get("nombre_empresa") or "").strip())

        fecha = (parsed.get("fecha_servicio") or "").strip()
        if fecha:
            self.seccion3.set_fecha_servicio(fecha)

        selected_prof = str(parsed.get("_resolved_profesional") or "").strip()
        if not selected_prof:
            selected_prof = self._resolve_profesional_import(
                (parsed.get("nombre_profesional") or "").strip(),
                parsed.get("candidatos_profesional") or [],
            )
        if selected_prof:
            self.seccion1.prof_var.set(selected_prof)

        modalidad = (parsed.get("modalidad_servicio") or "").strip()
        if modalidad and not self.seccion3.modalidad_var.get().strip():
            self.seccion3.modalidad_var.set(modalidad)

        if participantes:
            self.seccion4.clear_rows()
            for persona in participantes:
                self.seccion4._add_row()
                row = self.seccion4.rows[-1]
                ced = (persona.get("cedula_usuario") or "").strip()
                if ced:
                    row.cedula_var.set(ced)
                    self.seccion4._fill_user(row)
        else:
            self.seccion4.reset_for_new_entry()

        self._refresh_summary()

    def _flush_google_drive_queue(self) -> None:
        loading = LoadingDialog(self.root, "Reintentando sincronizacion Drive...", determinate=True)
        self.root.update_idletasks()
        loading.set_status("Procesando pendientes...", 60)

        def _worker() -> dict:
            return self.api.post("/wizard/google-drive/flush", {})

        def _on_success(response: dict) -> None:
            loading.set_status("Actualizando estado...", 90)
            loading.set_status("Finalizando...", 100)
            loading.close()
            data = response.get("data", {})
            self._update_queue_status()
            messagebox.showinfo(
                "Sincronizacion Drive",
                (
                    "Procesados: "
                    f"{data.get('procesados', 0)} | Pendientes: {data.get('pendientes', 0)}"
                ),
            )

        def _on_error(exc: Exception) -> None:
            loading.close()
            self._report_error("No se pudo reintentar la cola Drive", exc)

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=120,
            timeout_message="La re-ejecucion de cola Drive excedio el tiempo esperado.",
            operation_name="flush_google_drive_queue",
        )

    def _flush_google_drive_queue_silent(self) -> None:
        def _worker() -> dict:
            return self.api.post("/wizard/google-drive/flush", {})

        def _on_success(_response: dict) -> None:
            self._update_queue_status()

        def _on_error(exc: Exception) -> None:
            _LOGGER.warning("No se pudo procesar cola Drive en segundo plano: %s", exc)

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=120,
            timeout_message="La sincronizacion inicial de Drive excedio el tiempo esperado.",
            operation_name="flush_google_drive_queue_silent",
            disable_main_actions=False,
        )

    def _refresh_cache_from_supabase(self) -> None:
        dialog = LoadingDialog(self.root, "Actualizando base de datos...", determinate=True)
        timeout_sec = 60

        def _status(message: str, progress: int | None = None) -> None:
            self.root.after(0, lambda: dialog.set_status(message, progress))

        def _worker() -> dict:
            _status("Conectando a Supabase...", 0)
            self.api.reset_runtime_caches()
            return self.api.build_cache(INITIAL_PREFETCH_ITEMS, status_callback=_status)

        def _on_success(new_cache: dict) -> None:
            dialog.close()
            self.api.replace_cache(new_cache)
            messagebox.showinfo("Base de datos actualizada", "Datos actualizados correctamente.")

        def _on_error(exc: Exception) -> None:
            dialog.close()
            if isinstance(exc, TimeoutError):
                messagebox.showerror(
                    "Supabase no disponible",
                    "Supabase no respondio en 60 segundos. Intentalo mas tarde.",
                )
                return
            self._report_error("No se pudo actualizar la base de datos", exc, title="Error")

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=timeout_sec,
            timeout_message="Supabase no respondio en el tiempo esperado.",
            operation_name="refresh_cache_supabase",
        )

    def _open_update_page(self) -> None:
        dialog = LoadingDialog(self.root, "Verificando actualizacion...", determinate=True)
        self.root.update_idletasks()

        def _worker() -> tuple[str, str | None, dict]:
            from app.updater import get_latest_release_assets
            from app.version import get_version

            local = get_version()
            remote, assets = get_latest_release_assets()
            return local, remote, assets

        def _on_success(result: tuple[str, str | None, dict]) -> None:
            dialog.close()
            local, remote, assets = result
            if remote:
                self.set_version_info(local, remote)
            if not remote:
                messagebox.showerror("Actualizacion", "No se pudo obtener la version remota.")
                return
            from app.updater import is_update_available

            if not is_update_available(local, remote):
                messagebox.showinfo("Actualizacion", "Ya estas usando la ultima version.")
                return
            confirm = messagebox.askyesno(
                "Actualizacion disponible",
                f"Hay una nueva version disponible ({remote}).\n"
                "Deseas actualizar ahora?",
            )
            if not confirm:
                return
            self._start_manual_update(assets or {})

        def _on_error(exc: Exception) -> None:
            dialog.close()
            self._report_error("No se pudo verificar la actualizacion", exc, title="Actualizacion")

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            poll_ms=200,
            operation_name="check_update",
        )

    def _start_manual_update(self, assets: dict) -> None:
        dialog = LoadingDialog(self.root, "Descargando instalador...", determinate=True)
        self.root.update_idletasks()

        def _progress(message: str, value: int) -> None:
            self.root.after(0, lambda: dialog.set_status(message, value))

        def _worker() -> object:
            from app.updater import download_installer, run_installer

            path = download_installer(assets, progress_callback=_progress)
            self.root.after(0, lambda: dialog.set_mode(False))
            self.root.after(0, lambda: dialog.set_status("Instalando actualizacion..."))
            run_installer(path, wait=True)
            return path

        def _on_success(_path) -> None:
            dialog.close()
            self._show_restart_countdown()

        def _on_error(exc: Exception) -> None:
            dialog.close()
            self._report_error("No se pudo actualizar la aplicacion", exc, title="Actualizacion")

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            poll_ms=300,
            operation_name="manual_update_install",
        )

    def _show_restart_countdown(self, seconds: int = 5) -> None:
        dialog = tk.Toplevel(self.root)
        dialog.title("Actualizacion completada")
        dialog.resizable(False, False)
        dialog.configure(bg="white")
        dialog.transient(self.root)
        dialog.grab_set()

        container = tk.Frame(dialog, bg="white")
        container.pack(fill=tk.BOTH, expand=True, padx=24, pady=20)

        logo = _load_logo(subsample=7)
        if logo:
            tk.Label(container, image=logo, bg="white").pack(pady=(0, 8))
            dialog.logo_image = logo

        tk.Label(
            container,
            text="Instalacion terminada",
            font=("Arial", 12, "bold"),
            bg="white",
            fg=COLOR_PURPLE,
        ).pack(pady=(0, 6))

        countdown_label = tk.Label(
            container,
            text="Reiniciando en 5 segundos...",
            font=("Arial", 10),
            bg="white",
            fg=COLOR_TEAL,
        )
        countdown_label.pack()

        dialog.update_idletasks()
        self._center_dialog(dialog, 420, 200)

        def _tick(remaining: int) -> None:
            if remaining <= 0:
                dialog.destroy()
                self._restart_app()
                return
            countdown_label.config(text=f"Reiniciando en {remaining} segundos...")
            dialog.after(1000, lambda: _tick(remaining - 1))

        _tick(seconds)

    def _restart_app(self) -> None:
        try:
            if getattr(sys, "frozen", False):
                args = [sys.executable]
            else:
                script_path = os.path.abspath(__file__)
                args = [sys.executable, script_path]
            subprocess.Popen(args, close_fds=True)
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            _LOGGER.exception("No se pudo reiniciar la aplicacion: %s", exc)
        self.root.after(200, self.root.destroy)

    def _center_dialog(self, dialog: tk.Toplevel, width: int, height: int) -> None:
        dialog.update_idletasks()
        screen_w = dialog.winfo_screenwidth()
        screen_h = dialog.winfo_screenheight()
        x = int((screen_w - width) / 2)
        y = int((screen_h - height) / 2)
        dialog.geometry(f"{width}x{height}+{x}+{y}")

    def _update_queue_status(self) -> None:
        try:
            data = self.api.get("/wizard/google-drive/status").get("data", {})
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            _LOGGER.warning("No se pudo consultar estado de cola Drive: %s", exc)
            return
        pendientes = int(data.get("pendientes", 0) or 0)
        resumen = getattr(self, "resumen", None)
        if resumen and getattr(resumen, "queue_label", None):
            status = f"Cola Drive: {pendientes} pendiente(s)"
            try:
                if resumen.queue_label.winfo_exists():
                    resumen.queue_label.config(text=status)
            except tk.TclError:
                _LOGGER.debug("No se pudo actualizar queue_label en resumen (widget no disponible).")
                return

    def _load_section_data(self) -> None:
        self.seccion1.load_data()
        self.seccion2.load_data()
        self.seccion3.load_data()
        self.seccion4.load_data()

    def _reset_for_new_entry(self) -> None:
        self.seccion1.reset_for_new_entry()
        self.seccion2.reset_for_new_entry()
        self.seccion3.reset_for_new_entry()
        self.seccion4.reset_for_new_entry()
        self.seccion5.reset_for_new_entry()

    def _lock_sections(self) -> None:
        self.seccion1.set_enabled(True)
        self.seccion2.set_enabled(True)
        self.seccion3.set_enabled(True)
        self.seccion4.set_enabled(True)
        self.seccion5.set_enabled(True)
        self.resumen.grid()

    def _bind_summary_updates(self) -> None:
        def sync_interprete(*_args):
            self.seccion3.set_interprete_required(self.seccion1.selected_profesional_is_interprete())
            update()

        def update(*_args):
            if self._summary_after_id is not None:
                self.root.after_cancel(self._summary_after_id)
            self._summary_after_id = self.root.after(300, self._refresh_summary)

        self.seccion1.prof_var.trace_add("write", sync_interprete)
        self.seccion2.nombre_var.trace_add("write", update)
        self.seccion3.fecha_dia_var.trace_add("write", update)
        self.seccion3.fecha_mes_var.trace_add("write", update)
        self.seccion3.fecha_ano_var.trace_add("write", update)
        self.seccion3.codigo_var.trace_add("write", update)
        self.seccion3.horas_var.trace_add("write", update)
        self.seccion3.minutos_var.trace_add("write", update)
        self.seccion3.interpretacion_var.trace_add("write", update)
        self.seccion3.total_calculado_var.trace_add("write", update)
        sync_interprete()

    def _refresh_summary(self) -> None:
        self._summary_after_id = None
        self.seccion3.ensure_tarifa_loaded()
        data = {
            "fecha_servicio": self.seccion3.get_fecha_servicio(validate=False),
            "nombre_profesional": self.seccion1.prof_var.get().strip(),
            "nombre_empresa": self.seccion2.nombre_var.get().strip(),
            "codigo_servicio": self.seccion3.codigo_var.get().strip(),
            "valor_total": "",
        }
        try:
            result = calcular_servicio(self.seccion3._build_calculo_input())
            if self.seccion3.interpretacion_var.get():
                self.seccion3.horas_decimal_var.set(f"{(result.horas_interprete or Decimal('0')):.2f}")
                self.seccion3.total_calculado_var.set(format_currency(result.valor_total))
                data["valor_total"] = result.valor_total
            else:
                data["valor_total"] = result.valor_total if result.valor_total else ""
        except ValueError:
            base = safe_decimal(self.seccion3.valor_base_var.get() or 0).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
            data["valor_total"] = base if base else ""
        self.resumen.update_summary(data)

    def _build_ods_payload(self) -> dict:
        ods = {}
        for key in ["seccion1", "seccion2", "seccion3", "seccion4", "seccion5"]:
            ods.update(self.state.secciones.get(key, {}))
        return ods

    def _validar_secciones(self) -> dict:
        self._log_creation_flow("validacion_secciones_inicio")
        self.seccion3.ensure_tarifa_loaded()
        self._log_creation_flow("validando_seccion_1")
        seccion1 = self.api.post("/wizard/seccion-1/confirmar", self.seccion1.get_payload())["data"]
        self._log_creation_flow("validando_seccion_2")
        seccion2 = self.api.post("/wizard/seccion-2/confirmar", self.seccion2.get_payload())["data"]
        self._log_creation_flow("validando_seccion_3")
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
        self._log_creation_flow("validando_seccion_4")
        seccion4 = self.api.post("/wizard/seccion-4/confirmar", self.seccion4.get_payload())["data"]
        self._log_creation_flow("validando_seccion_5")
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
        self._log_creation_flow("validacion_secciones_ok")
        return self._build_ods_payload()

    def terminar_servicio(self) -> None:
        self._log_creation_flow("terminar_servicio_click")
        loading = LoadingDialog(self.root, "Preparando servicio...")
        self.root.update_idletasks()
        try:
            ods = self._validar_secciones()
            self._log_creation_flow("payload_ods_generado", campos=len(ods.keys()))
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            loading.close()
            self._log_creation_flow("error_validacion", error=str(exc))
            self._report_error("No se pudo validar la informacion", exc)
            return

        try:
            resumen = self.api.post("/wizard/resumen-final", {"ods": ods}, timeout=30)["data"]
            self.resumen.update_summary(resumen)
            self._log_creation_flow("resumen_generado")
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            loading.close()
            self._log_creation_flow("error_resumen", error=str(exc))
            self._report_error("No se pudo generar el resumen", exc)
            return
        loading.close()

        continuar = messagebox.askyesno(
            "Confirmar servicio",
            "Resumen generado. Deseas terminar y guardar el servicio?",
        )
        if not continuar:
            self._finish_creation_trace("cancelado_por_usuario")
            return

        loading = LoadingDialog(self.root, "Guardando servicio en la base de datos...")
        self.root.update_idletasks()
        loading.set_status("Guardando servicio...", None)
        payload = {"ods": ods, "usuarios_nuevos": self.state.usuarios_nuevos}
        self._log_creation_flow(
            "guardado_bd_inicio",
            usuarios_nuevos=len(self.state.usuarios_nuevos),
        )

        def _worker() -> dict:
            return self.api.post("/wizard/terminar-servicio", payload, timeout=120)

        def _on_success(response: dict) -> None:
            loading.close()
            sync_status = response.get("sync_status")
            sync_error = response.get("sync_error")
            sync_target = response.get("sync_target")
            self._log_creation_flow(
                "guardado_bd_ok",
                sync_status=sync_status,
                sync_error=sync_error,
                sync_target=sync_target,
            )
            if sync_status == "ok":
                message = "Servicio guardado en Supabase y sincronizado en Google Drive."
            elif sync_status == "pending":
                message = (
                    "Servicio guardado en Supabase. "
                    "La sincronizacion a Google Drive quedo pendiente de reintento."
                )
            else:
                message = (
                    "Servicio guardado en Supabase, pero la sincronizacion a Google Drive "
                    "no se pudo completar."
                )
            if sync_target:
                message += f"\nArchivo destino: {sync_target}"
            if sync_error:
                message += f"\nDetalle: {sync_error}"
            messagebox.showinfo("Servicio guardado", message)
            self._flush_google_drive_queue_silent()

            add_another = messagebox.askyesno(
                "Servicio terminado",
                "Servicio guardado. Deseas agregar otro servicio?",
            )
            if add_another:
                self._finish_creation_trace("servicio_guardado_agregar_otro")
                self.start_new_service()
            else:
                self._finish_creation_trace("servicio_guardado_fin")
                self.show_initial_screen()

        def _on_error(exc: Exception) -> None:
            loading.close()
            self._finish_creation_trace("error_guardado_bd", error=str(exc))
            self._report_error("No se pudo terminar el servicio", exc)

        self._run_background_task(
            _worker,
            _on_success,
            _on_error,
            timeout_sec=180,
            timeout_message="El guardado del servicio excedio el tiempo esperado.",
            poll_ms=200,
            operation_name="terminar_servicio_guardar",
            disable_main_actions=False,
        )

def main() -> None:
    try:
        from app.storage import ensure_appdata_files

        ensure_appdata_files()
    except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
        _LOGGER.warning("No se pudo asegurar estructura local de appdata: %s", exc)

    root = tk.Tk()
    root.withdraw()
    splash = StartupSplash(root)
    try:
        splash.deiconify()
    except tk.TclError:
        _LOGGER.debug("No se pudo mostrar splash con deiconify.")
    splash.lift()
    splash.update_idletasks()
    splash.update()
    api = ApiClient(None)

    start_time = time.time()
    while (time.time() - start_time) < 0.35:
        root.update_idletasks()
        root.update()
        time.sleep(0.01)

    splash.close()
    root.deiconify()
    screen_h = root.winfo_screenheight()
    base_font_size = 13 if screen_h >= 900 else 12 if screen_h >= 800 else 11
    default_font = tkfont.nametofont("TkDefaultFont")
    default_font.configure(size=base_font_size)
    root.option_add("*Font", default_font)
    style = ttk.Style(root)
    try:
        style.theme_use("clam")
    except tk.TclError:
        _LOGGER.debug("No se pudo aplicar tema 'clam' en ventana principal.")
    style.configure("TFrame", background="white")
    style.configure("TLabel", background="white")
    style.configure("TCombobox", padding=2)
    style.map("TEntry", fieldbackground=[("readonly", "white")], foreground=[("readonly", "black")])
    style.map("TCombobox", fieldbackground=[("readonly", "white")], foreground=[("readonly", "black")])
    style.configure("Highlight.TFrame", background="#FFF2CC")
    root.configure(bg="white")
    app = WizardApp(root, api)
    try:
        from app.version import get_version

        app.set_version_info(get_version(), None)
    except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
        _LOGGER.warning("No se pudo leer version local: %s", exc)

    def _on_close():
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)

    def _run_update() -> None:
        try:
            from app.updater import get_latest_version
            from app.version import get_version

            local = get_version()
            remote = get_latest_version()
            root.after(0, lambda: app.set_version_info(local, remote))
        except (RuntimeError, ValueError, TypeError, OSError, tk.TclError) as exc:
            _LOGGER.warning("No se pudo consultar version remota: %s", exc)

    threading.Thread(target=_run_update, daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    main()
