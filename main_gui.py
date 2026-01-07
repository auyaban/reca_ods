import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import date

import tkinter as tk
from tkinter import messagebox, ttk
import tkinter.font as tkfont

import threading


COLOR_PURPLE = "#7C3D96"
COLOR_TEAL = "#07B499"
_LOGO_PATH = None
_LOGO_CACHE: dict[int, tk.PhotoImage] = {}
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
    def __init__(self, _base_url: str | None = None) -> None:
        from app.services import wizard_service

        self._svc = wizard_service
        self._cache: dict[tuple[str, str, tuple[tuple[str, str], ...] | None], dict] = {}

    def _cache_key(self, path: str, params: dict | None) -> tuple[str, str, tuple[tuple[str, str], ...] | None]:
        if params:
            normalized = tuple(sorted((str(k), str(v)) for k, v in params.items()))
        else:
            normalized = None
        return ("GET", path, normalized)

    def get(self, path: str, params: dict | None = None, use_cache: bool = False) -> dict:
        cache_key = self._cache_key(path, params) if use_cache else None
        if cache_key and cache_key in self._cache:
            return self._cache[cache_key]
        try:
            data = self._dispatch_get(path, params or {})
            if cache_key:
                self._cache[cache_key] = data
            return data
        except Exception as exc:
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
        new_cache: dict[tuple[str, str, tuple[tuple[str, str], ...] | None], dict] = {}
        for index, (path, params, label) in enumerate(items, start=1):
            if status_callback:
                progress = int((index / max(total, 1)) * 100)
                status_callback(f"Cargando {label}...", progress)
            data = self._dispatch_get(path, params or {})
            key = self._cache_key(path, params)
            new_cache[key] = data
        return new_cache

    def replace_cache(self, new_cache: dict) -> None:
        self._cache = new_cache

    def post(self, path: str, payload: dict | None = None, timeout: int | float = 10) -> dict:
        try:
            return self._dispatch_post(path, payload or {})
        except Exception as exc:
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
        if path == "/wizard/editar/buscar":
            return self._svc.buscar_entradas(params)
        if path == "/wizard/editar/entrada":
            return self._svc.obtener_entrada(params)
        if path == "/wizard/editar/excel/status":
            return self._svc.excel_status()
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
        if path == "/wizard/editar/actualizar":
            return self._svc.actualizar_entrada(payload)
        if path == "/wizard/editar/eliminar":
            return self._svc.eliminar_entrada(payload)
        if path == "/wizard/editar/excel/flush":
            return self._svc.excel_flush()
        if path == "/wizard/editar/excel/rebuild":
            return self._svc.excel_rebuild()
        if path == "/wizard/facturas/crear":
            return self._svc.crear_factura(payload)
        raise RuntimeError(f"Endpoint no soportado: POST {path}")


class ScrollableFrame(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent)
        self.canvas = tk.Canvas(self, highlightthickness=0, bg="white")
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
        try:
            if not self.canvas.winfo_exists():
                return
        except tk.TclError:
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
        prof_labels = [item["nombre_profesional"] for item in prof_data["data"]]
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

            nuevo = data.get("data")
            nombre = ""
            if isinstance(nuevo, dict):
                nombre = nuevo.get("nombre_profesional", "").strip()
            elif isinstance(nuevo, list) and nuevo:
                nombre = (nuevo[0].get("nombre_profesional") or "").strip()
            self.api.invalidate("/wizard/seccion-1/profesionales")
            prof_data = self.api.get("/wizard/seccion-1/profesionales")
            prof_labels = sorted([item["nombre_profesional"] for item in prof_data["data"]])
            self.prof_combo.configure(values=prof_labels)
            self.prof_combo._all_values = prof_labels
            if nombre:
                self.prof_var.set(nombre)
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
        self.nit_combo.bind("<KeyRelease>", self._on_nit_typed)
        self.nit_combo.bind("<Return>", self._on_nit_confirm)
        self.nit_combo.bind("<FocusOut>", self._on_nit_confirm)
        configure_combobox(self.nit_combo)

        ttk.Label(self.body, text="Nombre Empresa").grid(row=1, column=0, sticky="w")
        self.nombre_combo = ttk.Combobox(self.body, textvariable=self.nombre_var, state="normal", width=50)
        self.nombre_combo.grid(row=1, column=1, sticky="w")
        self.nombre_combo.bind("<<ComboboxSelected>>", self._on_nombre_selected)
        self.nombre_combo.bind("<KeyRelease>", self._on_nombre_typed)
        self.nombre_combo.bind("<Return>", self._on_nombre_confirm)
        self.nombre_combo.bind("<FocusOut>", self._on_nombre_confirm)
        configure_combobox(self.nombre_combo)

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
        if self._nits:
            self.nit_combo.set(self._nits[0])
            self._fetch_empresa(self._nits[0])

    def _normalize_nombre(self, nombre: str) -> str:
        return " ".join(nombre.strip().lower().split())

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
        if nombre:
            self._fetch_empresa_por_nombre(nombre)

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
        self.sede_var.set(empresa.get("sede_empresa", ""))
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
        self.sede_var.set(empresa.get("sede_empresa", ""))
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
        self.fecha_ingreso_var.set("")
        if hasattr(self.fecha_ingreso_widget, "delete"):
            try:
                self.fecha_ingreso_widget.delete(0, "end")
            except tk.TclError:
                pass

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
        self._version_var = tk.StringVar()

        self.root.title("SISTEMA DE GESTIÓN ODS - RECA")
        self._set_window_size()

        self.header = tk.Frame(self.root, bg=COLOR_PURPLE, height=70)
        self.header.pack(fill=tk.X)
        self.header.grid_columnconfigure(0, weight=1)
        self.header.grid_columnconfigure(1, weight=0)
        self.header.grid_columnconfigure(2, weight=1)
        self.header_logo = _load_logo(subsample=8)
        logo_width = 0
        if self.header_logo:
            logo_width = self.header_logo.width()
            tk.Label(self.header, image=self.header_logo, bg=COLOR_PURPLE).grid(
                row=0, column=0, padx=(16, 6), pady=8, sticky="w"
            )
        title_frame = tk.Frame(self.header, bg=COLOR_PURPLE)
        title_frame.grid(row=0, column=1, pady=10, sticky="n")
        tk.Label(
            title_frame,
            text="SISTEMA DE GESTION ODS",
            font=("Arial", 20, "bold"),
            bg=COLOR_PURPLE,
            fg="white",
        ).pack()
        tk.Label(
            title_frame,
            text="RECA",
            font=("Arial", 20, "bold"),
            bg=COLOR_PURPLE,
            fg="white",
        ).pack()
        spacer = tk.Frame(self.header, bg=COLOR_PURPLE, width=logo_width)
        spacer.grid(row=0, column=2, padx=16, sticky="e")
        spacer.grid_propagate(False)

        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=16, pady=16)

        self.footer = ttk.Frame(self.root)
        self.footer.pack(fill=tk.X, padx=12, pady=(0, 6), side=tk.BOTTOM)
        self._version_var.set("Version local: - | GitHub: -")
        version_box = ttk.Frame(self.footer)
        version_box.pack(side=tk.LEFT, anchor="w")
        ttk.Label(
            version_box,
            textvariable=self._version_var,
            font=("Arial", 9),
            foreground="#666666",
        ).pack(anchor="w")
        tk.Button(
            version_box,
            text="Actualizar Versión de la Aplicación",
            command=self._open_update_page,
            bg="#4B8BBE",
            fg="white",
            font=("Arial", 10, "bold"),
            padx=10,
            pady=4,
        ).pack(anchor="w", pady=(4, 0))

        self.show_initial_screen()

    def set_version_info(self, local_version: str, remote_version: str | None) -> None:
        remote_label = remote_version or "-"
        self._version_var.set(f"Version local: {local_version} | GitHub: {remote_label}")

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
            width=28,
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
            width=28,
        ).pack(pady=8)

        tk.Button(
            self.main_frame,
            text="Crear factura",
            command=self._open_factura_dialog,
            bg="#4B8BBE",
            fg="white",
            font=("Arial", 12, "bold"),
            padx=16,
            pady=8,
            width=28,
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
            width=28,
        ).pack(pady=8)

        tk.Button(
            self.main_frame,
            text="Actualizar Base de Datos",
            command=self._refresh_cache_from_supabase,
            bg="#2E86C1",
            fg="white",
            font=("Arial", 12, "bold"),
            padx=16,
            pady=8,
            width=28,
        ).pack(pady=8)

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

    def _open_factura_dialog(self) -> None:
        dialog = tk.Toplevel(self.root)
        dialog.title("Crear factura")
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
        tipo_var = tk.StringVar(value="No clausulada")

        hoy = date.today()
        if 1 <= hoy.month <= 12:
            mes_var.set(meses[hoy.month - 1])
        ano_var.set(str(hoy.year))

        ttk.Label(container, text="Mes").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Combobox(container, textvariable=mes_var, values=meses, width=20, state="readonly").grid(
            row=0, column=1, sticky="w", pady=4
        )

        ttk.Label(container, text="Año").grid(row=1, column=0, sticky="w", pady=4)
        anos = [str(year) for year in range(2020, 2031)]
        ttk.Combobox(container, textvariable=ano_var, values=anos, width=10, state="readonly").grid(
            row=1, column=1, sticky="w", pady=4
        )

        ttk.Label(container, text="Tipo").grid(row=2, column=0, sticky="w", pady=4)
        ttk.Combobox(
            container,
            textvariable=tipo_var,
            values=["Clausulada", "No clausulada"],
            width=20,
            state="readonly",
        ).grid(row=2, column=1, sticky="w", pady=4)

        button_row = ttk.Frame(container)
        button_row.grid(row=3, column=0, columnspan=2, pady=(12, 0), sticky="e")

        def _confirmar():
            mes_nombre = mes_var.get().strip()
            ano_raw = ano_var.get().strip()
            tipo_raw = tipo_var.get().strip().lower()
            if mes_nombre not in meses:
                messagebox.showerror("Error", "Selecciona un mes valido")
                return
            try:
                ano = int(ano_raw)
            except ValueError:
                messagebox.showerror("Error", "Selecciona un año valido")
                return
            mes = meses.index(mes_nombre) + 1
            dialog.destroy()
            self._crear_factura(mes, ano, tipo_raw)

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
            text="Crear factura",
            command=_confirmar,
            bg=COLOR_TEAL,
            fg="white",
            padx=10,
            pady=4,
        ).pack(side=tk.RIGHT)

    def _crear_factura(self, mes: int, ano: int, tipo: str) -> None:
        loading = LoadingDialog(self.root, "Generando factura...")
        self.root.update_idletasks()
        try:
            self.api.post(
                "/wizard/facturas/crear",
                {"mes": mes, "ano": ano, "tipo": tipo},
                timeout=300,
            )
        except Exception as exc:
            loading.close()
            messagebox.showerror("Error", f"No se pudo crear la factura: {exc}")
            return
        loading.close()
        messagebox.showinfo("Factura creada", "La factura se genero en el Excel.")

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

            main_col = ttk.Frame(self.scroll.content)
            main_col.grid(row=0, column=0, sticky="nsew")
            main_col.grid_columnconfigure(0, weight=1)

            nav = ttk.Frame(main_col)
            nav.grid(row=0, column=0, sticky="ew", pady=(0, 6))
            tk.Button(
                nav,
                text="Volver",
                command=self.show_initial_screen,
                bg=COLOR_PURPLE,
                fg="white",
                padx=12,
                pady=4,
            ).pack(side="left")

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

            self.resumen = ResumenFrame(main_col, self.terminar_servicio, self._flush_excel_queue)
            self.resumen.grid(row=6, column=0, sticky="ew", pady=8)

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

            main_col = ttk.Frame(self.scroll.content)
            main_col.grid(row=0, column=0, sticky="nsew")
            main_col.grid_columnconfigure(0, weight=1)

            self.seccion1 = Seccion1Frame(main_col, self.api, self.state)
            self.seccion1.grid(row=0, column=0, sticky="ew", pady=8)

            self.seccion2 = Seccion2Frame(main_col, self.api)
            self.seccion2.grid(row=1, column=0, sticky="ew", pady=8)

            self.seccion3 = Seccion3Frame(main_col, self.api)
            self.seccion3.grid(row=2, column=0, sticky="ew", pady=8)

            self.seccion4 = Seccion4Frame(main_col, self.api, self.state)
            self.seccion4.grid(row=3, column=0, sticky="ew", pady=8)

            self.seccion5 = Seccion5Frame(main_col, self.api)
            self.seccion5.grid(row=4, column=0, sticky="ew", pady=8)

            self.resumen = ResumenFrame(
                main_col,
                self.terminar_servicio,
                self._flush_excel_queue,
                show_terminar=False,
            )
            self.resumen.grid(row=5, column=0, sticky="ew", pady=8)

            self.edit_actions = ttk.Frame(self.scroll.content)
            self.edit_actions.grid(row=1, column=0, columnspan=1, sticky="w", pady=8)
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
        label = getattr(self, "queue_status_label", None)
        if label and label.winfo_exists():
            try:
                label.config(
                    text=(
                        f"Cola Excel: procesados {data.get('procesados', 0)} "
                        f"/ pendientes {data.get('pendientes', 0)}"
                    )
                )
            except tk.TclError:
                pass
        self._update_queue_status()

    def _refresh_cache_from_supabase(self) -> None:
        dialog = LoadingDialog(self.root, "Actualizando base de datos...", determinate=True)
        start_time = time.time()
        timeout_sec = 60
        result = {"cache": None, "error": None}

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

        def _status(message: str, progress: int | None = None) -> None:
            self.root.after(0, lambda: dialog.set_status(message, progress))

        def _worker() -> None:
            try:
                _status("Conectando a Supabase...", 0)
                new_cache = self.api.build_cache(items, status_callback=_status)
                result["cache"] = new_cache
            except Exception as exc:
                result["error"] = str(exc)

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

        def _check_done() -> None:
            if thread.is_alive():
                if time.time() - start_time >= timeout_sec:
                    dialog.close()
                    messagebox.showerror(
                        "Supabase no disponible",
                        "Supabase no respondio en 60 segundos. Intentalo mas tarde.",
                    )
                    return
                self.root.after(250, _check_done)
                return
            dialog.close()
            if result["error"]:
                messagebox.showerror(
                    "Error",
                    f"No se pudo actualizar la base de datos: {result['error']}",
                )
                return
            if result["cache"] is not None:
                self.api.replace_cache(result["cache"])
                messagebox.showinfo("Base de datos actualizada", "Datos actualizados correctamente.")

        self.root.after(250, _check_done)

    def _open_update_page(self) -> None:
        dialog = LoadingDialog(self.root, "Verificando actualizacion...", determinate=True)
        self.root.update_idletasks()
        result = {"error": None, "local": None, "remote": None, "assets": None}

        def _worker():
            try:
                from app.updater import get_latest_release_assets
                from app.version import get_version

                local = get_version()
                remote, assets = get_latest_release_assets()
                result["local"] = local
                result["remote"] = remote
                result["assets"] = assets
            except Exception as exc:
                result["error"] = str(exc)

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

        def _check_done():
            if thread.is_alive():
                self.root.after(200, _check_done)
                return
            dialog.close()
            if result["error"]:
                messagebox.showerror("Actualizacion", f"No se pudo verificar: {result['error']}")
                return
            local = result["local"] or "0.0.0"
            remote = result["remote"]
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
            self._start_manual_update(result["assets"] or {})

        self.root.after(200, _check_done)

    def _start_manual_update(self, assets: dict) -> None:
        dialog = LoadingDialog(self.root, "Descargando instalador...", determinate=True)
        self.root.update_idletasks()
        result = {"error": None, "path": None}

        def _progress(message: str, value: int) -> None:
            self.root.after(0, lambda: dialog.set_status(message, value))

        def _worker():
            try:
                from app.updater import download_installer, run_installer

                path = download_installer(assets, progress_callback=_progress)
                result["path"] = path
                self.root.after(0, lambda: dialog.set_mode(False))
                self.root.after(0, lambda: dialog.set_status("Instalando actualizacion..."))
                run_installer(path, wait=True)
            except Exception as exc:
                result["error"] = str(exc)

        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

        def _check_done():
            if thread.is_alive():
                self.root.after(300, _check_done)
                return
            dialog.close()
            if result["error"]:
                messagebox.showerror("Actualizacion", f"No se pudo actualizar: {result['error']}")
                return
            self._show_restart_countdown()

        self.root.after(300, _check_done)

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
        except Exception:
            pass
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
            data = self.api.get("/wizard/editar/excel/status").get("data", {})
        except Exception:
            return
        pendientes = int(data.get("pendientes", 0) or 0)
        locked = bool(data.get("locked"))
        resumen = getattr(self, "resumen", None)
        if resumen and getattr(resumen, "queue_label", None):
            status = f"Cola Excel: {pendientes} pendiente(s)"
            if locked:
                status += " (Excel en uso)"
            try:
                if resumen.queue_label.winfo_exists():
                    resumen.queue_label.config(text=status)
            except tk.TclError:
                return

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
                "Servicio guardado. El Excel se actualiza en segundo plano.",
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
        self.show_initial_screen()

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
        force_excel_sync = False
        if cambios:
            detalle = ", ".join(sorted(cambios))
            confirmar = messagebox.askyesno(
                "Confirmar cambios",
                f"Se actualizaran {len(cambios)} campos:\n{detalle}\n\nDeseas continuar?",
            )
            if not confirmar:
                return
        else:
            force_excel_sync = True
        loading = LoadingDialog(self.root, "Actualizando entrada...")
        self.root.update_idletasks()
        payload = {"filtro": {"id": self.edit_entry_id}, "datos": ods}
        if self.edit_original_entry:
            payload["original"] = self.edit_original_entry
        if force_excel_sync:
            payload["force_excel_sync"] = True
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
                "Entrada actualizada. El Excel se actualiza en segundo plano.",
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
                "Entrada eliminada. El Excel se actualiza en segundo plano.",
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
    try:
        from app.storage import ensure_appdata_files

        ensure_appdata_files()
    except Exception:
        pass

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
    start_time = time.time()
    api = ApiClient(None)
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
    try:
        from app.version import get_version

        app.set_version_info(get_version(), None)
    except Exception:
        pass

    def _on_close():
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _on_close)

    def _run_update():
        try:
            from app.updater import get_latest_version
            from app.version import get_version

            local = get_version()
            remote = get_latest_version()
            root.after(0, lambda: app.set_version_info(local, remote))
        except Exception:
            pass

    threading.Thread(target=_run_update, daemon=True).start()

    root.mainloop()


if __name__ == "__main__":
    main()
