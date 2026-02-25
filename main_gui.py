import os
import re
import subprocess
import sys
import time
import webbrowser
import difflib
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import tkinter as tk
from tkinter import filedialog, messagebox, ttk
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
        if path == "/wizard/monitor/entradas":
            return self._svc.listar_entradas_monitor(params)
        if path == "/wizard/actas-finalizadas":
            return self._svc.listar_actas_finalizadas(params)
        if path == "/wizard/actas-finalizadas/status":
            return self._svc.estado_actas_finalizadas()
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
        if path == "/wizard/actas-finalizadas/revisado":
            return self._svc.actualizar_acta_revisado(payload)
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

    def _on_canvas_configure(self, event) -> None:
        try:
            if self.canvas.winfo_exists():
                self.canvas.itemconfigure(self._content_window, width=event.width)
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
        prof_labels = [item["nombre_profesional"] for item in prof_data["data"]]
        self.prof_combo.configure(values=prof_labels)
        self.prof_combo._all_values = prof_labels

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

    def reset_for_new_entry(self) -> None:
        self.nit_var.set("")
        self.nombre_var.set("")
        self.caja_var.set("")
        self.asesor_var.set("")
        self.sede_var.set("")
        self.nit_combo.configure(values=self._nits)
        self.nombre_combo.configure(values=self._nombres)

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
        self._codigos_popup: tk.Toplevel | None = None

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

    def reset_for_new_entry(self) -> None:
        self.fecha_var.set("")
        try:
            if hasattr(self.fecha_widget, "delete"):
                self.fecha_widget.delete(0, "end")
        except tk.TclError:
            pass
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
        def _codigo_sort_key(value: str):
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


class LiveMonitorPanel(tk.Toplevel):
    COLUMNS = [
        ("id", "ID", 70),
        ("created_at_local", "CREADO (COL UTC-5)", 170),
        ("nombre_profesional", "PROFESIONAL", 160),
        ("codigo_servicio", "NUEVO CODIGO", 110),
        ("nombre_empresa", "EMPRESA", 180),
        ("nit_empresa", "NIT", 100),
        ("caja_compensacion", "CCF", 100),
        ("fecha_servicio", "FECHA", 110),
        ("referencia_servicio", "REFERENCIA", 120),
        ("descripcion_servicio", "NOMBRE", 160),
        ("nombre_usuario", "OFERENTES", 140),
        ("cedula_usuario", "CEDULA", 110),
        ("discapacidad_usuario", "TIPO DISCAPACIDAD", 140),
        ("fecha_ingreso", "FECHA INGRESO", 120),
        ("valor_virtual", "VALOR SERVICIO VIRTUAL", 140),
        ("valor_bogota", "VALOR SERVICIO BOGOTA", 140),
        ("valor_otro", "VALOR FUERA DE BOGOTA", 155),
        ("todas_modalidades", "TODAS LAS MODALIDADES", 165),
        ("horas_interprete", "TOTAL HORAS", 100),
        ("valor_interprete", "VALOR A PAGAR", 130),
        ("valor_total", "TOTAL VALOR SERVICIO", 155),
        ("observaciones", "OBSERVACIONES", 140),
        ("asesor_empresa", "ASESOR", 120),
        ("sede_empresa", "SEDE", 120),
        ("modalidad_servicio", "MODALIDAD", 130),
        ("observacion_agencia", "OBSERVACION AGENCIA", 165),
    ]
    EDITABLE_KEYS = {key for key, _title, _width in COLUMNS if key not in {"id", "created_at_local"}}
    DATE_KEYS = {"fecha_servicio", "fecha_ingreso"}
    MONEY_KEYS = {
        "valor_virtual",
        "valor_bogota",
        "valor_otro",
        "todas_modalidades",
        "valor_interprete",
        "valor_total",
    }
    INT_KEYS = {"horas_interprete"}

    def __init__(self, root: tk.Tk, api: ApiClient):
        super().__init__(root)
        self.root = root
        self.api = api
        self.title("Monitor en tiempo real")
        self.geometry("1280x640")
        self.configure(bg="white")
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._rows_by_id: dict[str, dict] = {}
        self._syncing = False
        self._server_total = 0
        self._server_loaded = 0
        self._sort_key: str | None = None
        self._sort_desc = False
        self._sort_buttons: dict[str, tk.Button] = {}
        self._filter_vars: dict[str, tk.StringVar] = {key: tk.StringVar() for key, _t, _w in self.COLUMNS}

        top = ttk.Frame(self, padding=(10, 8))
        top.pack(fill=tk.X)
        ttk.Label(
            top,
            text="Monitor editable (doble clic para editar, pendiente en amarillo)",
            font=("Arial", 11, "bold"),
        ).pack(side=tk.LEFT)

        self.pending_var = tk.StringVar(value="Cambios pendientes: 0")
        ttk.Label(top, textvariable=self.pending_var, foreground=COLOR_PURPLE).pack(side=tk.LEFT, padx=(14, 0))
        self.records_var = tk.StringVar(value="Registros en Supabase: - | Cargados: - | Filtrados: -")
        ttk.Label(top, textvariable=self.records_var, foreground="#2E86C1").pack(side=tk.LEFT, padx=(14, 0))

        btns = ttk.Frame(top)
        btns.pack(side=tk.RIGHT)
        tk.Button(btns, text="Refrescar", command=self.force_refresh, bg="#2E86C1", fg="white", padx=10, pady=3).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        tk.Button(
            btns,
            text="Descartar cambios",
            command=self.discard_changes,
            bg=COLOR_PURPLE,
            fg="white",
            padx=10,
            pady=3,
        ).pack(side=tk.LEFT, padx=(0, 6))
        tk.Button(
            btns,
            text="Guardar cambios",
            command=self.save_changes,
            bg=COLOR_TEAL,
            fg="white",
            padx=10,
            pady=3,
        ).pack(side=tk.LEFT)

        table_wrap = ttk.Frame(self)
        table_wrap.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.table_canvas = tk.Canvas(table_wrap, highlightthickness=0, bg="white")
        self.table_x_scroll = ttk.Scrollbar(table_wrap, orient="horizontal", command=self.table_canvas.xview)
        self.table_y_scroll = ttk.Scrollbar(table_wrap, orient="vertical", command=self.table_canvas.yview)
        self.table_canvas.configure(
            xscrollcommand=self.table_x_scroll.set,
            yscrollcommand=self.table_y_scroll.set,
        )
        self.table_canvas.grid(row=0, column=0, sticky="nsew")
        self.table_y_scroll.grid(row=0, column=1, sticky="ns")
        self.table_x_scroll.grid(row=1, column=0, sticky="ew")
        table_wrap.grid_rowconfigure(0, weight=1)
        table_wrap.grid_columnconfigure(0, weight=1)

        self.table_content = ttk.Frame(self.table_canvas)
        self.table_canvas.create_window((0, 0), window=self.table_content, anchor="nw")
        self.table_content.bind(
            "<Configure>",
            lambda _e: self.table_canvas.configure(scrollregion=self.table_canvas.bbox("all")),
        )

        header = ttk.Frame(self.table_content)
        for key, title, width in self.COLUMNS:
            col_w = max(8, int(width / 10))
            cell = ttk.Frame(header)
            cell.pack(side=tk.LEFT, padx=(0, 4))
            title_row = ttk.Frame(cell)
            title_row.pack(fill=tk.X)
            ttk.Label(title_row, text=title, width=max(6, col_w - 2), anchor="w").pack(side=tk.LEFT)
            sort_btn = tk.Button(
                title_row,
                text="↕",
                command=lambda k=key: self._toggle_sort(k),
                bg=COLOR_PURPLE,
                fg="white",
                padx=2,
                pady=0,
                width=2,
                font=("Arial", 8, "bold"),
            )
            sort_btn.pack(side=tk.LEFT, padx=(2, 0))
            self._sort_buttons[key] = sort_btn

            filter_entry = ttk.Entry(cell, textvariable=self._filter_vars[key], width=col_w)
            filter_entry.pack(fill=tk.X, pady=(2, 0))
            self._filter_vars[key].trace_add("write", lambda *_args: self._apply_filters_and_sort())

        header.pack(fill=tk.X)

        self.rows_container = ttk.Frame(self.table_content)
        self.rows_container.pack(fill=tk.X, expand=True)

        self._initial_load()

    def _on_close(self) -> None:
        self.destroy()

    def _initial_load(self) -> None:
        self.force_refresh()

    def _fetch_rows(self) -> tuple[list[dict], int, int]:
        payload = self.api.get("/wizard/monitor/entradas", params={"limit": 1000})
        rows = list(payload.get("data", []) or [])
        for row in rows:
            row["created_at_local"] = self._format_created_at_es(row.get("created_at"))
        total = int(payload.get("total", len(rows)) or 0)
        shown = int(payload.get("shown", len(rows)) or 0)
        return rows, total, shown

    def _format_created_at_es(self, value) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            dt = datetime.fromisoformat(raw)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            col_tz = timezone(timedelta(hours=-5))
            dt_col = dt.astimezone(col_tz)
            meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
            return (
                f"{dt_col.day:02d} {meses[dt_col.month - 1]} {dt_col.year} "
                f"{dt_col.hour:02d}:{dt_col.minute:02d}:{dt_col.second:02d}"
            )
        except Exception:
            return str(value)

    def force_refresh(self, silent: bool = False) -> None:
        if self._syncing:
            return
        self._syncing = True
        try:
            rows, total, shown = self._fetch_rows()
            self._merge_rows(rows)
            self._server_total = total
            self._server_loaded = shown
            self._apply_filters_and_sort()
            self._update_pending_label()
        except Exception as exc:
            if not silent:
                messagebox.showerror("Monitor", f"No se pudo refrescar: {exc}")
        finally:
            self._syncing = False

    def _add_row_widget(self, data: dict) -> None:
        row_id = str(data.get("id", "")).strip()
        if not row_id:
            return
        row_frame = ttk.Frame(self.rows_container)
        row_frame.pack(fill=tk.X, pady=1)

        row_state = {
            "id": row_id,
            "frame": row_frame,
            "orig": {},
            "vars": {},
            "entries": {},
            "dirty": set(),
            "suspend_trace": False,
            "created_at_raw": data.get("created_at"),
        }
        self._rows_by_id[row_id] = row_state

        for key, _title, width in self.COLUMNS:
            if key == "id":
                lbl = ttk.Label(row_frame, text=row_id, width=max(8, int(width / 10)), anchor="w")
                lbl.pack(side=tk.LEFT, padx=(0, 4))
                continue
            var = tk.StringVar()
            entry = tk.Entry(row_frame, textvariable=var, width=max(8, int(width / 10)), relief="solid", bd=1)
            entry.pack(side=tk.LEFT, padx=(0, 4))
            entry.configure(state="readonly", readonlybackground="white")
            if key in self.EDITABLE_KEYS:
                entry.bind("<Double-1>", lambda _e, k=key, st=row_state: self._open_cell_editor(st, k))
            row_state["vars"][key] = var
            row_state["entries"][key] = entry

            def _on_var_change(*_args, key_name=key, state=row_state):
                if state.get("suspend_trace"):
                    return
                self._mark_dirty_state(state, key_name)

            var.trace_add("write", _on_var_change)

        self._set_row_data(row_state, data, set_as_original=True)

    def _set_row_data(self, row_state: dict, data: dict, set_as_original: bool = False) -> None:
        row_state["suspend_trace"] = True
        row_state["created_at_raw"] = data.get("created_at")
        for key, _title, _width in self.COLUMNS:
            if key == "id":
                continue
            value = self._display_value(key, data.get(key))
            if key in row_state["vars"]:
                row_state["vars"][key].set(value)
            if set_as_original:
                row_state["orig"][key] = value
        row_state["suspend_trace"] = False
        if set_as_original:
            row_state["dirty"].clear()
            self._refresh_row_colors(row_state)

    def _mark_dirty_state(self, row_state: dict, key: str) -> None:
        if key not in row_state["vars"]:
            return
        current = row_state["vars"][key].get()
        original = row_state["orig"].get(key, "")
        if current != original:
            row_state["dirty"].add(key)
        else:
            row_state["dirty"].discard(key)
        self._refresh_row_colors(row_state)
        self._apply_filters_and_sort()
        self._update_pending_label()

    def _refresh_row_colors(self, row_state: dict) -> None:
        for key, entry in row_state["entries"].items():
            try:
                if key in row_state["dirty"]:
                    entry.configure(readonlybackground="#FFF59D")
                else:
                    entry.configure(readonlybackground="white")
            except tk.TclError:
                continue

    def _display_value(self, key: str, value) -> str:
        if value in (None, ""):
            return ""
        if key in self.MONEY_KEYS:
            try:
                return format_currency(float(value))
            except Exception:
                return str(value)
        if key in self.INT_KEYS:
            try:
                return str(int(float(value)))
            except Exception:
                return str(value)
        return str(value)

    def _parse_money(self, text: str) -> float:
        clean = str(text).replace("$", "").replace(",", "").strip()
        if not clean:
            return 0.0
        return float(clean)

    def _parse_for_save(self, key: str, text: str):
        if key in self.MONEY_KEYS:
            return self._parse_money(text)
        if key in self.INT_KEYS:
            clean = str(text).strip()
            if not clean:
                return 0
            return int(float(clean))
        return text.strip()

    def _open_cell_editor(self, row_state: dict, key: str) -> None:
        if key not in self.EDITABLE_KEYS:
            return
        current = row_state["vars"][key].get()
        dialog = tk.Toplevel(self)
        dialog.title(f"Editar {key}")
        dialog.resizable(False, False)
        dialog.transient(self)
        dialog.grab_set()

        frame = ttk.Frame(dialog, padding=12)
        frame.pack(fill=tk.BOTH, expand=True)
        ttk.Label(frame, text=f"Campo: {key}", font=("Arial", 10, "bold")).pack(anchor="w", pady=(0, 8))

        editor_var = tk.StringVar(value=current)
        editor_widget = None
        if key in self.DATE_KEYS:
            DateEntry = _get_date_entry()
            if DateEntry:
                editor_widget = DateEntry(frame, date_pattern="yyyy-mm-dd", textvariable=editor_var, width=18)
            else:
                editor_widget = ttk.Entry(frame, textvariable=editor_var, width=28)
        else:
            editor_widget = ttk.Entry(frame, textvariable=editor_var, width=36)
        editor_widget.pack(anchor="w")

        def _apply():
            raw = editor_var.get().strip()
            if key in self.DATE_KEYS and raw:
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
                    messagebox.showerror("Monitor", "La fecha debe estar en formato YYYY-MM-DD.")
                    return
            if key in self.MONEY_KEYS and raw:
                try:
                    raw = self._display_value(key, self._parse_money(raw))
                except Exception:
                    messagebox.showerror("Monitor", "Valor monetario invalido.")
                    return
            if key in self.INT_KEYS and raw:
                try:
                    raw = str(int(float(raw)))
                except Exception:
                    messagebox.showerror("Monitor", "El valor debe ser numerico entero.")
                    return
            row_state["vars"][key].set(raw)
            dialog.destroy()

        btns = ttk.Frame(frame)
        btns.pack(anchor="e", pady=(10, 0))
        tk.Button(btns, text="Cancelar", command=dialog.destroy, bg=COLOR_PURPLE, fg="white", padx=8, pady=3).pack(
            side=tk.RIGHT, padx=(8, 0)
        )
        tk.Button(btns, text="Aplicar", command=_apply, bg=COLOR_TEAL, fg="white", padx=8, pady=3).pack(side=tk.RIGHT)

    def _merge_rows(self, rows: list[dict]) -> None:
        seen_ids = set()
        for row in rows:
            row_id = str(row.get("id", "")).strip()
            if not row_id:
                continue
            seen_ids.add(row_id)
            if row_id not in self._rows_by_id:
                self._add_row_widget(row)
                continue
            state = self._rows_by_id[row_id]
            for key, _title, _width in self.COLUMNS:
                if key == "id" or key not in state["vars"]:
                    continue
                incoming = self._display_value(key, row.get(key))
                if key in state["dirty"]:
                    continue
                state["orig"][key] = incoming
                state["created_at_raw"] = row.get("created_at")
                state["suspend_trace"] = True
                state["vars"][key].set(incoming)
                state["suspend_trace"] = False
            self._refresh_row_colors(state)

        existing_ids = list(self._rows_by_id.keys())
        for row_id in existing_ids:
            if row_id in seen_ids:
                continue
            state = self._rows_by_id[row_id]
            if state["dirty"]:
                continue
            try:
                state["frame"].destroy()
            except tk.TclError:
                pass
            self._rows_by_id.pop(row_id, None)
        self._apply_filters_and_sort()

    def _toggle_sort(self, key: str) -> None:
        self._sort_key = "created_at_local"
        self._sort_desc = False
        self._apply_filters_and_sort()

    def _sort_value(self, row_state: dict, key: str):
        text = row_state["vars"].get(key).get() if key in row_state["vars"] else ""
        if key in self.MONEY_KEYS:
            try:
                return self._parse_money(text)
            except Exception:
                return 0.0
        if key in self.INT_KEYS:
            try:
                return int(float(text or 0))
            except Exception:
                return 0
        if key in self.DATE_KEYS:
            raw = (text or "").strip()
            if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
                return raw
            return ""
        return (text or "").strip().lower()

    def _apply_filters_and_sort(self) -> None:
        states = list(self._rows_by_id.values())

        def _matches(state: dict) -> bool:
            for key, var in self._filter_vars.items():
                needle = var.get().strip().lower()
                if not needle:
                    continue
                value = state["vars"].get(key).get().strip().lower() if key in state["vars"] else ""
                if needle not in value:
                    return False
            return True

        filtered = [state for state in states if _matches(state)]

        def _created_sort(state: dict):
            raw = state.get("created_at_raw")
            if raw in (None, ""):
                return datetime.min.replace(tzinfo=timezone.utc)
            txt = str(raw).strip()
            if txt.endswith("Z"):
                txt = txt[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(txt)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except Exception:
                return datetime.min.replace(tzinfo=timezone.utc)

        # Always oldest first, newest at the bottom.
        filtered.sort(key=_created_sort, reverse=False)

        for state in states:
            try:
                state["frame"].pack_forget()
            except tk.TclError:
                continue
        for state in filtered:
            try:
                state["frame"].pack(fill=tk.X, pady=1)
            except tk.TclError:
                continue

        for key, btn in self._sort_buttons.items():
            if key == "created_at_local":
                btn.configure(text="ASC", state="disabled")
            else:
                btn.configure(text="FIX", state="disabled")
        self.records_var.set(
            f"Registros en Supabase: {self._server_total} | Cargados: {self._server_loaded} | Filtrados: {len(filtered)}"
        )

    def _update_pending_label(self) -> None:
        dirty_cells = 0
        dirty_rows = 0
        for state in self._rows_by_id.values():
            if state["dirty"]:
                dirty_rows += 1
                dirty_cells += len(state["dirty"])
        self.pending_var.set(f"Cambios pendientes: {dirty_cells} celda(s) en {dirty_rows} fila(s)")

    def discard_changes(self) -> None:
        for state in self._rows_by_id.values():
            if not state["dirty"]:
                continue
            for key in list(state["dirty"]):
                original = state["orig"].get(key, "")
                state["vars"][key].set(original)
            state["dirty"].clear()
            self._refresh_row_colors(state)
        self._update_pending_label()

    def has_pending_changes(self) -> bool:
        return any(state["dirty"] for state in self._rows_by_id.values())

    def save_changes(self) -> None:
        pending = [state for state in self._rows_by_id.values() if state["dirty"]]
        if not pending:
            messagebox.showinfo("Monitor", "No hay cambios pendientes.")
            return

        ok = 0
        errors = []
        for state in pending:
            row_id = state["id"]
            datos = {}
            for key in state["dirty"]:
                try:
                    datos[key] = self._parse_for_save(key, state["vars"][key].get())
                except Exception:
                    errors.append(f"ID {row_id}: valor invalido en {key}")
                    continue
            if not datos:
                continue
            payload = {"filtro": {"id": row_id}, "datos": datos}
            try:
                self.api.post("/wizard/editar/actualizar", payload, timeout=120)
                for key in list(state["dirty"]):
                    state["orig"][key] = state["vars"][key].get()
                state["dirty"].clear()
                self._refresh_row_colors(state)
                ok += 1
            except Exception as exc:
                errors.append(f"ID {row_id}: {exc}")

        self._update_pending_label()
        if errors:
            preview = "\n".join(errors[:5])
            more = "\n..." if len(errors) > 5 else ""
            messagebox.showwarning(
                "Monitor",
                f"Guardadas {ok} fila(s). Fallaron {len(errors)}.\n{preview}{more}",
            )
        else:
            messagebox.showinfo("Monitor", f"Guardadas {ok} fila(s) correctamente.")


class ActasTerminadasPanel(tk.Toplevel):
    def __init__(self, root: tk.Tk, api: ApiClient, on_status_change=None):
        super().__init__(root)
        self.root = root
        self.api = api
        self.on_status_change = on_status_change
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
        except Exception:
            return text

    def _set_pending(self, pendientes: int) -> None:
        self.pending_var.set(f"Pendientes por revisar: {pendientes}")
        if self.on_status_change:
            try:
                self.on_status_change(pendientes)
            except Exception:
                pass

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
            revisado = bool(row.get("revisado"))
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
                    row.get("path_formato", "") or "",
                    "Si" if revisado else "No",
                ),
                tags=(tag,) if tag else (),
            )
            self._rows_by_item[iid] = row

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
        try:
            if re.match(r"^https?://", path, re.IGNORECASE):
                webbrowser.open(path)
                return
            normalized = os.path.expanduser(os.path.expandvars(path))
            if os.path.exists(normalized):
                os.startfile(normalized)
                return
            messagebox.showwarning("Actas Terminadas", f"La ruta no existe:\n{path}")
        except Exception as exc:
            messagebox.showerror("Actas Terminadas", f"No se pudo abrir la ruta: {exc}")

    def _toggle_revisado(self, row: dict) -> None:
        nuevo = not bool(row.get("revisado"))
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
        except Exception as exc:
            messagebox.showerror("Actas Terminadas", f"No se pudo actualizar 'revisado': {exc}")


class WizardApp:
    def __init__(self, root: tk.Tk, api: ApiClient):
        self.root = root
        self.api = api
        self.state = WizardState()
        self._summary_after_id: str | None = None
        self._version_var = tk.StringVar()
        self.monitor_panel: LiveMonitorPanel | None = None
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
        tk.Button(
            actions_right,
            text="Actualizar Version de la Aplicacion",
            command=self._open_update_page,
            bg="#4B8BBE",
            fg="white",
            font=("Arial", self._scaled_font(9, 8), "bold"),
            padx=7 if self._is_small_screen else 8,
            pady=2 if self._is_small_screen else 3,
        ).pack(side=tk.LEFT, padx=(0, 6))
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

        ttk.Label(
            button_col,
            text="Seleccione una opcion para iniciar",
            font=("Arial", self._scaled_font(14, 11), "bold"),
            foreground=COLOR_PURPLE,
        ).pack(pady=(max(12, int(16 * self._ui_scale)), max(8, int(12 * self._ui_scale))))

        tk.Button(
            button_col,
            text="Crear nueva entrada",
            command=self.start_new_service,
            bg=COLOR_TEAL,
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        ).pack(pady=self._menu_button_pady)

        tk.Button(
            button_col,
            text="Crear factura",
            command=self._open_factura_dialog,
            bg="#4B8BBE",
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        ).pack(pady=self._menu_button_pady)

        tk.Button(
            button_col,
            text="Reconstruir Excel desde Supabase",
            command=self._rebuild_excel_from_supabase,
            bg="#C0392B",
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        ).pack(pady=self._menu_button_pady)

        tk.Button(
            button_col,
            text="Actualizar Base de Datos",
            command=self._refresh_cache_from_supabase,
            bg="#2E86C1",
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        ).pack(pady=self._menu_button_pady)

        tk.Button(
            button_col,
            text="Abrir monitor en tiempo real",
            command=self._open_live_monitor,
            bg="#5D6D7E",
            fg="white",
            font=("Arial", self._scaled_font(12, 10), "bold"),
            padx=16,
            pady=self._menu_button_pady,
            width=self._menu_button_width,
        ).pack(pady=self._menu_button_pady)
        self._refresh_actas_alert(silent=True)

    def _scaled_font(self, base_size: int, min_size: int = 9) -> int:
        return max(min_size, int(round(base_size * self._ui_scale)))

    def _open_live_monitor(self) -> None:
        if self.monitor_panel and self.monitor_panel.winfo_exists():
            self.monitor_panel.lift()
            self.monitor_panel.focus_force()
            return
        self.monitor_panel = LiveMonitorPanel(self.root, self.api)

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
            return

    def _on_canvas_configure(self, event) -> None:
        try:
            if self.canvas.winfo_exists():
                self.canvas.itemconfigure(self._content_window, width=event.width)
        except tk.TclError:
            return

    def _refresh_actas_alert(self, silent: bool = False) -> None:
        try:
            payload = self.api.get("/wizard/actas-finalizadas/status")
            data = payload.get("data", {})
            pendientes = int(data.get("pendientes", 0) or 0)
            self._set_actas_pending(pendientes)
        except Exception as exc:
            if not silent:
                messagebox.showerror("Actas Terminadas", f"No se pudo cargar el estado: {exc}")

    def _open_actas_terminadas(self) -> None:
        if self.actas_panel and self.actas_panel.winfo_exists():
            self.actas_panel.lift()
            self.actas_panel.focus_force()
            return
        self.actas_panel = ActasTerminadasPanel(self.root, self.api, on_status_change=self._set_actas_pending)

    def _notify_monitor_refresh(self) -> None:
        panel = self.monitor_panel
        if not panel or not panel.winfo_exists():
            return
        try:
            panel.force_refresh(silent=True)
        except Exception:
            return

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
        mes_combo = ttk.Combobox(container, textvariable=mes_var, values=meses, width=20, state="readonly")
        mes_combo.grid(row=0, column=1, sticky="w", pady=4)
        configure_combobox(mes_combo, meses)

        ttk.Label(container, text="Año").grid(row=1, column=0, sticky="w", pady=4)
        anos = [str(year) for year in range(2020, 2031)]
        ano_combo = ttk.Combobox(container, textvariable=ano_var, values=anos, width=10, state="readonly")
        ano_combo.grid(row=1, column=1, sticky="w", pady=4)
        configure_combobox(ano_combo, anos)

        ttk.Label(container, text="Tipo").grid(row=2, column=0, sticky="w", pady=4)
        tipo_combo = ttk.Combobox(
            container,
            textvariable=tipo_var,
            values=["Clausulada", "No clausulada"],
            width=20,
            state="readonly",
        )
        tipo_combo.grid(row=2, column=1, sticky="w", pady=4)
        configure_combobox(tipo_combo, ["Clausulada", "No clausulada"])

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
        # Keep window inside the visible work area for low-resolution displays.
        min_w = 760 if self._is_small_screen else 900
        min_h = 520 if self._is_small_screen else 560
        width = min(max(min_w, int(screen_w * 0.9)), max(min_w, screen_w - 20))
        height = min(max(min_h, int(screen_h * 0.9)), max(min_h, screen_h - 80))
        x = max(0, int((screen_w - width) / 2))
        y = max(0, int((screen_h - height) / 2))
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        self.root.minsize(min_w, min_h)

    def start_new_service(self) -> None:
        try:
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
                command=self.show_initial_screen,
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

            self.resumen = ResumenFrame(main_col, self.terminar_servicio, self._flush_excel_queue)
            self.resumen.grid(row=6, column=0, sticky="ew", pady=8)

            self._load_section_data()
            self._reset_for_new_entry()
            self._lock_sections()
            self._bind_summary_updates()
            self._refresh_summary()
            self._update_queue_status()
        except Exception as exc:
            messagebox.showerror("Error", f"No se pudo iniciar el formulario: {exc}")

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

        loading = LoadingDialog(self.root, "Leyendo acta de Excel...")
        self.root.update_idletasks()
        try:
            from app.services.excel_acta_import import parse_acta_excel

            parsed = parse_acta_excel(file_path)
        except Exception as exc:
            loading.close()
            messagebox.showerror("Importar acta", f"No se pudo leer el archivo:\n{exc}")
            return
        loading.close()

        nit = (parsed.get("nit_empresa") or "").strip()
        if not nit:
            messagebox.showerror(
                "Importar acta",
                "No se detecto NIT en el archivo. Verifica la plantilla.",
            )
            return
        try:
            empresa_lookup = self.api.get_cached("/wizard/seccion-2/empresa", params={"nit": nit})
        except Exception as exc:
            messagebox.showerror("Importar acta", f"No se pudo validar el NIT contra la base de datos:\n{exc}")
            return
        empresas_encontradas = list(empresa_lookup.get("data") or [])
        if not empresas_encontradas:
            messagebox.showerror(
                "Importar acta",
                f"El NIT {nit} no existe en la base de datos. Verifica el formulario.",
            )
            return
        parsed["_nit_validado_bd"] = True
        parsed["_empresa_bd_nombre"] = str(empresas_encontradas[0].get("nombre_empresa") or "").strip()

        participantes_raw = list(parsed.get("participantes") or [])
        cedulas_bd = set()
        try:
            usuarios_data = self.api.get_cached("/wizard/seccion-4/usuarios")
            for item in list(usuarios_data.get("data") or []):
                ced = str(item.get("cedula_usuario") or "").strip()
                if ced:
                    cedulas_bd.add(ced)
        except Exception:
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
            parsed.setdefault("warnings", []).append(
                f"Se descartaron {len(descartados)} cedula(s) que no existen en BD."
            )
            parsed["_cedulas_descartadas"] = descartados

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

        dialog.wait_window()
        return result["value"]

    def _resolve_profesional_import(self, profesional: str) -> str:
        profesional = (profesional or "").strip()
        if not profesional:
            return ""
        prof_values = list(getattr(self.seccion1.prof_combo, "_all_values", []) or [])
        if profesional in prof_values:
            return profesional

        def _norm_name(text: str) -> str:
            text = str(text).strip().lower()
            text = unicodedata.normalize("NFKD", text)
            text = "".join(ch for ch in text if not unicodedata.combining(ch))
            text = re.sub(r"[^a-z0-9\\s]", " ", text)
            text = re.sub(r"\\s+", " ", text).strip()
            return text

        src = _norm_name(profesional)
        if not src:
            return ""
        src_tokens = set(src.split())

        best_item = ""
        best_score = 0.0
        for item in prof_values:
            norm_item = _norm_name(item)
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
        resolved_prof = self._resolve_profesional_import(raw_prof)
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

        dialog.wait_window()
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
            self.seccion3.fecha_var.set(fecha)

        selected_prof = self._resolve_profesional_import((parsed.get("nombre_profesional") or "").strip())
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
        self._notify_monitor_refresh()
        self.show_initial_screen()

        add_another = messagebox.askyesno(
            "Servicio terminado",
            "Servicio guardado. Deseas agregar otro servicio?",
        )
        if add_another:
            self.start_new_service()
        else:
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
    screen_h = root.winfo_screenheight()
    base_font_size = 13 if screen_h >= 900 else 12 if screen_h >= 800 else 11
    default_font = tkfont.nametofont("TkDefaultFont")
    default_font.configure(size=base_font_size)
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
