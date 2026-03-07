"""
drawing_viewer.py — Part drawing viewer

Looks up the latest drawing revision from the local DB,
downloads the PDF from Box, and displays it in a scrollable viewer.
Optionally queries Epicor for additional part information.

Usage:
    python drawing_viewer.py
"""

from __future__ import annotations

import collections
import json
import os
import sys
import threading
import tkinter as tk
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText
import warnings
from datetime import datetime as _dt

import fitz  # PyMuPDF
from PIL import Image, ImageTk

from box_sdk_gen import BoxCCGAuth, BoxClient, CCGConfig
from drawing_db import DrawingDatabase

warnings.filterwarnings("ignore", message="Unverified HTTPS request")


# ---------------------------------------------------------------------------
# In-app log capture
# ---------------------------------------------------------------------------

# All captured log lines, capped at 5 000 (survive for the process lifetime)
_log_lines: collections.deque = collections.deque(maxlen=5_000)
_log_file_handle = None   # file handle, opened lazily when running frozen


def _get_log_file():
    """Return (and lazily open) the on-disk log file when frozen."""
    global _log_file_handle
    if _log_file_handle is not None:
        return _log_file_handle
    if not getattr(sys, "frozen", False):
        return None
    try:
        log_dir = os.path.join(
            os.environ.get("APPDATA", os.path.expanduser("~")), "DrawingViewer"
        )
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "app.log")
        # Rotate: keep last 512 KB to prevent unbounded growth
        if os.path.exists(log_path) and os.path.getsize(log_path) > 512 * 1024:
            os.replace(log_path, log_path + ".old")
        _log_file_handle = open(log_path, "a", encoding="utf-8", buffering=1)
        _log_file_handle.write(
            f"\n{'='*60}\n"
            f"Session started {_dt.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"{'='*60}\n"
        )
    except Exception:
        pass
    return _log_file_handle


class _LogCapture:
    """File-like wrapper that tees writes to the in-app log buffer (+ disk when frozen)."""

    def __init__(self, original):
        self._original = original
        self._buf = ""

    def write(self, text: str) -> None:
        # Forward to real stream in dev mode
        if not getattr(sys, "frozen", False) and self._original:
            try:
                self._original.write(text)
            except Exception:
                pass
        # Buffer until we have complete lines
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                entry = f"[{_dt.now().strftime('%H:%M:%S')}]  {line}"
                _log_lines.append(entry)
                f = _get_log_file()
                if f:
                    try:
                        f.write(entry + "\n")
                    except Exception:
                        pass

    def flush(self) -> None:
        # Flush any partial line still in the buffer
        if self._buf.strip():
            entry = f"[{_dt.now().strftime('%H:%M:%S')}]  {self._buf}"
            _log_lines.append(entry)
            f = _get_log_file()
            if f:
                try:
                    f.write(entry + "\n")
                except Exception:
                    pass
            self._buf = ""
        if not getattr(sys, "frozen", False) and self._original:
            try:
                self._original.flush()
            except Exception:
                pass

    def isatty(self) -> bool:
        return False

    def fileno(self):
        raise OSError("not a real file")


def _install_log_capture() -> None:
    """Redirect stdout and stderr to the in-app log buffer (idempotent)."""
    if not isinstance(sys.stdout, _LogCapture):
        sys.stdout = _LogCapture(sys.__stdout__)
    if not isinstance(sys.stderr, _LogCapture):
        sys.stderr = _LogCapture(sys.__stderr__)


def _settings_dir() -> str:
    """Return a writable directory for user settings."""
    if getattr(sys, "frozen", False):
        # Running as PyInstaller bundle — write to %APPDATA%\DrawingViewer
        base = os.environ.get("APPDATA", os.path.expanduser("~"))
        path = os.path.join(base, "DrawingViewer")
        os.makedirs(path, exist_ok=True)
        return path
    return os.path.dirname(os.path.abspath(__file__))

SETTINGS_FILE = os.path.join(_settings_dir(), "viewer_settings.json")


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def _load_settings() -> dict:
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"query_epicor": True, "zoom": 1.5}


def _save_settings(s: dict) -> None:
    with open(SETTINGS_FILE, "w") as f:
        json.dump(s, f, indent=2)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

class DrawingViewer:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Part Drawing Viewer")
        self.root.geometry("1100x850")
        self.root.minsize(700, 500)

        self._settings = _load_settings()
        self._db: DrawingDatabase | None = None
        self._box: BoxClient | None = None

        # PDF / STEP state
        self._doc: fitz.Document | None = None
        self._pdf_bytes: bytes | None = None
        self._current_part:     str = ""
        self._current_rev:      str = ""
        self._current_step_id:  str | None = None   # Box file ID for matched STEP
        self._page_idx = 0
        self._zoom = float(self._settings.get("zoom", 1.5))
        self._photo: ImageTk.PhotoImage | None = None  # prevent GC

        self._build_menu()
        self._build_ui()

    # ------------------------------------------------------------------
    # Menu
    # ------------------------------------------------------------------

    def _build_menu(self) -> None:
        menubar = tk.Menu(self.root)

        self._query_epicor = tk.BooleanVar(
            value=self._settings.get("query_epicor", True)
        )
        cfg = tk.Menu(menubar, tearoff=0)
        cfg.add_checkbutton(
            label="Query Epicor for part info",
            variable=self._query_epicor,
            command=self._on_settings_change,
        )
        menubar.add_cascade(label="Config", menu=cfg)

        view = tk.Menu(menubar, tearoff=0)
        view.add_command(label="Application Log\u2026", command=self._open_log_viewer)
        menubar.add_cascade(label="View", menu=view)

        self.root.config(menu=menubar)

    # ------------------------------------------------------------------
    # UI layout
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        # ── Top bar: part entry + zoom + page controls ────────────────
        top = tk.Frame(self.root, padx=6, pady=5)
        top.pack(fill=tk.X, side=tk.TOP)

        tk.Label(top, text="Part Number:", font=("", 11)).pack(side=tk.LEFT)
        self._part_entry = tk.Entry(top, width=10, font=("Courier", 15, "bold"))
        self._part_entry.pack(side=tk.LEFT, padx=(5, 10))
        self._part_entry.bind("<Return>", lambda _e: self._start_lookup())
        self._part_entry.focus_set()

        tk.Button(top, text="Look Up", width=8,
                  command=self._start_lookup).pack(side=tk.LEFT)
        tk.Button(top, text="BOM View", width=8,
                  command=self._open_bom_viewer).pack(side=tk.LEFT, padx=(6, 0))
        tk.Button(top, text="Where Used", width=9,
                  command=self._open_where_used_viewer).pack(side=tk.LEFT, padx=(6, 0))
        tk.Button(top, text="Job Summary", width=10,
                  command=self._open_job_summary).pack(side=tk.LEFT, padx=(6, 0))
        tk.Button(top, text="Rev History", width=10,
                  command=self._open_rev_history).pack(side=tk.LEFT, padx=(6, 0))
        tk.Button(top, text="Order Info", width=9,
                  command=self._open_order_info).pack(side=tk.LEFT, padx=(6, 0))

        # Page navigation (right side)
        self._page_label = tk.Label(top, text="", width=12, anchor="e", font=("", 10))
        self._page_label.pack(side=tk.RIGHT, padx=4)

        tk.Button(top, text="\u25b6", width=2,
                  command=self._next_page).pack(side=tk.RIGHT)
        tk.Button(top, text="\u25c4", width=2,
                  command=self._prev_page).pack(side=tk.RIGHT)
        tk.Label(top, text="Page:").pack(side=tk.RIGHT, padx=(10, 2))

        # Zoom controls
        self._zoom_label = tk.Label(top, text=f"{self._zoom:.0%}", width=5, anchor="e")
        self._zoom_label.pack(side=tk.RIGHT)
        tk.Button(top, text="Fit", width=3, command=self._zoom_fit).pack(side=tk.RIGHT, padx=(0, 4))
        tk.Button(top, text="+", width=2, command=self._zoom_in).pack(side=tk.RIGHT)
        tk.Button(top, text="\u2212", width=2, command=self._zoom_out).pack(side=tk.RIGHT)
        tk.Label(top, text="Zoom:").pack(side=tk.RIGHT, padx=(10, 2))

        # Download buttons — right side, separated from zoom controls
        self._dl_both_btn = tk.Button(
            top, text="\u2b07 PDF+STEP", width=10,
            command=self._download_pdf_step, state="disabled",
        )
        self._dl_both_btn.pack(side=tk.RIGHT, padx=(4, 0))
        self._dl_btn = tk.Button(
            top, text="\u2b07 PDF", width=6,
            command=self._download_pdf, state="disabled",
        )
        self._dl_btn.pack(side=tk.RIGHT, padx=(4, 16))

        # ── Info container (DB info + Epicor info, stacked) ───────────
        info_frame = tk.Frame(self.root)
        info_frame.pack(fill=tk.X, side=tk.TOP)

        self._info_var = tk.StringVar(value="Enter a part number above and press Enter.")
        self._info_label = tk.Label(
            info_frame,
            textvariable=self._info_var,
            anchor="w", bg="#d6e8f7", padx=8, pady=3,
            font=("", 10),
        )
        self._info_label.pack(fill=tk.X)

        self._epicor_var = tk.StringVar(value="")
        self._epicor_bar = tk.Label(
            info_frame,
            textvariable=self._epicor_var,
            anchor="w", bg="#dff0d8", padx=8, pady=3,
            font=("", 10),
        )
        # Packed/forgotten dynamically — stays inside info_frame so ordering is stable

        # ── PDF canvas with scrollbars ─────────────────────────────────
        pdf_outer = tk.Frame(self.root)
        pdf_outer.pack(fill=tk.BOTH, expand=True, side=tk.TOP)

        self._canvas = tk.Canvas(pdf_outer, bg="#505050", highlightthickness=0)
        vbar = tk.Scrollbar(pdf_outer, orient=tk.VERTICAL,   command=self._canvas.yview)
        hbar = tk.Scrollbar(pdf_outer, orient=tk.HORIZONTAL, command=self._canvas.xview)
        self._canvas.configure(yscrollcommand=vbar.set, xscrollcommand=hbar.set)

        vbar.pack(side=tk.RIGHT,  fill=tk.Y)
        hbar.pack(side=tk.BOTTOM, fill=tk.X)
        self._canvas.pack(fill=tk.BOTH, expand=True)

        self._canvas.bind("<MouseWheel>",         self._on_scroll)
        self._canvas.bind("<Control-MouseWheel>", self._on_ctrl_scroll)

        # ── Status bar ─────────────────────────────────────────────────
        self._status_var = tk.StringVar(value="Ready")
        tk.Label(
            self.root,
            textvariable=self._status_var,
            anchor="w", relief=tk.SUNKEN, padx=5, font=("", 9),
        ).pack(fill=tk.X, side=tk.BOTTOM)

    # ------------------------------------------------------------------
    # Lazy clients
    # ------------------------------------------------------------------

    @property
    def db(self) -> DrawingDatabase:
        if self._db is None:
            from config import DATABASE_URL
            self._db = DrawingDatabase(DATABASE_URL)
        return self._db

    @property
    def box(self) -> BoxClient:
        if self._box is None:
            from config import BOX_CLIENT_ID, BOX_CLIENT_SECRET, BOX_ENTERPRISE_ID
            self._box = BoxClient(
                BoxCCGAuth(CCGConfig(
                    client_id=BOX_CLIENT_ID,
                    client_secret=BOX_CLIENT_SECRET,
                    enterprise_id=BOX_ENTERPRISE_ID,
                ))
            )
        return self._box

    # ------------------------------------------------------------------
    # Part lookup (runs in background thread)
    # ------------------------------------------------------------------

    def _start_lookup(self) -> None:
        part_num = self._part_entry.get().strip().upper()
        if not part_num:
            return
        self._pdf_bytes       = None
        self._current_step_id = None
        self._dl_btn.config(state="disabled")
        self._dl_both_btn.config(state="disabled")
        self._hide_epicor_bar()
        self._info_label.config(bg="#d6e8f7")  # reset to default blue
        self._info_var.set(f"Looking up {part_num}...")
        self._set_status("Looking up...")
        threading.Thread(target=self._do_lookup, args=(part_num,), daemon=True).start()

    def _do_lookup(self, part_num: str) -> None:
        try:
            part    = self.db.get_part(part_num)
            drawing = self.db.get_latest_drawing(part_num)

            if part is None:
                # Not in local drawing DB — still query Epicor for part info
                self._ui(self._info_var.set, f"{part_num} — no drawing on file.")
                self._ui(self._canvas_message, f"No drawing on file for {part_num}")
                if self._query_epicor.get():
                    self._ui(self._set_status, "Querying Epicor...")
                    threading.Thread(
                        target=self._fetch_epicor, args=(part_num, None), daemon=True
                    ).start()
                else:
                    self._ui(self._set_status, "No drawing on file.")
                return

            rev  = drawing.revision if drawing else "\u2014"
            desc = part.description or ""
            self._ui(
                self._info_var.set,
                f"Part: {part.part_num}     Rev: {rev}     {desc}",
            )

            # Kick off Epicor query in parallel if enabled
            drawing_rev = drawing.revision if drawing else None
            if self._query_epicor.get():
                self._ui(self._set_status, "Querying Epicor...")
                threading.Thread(
                    target=self._fetch_epicor, args=(part_num, drawing_rev), daemon=True
                ).start()

            # Store STEP file ID for the combined download button
            self._ui(self._set_step_id, drawing.step_file_id if drawing else None)

            # Download PDF
            if drawing and drawing.pdf_file_id:
                self._ui(self._set_status, "Downloading PDF from Box...")
                stream    = self.box.downloads.download_file(drawing.pdf_file_id)
                pdf_bytes = stream.read()
                self._ui(self._load_pdf, pdf_bytes, part_num, rev)
            else:
                self._ui(
                    self._canvas_message,
                    f"No PDF on file for {part_num}  Rev {rev}",
                )
                if not self._query_epicor.get():
                    self._ui(self._set_status, "No PDF available.")

        except Exception as exc:
            import traceback
            traceback.print_exc()
            short = str(exc).splitlines()[0][:120]
            self._ui(self._info_var.set, f"Error: {short}")
            self._ui(self._set_status, f"Error (see console for details)")

    # ------------------------------------------------------------------
    # Epicor query (runs in background thread)
    # ------------------------------------------------------------------

    def _fetch_epicor(self, part_num: str, drawing_rev: str | None) -> None:
        try:
            import json as _json
            import requests
            from config import (
                EPICOR_API_KEY, EPICOR_BASE_URL, EPICOR_COMPANY,
                EPICOR_PASSWORD, EPICOR_PLANT, EPICOR_USERNAME,
            )

            # Use POST to GetByID — avoids OData $filter params which requests
            # percent-encodes ($filter → %24filter), causing Epicor 400 errors.
            url = (
                f"{EPICOR_BASE_URL}/api/v2/odata/{EPICOR_COMPANY}"
                f"/Erp.BO.PartSvc/GetByID"
            )
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "CallSettings": _json.dumps(
                    {"Company": EPICOR_COMPANY, "Plant": EPICOR_PLANT}
                ),
            }
            resp = requests.post(
                url,
                auth=(EPICOR_USERNAME, EPICOR_PASSWORD),
                headers=headers,
                params={"api-key": EPICOR_API_KEY},
                json={"partNum": part_num},
                timeout=15,
                verify=False,
            )
            resp.raise_for_status()
            data = resp.json()
            obj  = data.get("returnObj", {})

            # GetByID returns {"returnObj": {"Part": [...], "PartRev": [...], ...}}
            parts = obj.get("Part", [])
            if not parts:
                self._ui(self._show_epicor_bar, f"Epicor: part {part_num!r} not found in ERP.")
                self._ui(self._set_status, "Ready")
                return

            p    = parts[0]
            desc = p.get("PartDescription", "")
            cls  = p.get("ClassID", "")
            typ  = {"M": "Mfg", "P": "Purch", "R": "Raw"}.get(
                p.get("TypeCode", ""), p.get("TypeCode", "")
            )
            uom  = p.get("IUM", "") or p.get("UOMCode", "")

            # Extract latest approved revision from PartRev dataset
            part_revs    = obj.get("PartRev", [])
            epicor_rev   = self._latest_epicor_rev(part_revs)

            # Compare with drawing revision
            rev_match = (
                epicor_rev is not None
                and drawing_rev is not None
                and epicor_rev.upper() == str(drawing_rev).upper()
            )
            rev_unknown = epicor_rev is None or drawing_rev is None

            if rev_unknown:
                rev_flag = f"  |  Epicor Rev: {epicor_rev or '—'}"
            elif rev_match:
                rev_flag = f"  |  Epicor Rev: {epicor_rev}  \u2713"
            else:
                rev_flag = f"  |  Epicor Rev: {epicor_rev}  \u26a0 drawing is Rev {drawing_rev}"

            text = (
                f"Epicor \u2014  {desc}"
                f"     Class: {cls}     Type: {typ}     UOM: {uom}"
                f"{rev_flag}"
            )

            self._ui(self._show_epicor_bar, text)
            if not rev_unknown and not rev_match:
                self._ui(self._flag_rev_mismatch)
            self._ui(self._set_status, "Ready")

        except Exception as exc:
            import traceback
            print("\n=== EPICOR ERROR ===", flush=True)
            traceback.print_exc()
            if hasattr(exc, "response") and exc.response is not None:
                print(f"Status: {exc.response.status_code}")
                print(f"URL:    {exc.response.url}")
                print(f"Body:   {exc.response.text[:2000]}", flush=True)
            print("====================\n", flush=True)
            self._ui(self._show_epicor_bar, f"Epicor error: {type(exc).__name__} (see console)")
            self._ui(self._set_status, "Epicor error (see console)")

    # ------------------------------------------------------------------
    # PDF rendering
    # ------------------------------------------------------------------

    def _load_pdf(self, pdf_bytes: bytes, part_num: str, rev: str) -> None:
        self._pdf_bytes    = pdf_bytes
        self._current_part = part_num
        self._current_rev  = rev
        self._doc          = fitz.open(stream=pdf_bytes, filetype="pdf")
        self._page_idx     = 0
        self.root.title(f"Part Drawing Viewer — {part_num}  Rev {rev}")
        self._dl_btn.config(state="normal")
        if self._current_step_id:
            self._dl_both_btn.config(state="normal")
        self._zoom = self._fit_zoom()
        self._render_page()

    def _fit_zoom(self) -> float:
        """Calculate zoom so the first page fills the canvas."""
        if not self._doc:
            return self._zoom
        # Use update_idletasks to ensure the canvas has its real dimensions
        self._canvas.update_idletasks()
        cw = self._canvas.winfo_width()
        ch = self._canvas.winfo_height()
        if cw < 10 or ch < 10:
            return self._zoom  # canvas not yet laid out; keep current zoom
        page = self._doc[0]
        pw = page.rect.width   # page size in points at zoom=1
        ph = page.rect.height
        zoom = min(cw / pw, ch / ph)
        return max(0.2, min(round(zoom, 3), 6.0))

    def _render_page(self) -> None:
        if not self._doc:
            return
        page = self._doc[self._page_idx]
        mat  = fitz.Matrix(self._zoom, self._zoom)
        pix  = page.get_pixmap(matrix=mat, alpha=False)
        img  = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

        self._photo = ImageTk.PhotoImage(img)
        self._canvas.delete("all")
        self._canvas.create_image(0, 0, anchor=tk.NW, image=self._photo)
        self._canvas.configure(scrollregion=(0, 0, pix.width, pix.height))
        # Scroll back to top on new page
        self._canvas.yview_moveto(0)

        n = len(self._doc)
        self._page_label.config(text=f"{self._page_idx + 1} / {n}")
        self._zoom_label.config(text=f"{self._zoom:.0%}")
        self._set_status(
            f"Page {self._page_idx + 1} of {n}   |   zoom {self._zoom:.0%}"
            f"   |   Ctrl+scroll to zoom   |   Scroll to pan"
        )

    def _canvas_message(self, text: str) -> None:
        self._doc             = None
        self._pdf_bytes       = None
        self._current_step_id = None
        self._dl_btn.config(state="disabled")
        self._dl_both_btn.config(state="disabled")
        self._page_label.config(text="")
        self._canvas.delete("all")
        w = self._canvas.winfo_width()  or 600
        h = self._canvas.winfo_height() or 400
        self._canvas.create_text(
            w // 2, h // 2, text=text,
            font=("", 14), fill="#cccccc",
        )
        self._canvas.configure(scrollregion=(0, 0, w, h))

    # ------------------------------------------------------------------
    # Page / zoom controls
    # ------------------------------------------------------------------

    def _prev_page(self) -> None:
        if self._doc and self._page_idx > 0:
            self._page_idx -= 1
            self._render_page()

    def _next_page(self) -> None:
        if self._doc and self._page_idx < len(self._doc) - 1:
            self._page_idx += 1
            self._render_page()

    def _zoom_fit(self) -> None:
        if not self._doc:
            return
        self._zoom = self._fit_zoom()
        self._render_page()

    def _zoom_in(self) -> None:
        self._zoom = min(round(self._zoom * 1.25, 3), 6.0)
        self._settings["zoom"] = self._zoom
        _save_settings(self._settings)
        self._render_page()

    def _zoom_out(self) -> None:
        self._zoom = max(round(self._zoom / 1.25, 3), 0.2)
        self._settings["zoom"] = self._zoom
        _save_settings(self._settings)
        self._render_page()

    def _on_scroll(self, event: tk.Event) -> None:
        self._canvas.yview_scroll(int(-1 * event.delta / 120), "units")

    def _on_ctrl_scroll(self, event: tk.Event) -> None:
        if event.delta > 0:
            self._zoom_in()
        else:
            self._zoom_out()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ui(self, fn, *args) -> None:
        """Schedule *fn(*args)* on the Tk main thread."""
        self.root.after(0, fn, *args)

    def _set_status(self, msg: str) -> None:
        self._status_var.set(msg)

    def _flag_rev_mismatch(self) -> None:
        """Turn the info bar red to signal a revision mismatch."""
        self._info_label.config(bg="#f4b8b8")

    @staticmethod
    def _latest_epicor_rev(part_revs: list) -> str | None:
        return _latest_epicor_rev(part_revs)

    def _show_epicor_bar(self, text: str) -> None:
        self._epicor_var.set(text)
        self._epicor_bar.pack(fill=tk.X)

    def _hide_epicor_bar(self) -> None:
        self._epicor_bar.pack_forget()
        self._epicor_var.set("")

    def _download_pdf(self) -> None:
        if not self._pdf_bytes:
            return
        from tkinter import filedialog
        rev = self._current_rev or ""
        default = (
            f"{self._current_part}_Rev{rev}.pdf" if rev
            else f"{self._current_part}.pdf"
        )
        path = filedialog.asksaveasfilename(
            parent=self.root,
            title="Save PDF",
            initialfile=default,
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            with open(path, "wb") as f:
                f.write(self._pdf_bytes)
            self._set_status(f"Saved to {path}")
        except Exception as exc:
            self._set_status(f"Save failed: {exc}")

    def _set_step_id(self, step_file_id: str | None) -> None:
        """Store the STEP Box file ID for the current part (main thread)."""
        self._current_step_id = step_file_id

    def _download_pdf_step(self) -> None:
        """Save the current PDF and download the matched STEP to a chosen folder."""
        if not self._pdf_bytes or not self._current_step_id:
            return
        from tkinter import filedialog
        folder = filedialog.askdirectory(
            parent=self.root,
            title="Choose folder to save PDF + STEP",
        )
        if not folder:
            return
        rev      = self._current_rev or ""
        stem     = f"{self._current_part}_Rev{rev}" if rev else self._current_part
        pdf_path = os.path.join(folder, f"{stem}.pdf")
        try:
            with open(pdf_path, "wb") as f:
                f.write(self._pdf_bytes)
        except Exception as exc:
            self._set_status(f"PDF save failed: {exc}")
            return
        # Download STEP from Box in a background thread
        self._set_status("Downloading STEP from Box\u2026")
        threading.Thread(
            target=self._step_download_worker,
            args=(folder, stem, self._current_step_id),
            daemon=True,
        ).start()

    def _step_download_worker(self, folder: str, stem: str, step_file_id: str) -> None:
        try:
            stream     = self.box.downloads.download_file(step_file_id)
            step_bytes = stream.read()
            step_path  = os.path.join(folder, f"{stem}.step")
            with open(step_path, "wb") as f:
                f.write(step_bytes)
            self._ui(self._set_status, f"Saved PDF + STEP to {folder}")
        except Exception as exc:
            self._ui(self._set_status, f"STEP download failed: {exc}")

    def _on_settings_change(self) -> None:
        self._settings["query_epicor"] = self._query_epicor.get()
        _save_settings(self._settings)
        if not self._query_epicor.get():
            self._hide_epicor_bar()

    def _open_log_viewer(self) -> None:
        LogViewerWindow(self.root)

    def _open_bom_viewer(self) -> None:
        """Open a BOM viewer window, pre-seeded with the current part if any."""
        win = BOMViewerWindow(self)
        part_num = self._part_entry.get().strip().upper()
        if part_num:
            win.seed_part(part_num)

    def _open_where_used_viewer(self) -> None:
        """Open a Where Used viewer window, pre-seeded with the current part if any."""
        win = WhereUsedViewerWindow(self)
        part_num = self._part_entry.get().strip().upper()
        if part_num:
            win.seed_part(part_num)

    def _open_job_summary(self) -> None:
        """Open a Job Summary window."""
        JobSummaryWindow(self)

    def _open_rev_history(self) -> None:
        """Open a Revision History window, seeded with the current part number."""
        win = RevisionHistoryWindow(self)
        part = self._part_entry.get().strip()
        if part:
            win.seed_part(part)

    def _open_order_info(self) -> None:
        """Open an Order Info window, seeded with the current part number."""
        win = OrderInfoWindow(self)
        part = self._part_entry.get().strip().upper()
        if part:
            win.seed_part(part)


# ---------------------------------------------------------------------------
# Log viewer window
# ---------------------------------------------------------------------------

class LogViewerWindow:
    """
    Scrollable window showing everything that has been printed to stdout/stderr.
    Opens from View > Application Log.  Polls for new messages every 300 ms.
    When running as a frozen exe, messages are also written to
    %APPDATA%\\DrawingViewer\\app.log.
    """

    def __init__(self, parent: tk.Misc) -> None:
        self._shown = 0   # how many lines from _log_lines are already displayed

        self._win = tk.Toplevel(parent)
        self._win.title("Application Log")
        self._win.geometry("950x520")
        self._win.minsize(500, 300)

        # ── Toolbar ────────────────────────────────────────────────────
        top = tk.Frame(self._win, padx=6, pady=4)
        top.pack(fill=tk.X, side=tk.TOP)

        tk.Button(top, text="Clear display", command=self._clear).pack(side=tk.LEFT)
        tk.Button(top, text="Copy all",      command=self._copy_all).pack(side=tk.LEFT, padx=(4, 0))

        if getattr(sys, "frozen", False):
            log_path = os.path.join(
                os.environ.get("APPDATA", "~"), "DrawingViewer", "app.log"
            )
            tk.Label(
                top, text=f"Also written to: {log_path}",
                font=("", 9), fg="#666666", anchor="e",
            ).pack(side=tk.RIGHT)

        # ── Text area ──────────────────────────────────────────────────
        self._text = ScrolledText(
            self._win,
            font=("Courier", 9),
            state="disabled",
            wrap="word",
            bg="#1a1a1a", fg="#d4d4d4",
            insertbackground="#d4d4d4",
        )
        self._text.pack(fill=tk.BOTH, expand=True)

        # Show everything captured so far, then start live polling
        self._refresh()
        self._poll()

    # ------------------------------------------------------------------

    def _append(self, text: str) -> None:
        self._text.config(state="normal")
        self._text.insert("end", text + "\n")
        self._text.see("end")
        self._text.config(state="disabled")

    def _refresh(self) -> None:
        """Append any new lines that arrived since the last refresh."""
        lines = list(_log_lines)   # GIL makes this safe enough for log display
        if len(lines) > self._shown:
            for line in lines[self._shown:]:
                self._append(line)
            self._shown = len(lines)

    def _poll(self) -> None:
        if not self._win.winfo_exists():
            return
        self._refresh()
        self._win.after(300, self._poll)

    def _clear(self) -> None:
        self._text.config(state="normal")
        self._text.delete("1.0", "end")
        self._text.config(state="disabled")
        self._shown = len(_log_lines)   # don't re-show cleared lines

    def _copy_all(self) -> None:
        text = self._text.get("1.0", "end")
        self._win.clipboard_clear()
        self._win.clipboard_append(text)


# ---------------------------------------------------------------------------
# Epicor client factory
# ---------------------------------------------------------------------------

def _make_epicor_client():
    """Create an EpicorClient from config credentials."""
    from epicor_po_x2 import EpicorClient
    import config as _cfg
    return EpicorClient(
        base_url=_cfg.EPICOR_BASE_URL,
        company=_cfg.EPICOR_COMPANY,
        plant=_cfg.EPICOR_PLANT,
        api_key=_cfg.EPICOR_API_KEY,
        username=_cfg.EPICOR_USERNAME,
        password=_cfg.EPICOR_PASSWORD,
        learn_missing_getrows_params=True,
    )


def _latest_epicor_rev(part_revs: list) -> str | None:
    """
    From Epicor's PartRev list, return the most recent approved revision.
    Falls back to the last revision in the list if none are approved.

    Uses sorted(...)[-1] so that when dates are equal (or null), the last
    item in the API's returned order is chosen — consistent across all callers.
    """
    if not part_revs:
        return None
    approved   = [r for r in part_revs if r.get("Approved")]
    candidates = approved if approved else part_revs
    best = sorted(
        candidates,
        key=lambda r: r.get("ApprovedDate") or r.get("EffectiveDate") or "",
    )[-1]
    return (best.get("RevisionNum") or "").strip() or None


# ---------------------------------------------------------------------------
# Drawing view Toplevel (opened from BOM viewer row double-click)
# ---------------------------------------------------------------------------

class DrawingViewToplevel:
    """Minimal drawing viewer in a secondary Toplevel window."""

    def __init__(
        self,
        parent: tk.Misc,
        part_num: str,
        db,
        box,
    ) -> None:
        self._db  = db
        self._box = box
        self._doc: fitz.Document | None = None
        self._page_idx = 0
        self._zoom = 1.5
        self._photo: ImageTk.PhotoImage | None = None

        self._win = tk.Toplevel(parent)
        self._win.title(f"Drawing \u2014 {part_num}")
        self._win.geometry("1000x800")
        self._win.minsize(500, 400)

        self._build_ui()
        threading.Thread(target=self._load, args=(part_num,), daemon=True).start()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        top = tk.Frame(self._win, padx=6, pady=4)
        top.pack(fill=tk.X, side=tk.TOP)

        self._page_label = tk.Label(top, text="", width=12, anchor="e", font=("", 10))
        self._page_label.pack(side=tk.RIGHT, padx=4)
        tk.Button(top, text="\u25b6", width=2, command=self._next_page).pack(side=tk.RIGHT)
        tk.Button(top, text="\u25c4", width=2, command=self._prev_page).pack(side=tk.RIGHT)
        tk.Label(top, text="Page:").pack(side=tk.RIGHT, padx=(10, 2))

        self._zoom_label = tk.Label(top, text="", width=5, anchor="e")
        self._zoom_label.pack(side=tk.RIGHT)
        tk.Button(top, text="Fit",    width=3, command=self._zoom_fit).pack(side=tk.RIGHT, padx=(0, 4))
        tk.Button(top, text="+",      width=2, command=self._zoom_in).pack(side=tk.RIGHT)
        tk.Button(top, text="\u2212", width=2, command=self._zoom_out).pack(side=tk.RIGHT)
        tk.Label(top, text="Zoom:").pack(side=tk.RIGHT, padx=(10, 2))

        # ── Info bars (blue DB info + green Epicor info) ──────────────
        info_frame = tk.Frame(self._win)
        info_frame.pack(fill=tk.X, side=tk.TOP)

        self._info_var = tk.StringVar(value="Loading\u2026")
        self._info_label = tk.Label(
            info_frame,
            textvariable=self._info_var,
            anchor="w", bg="#d6e8f7", padx=8, pady=3,
            font=("", 10),
        )
        self._info_label.pack(fill=tk.X)

        self._epicor_var = tk.StringVar(value="")
        self._epicor_bar = tk.Label(
            info_frame,
            textvariable=self._epicor_var,
            anchor="w", bg="#dff0d8", padx=8, pady=3,
            font=("", 10),
        )
        # Packed/forgotten dynamically — stays inside info_frame so ordering is stable

        outer = tk.Frame(self._win)
        outer.pack(fill=tk.BOTH, expand=True)
        self._canvas = tk.Canvas(outer, bg="#505050", highlightthickness=0)
        vbar = tk.Scrollbar(outer, orient=tk.VERTICAL,   command=self._canvas.yview)
        hbar = tk.Scrollbar(outer, orient=tk.HORIZONTAL, command=self._canvas.xview)
        self._canvas.configure(yscrollcommand=vbar.set, xscrollcommand=hbar.set)
        vbar.pack(side=tk.RIGHT,  fill=tk.Y)
        hbar.pack(side=tk.BOTTOM, fill=tk.X)
        self._canvas.pack(fill=tk.BOTH, expand=True)
        self._canvas.bind("<MouseWheel>",         self._on_scroll)
        self._canvas.bind("<Control-MouseWheel>", self._on_ctrl_scroll)

        self._status_var = tk.StringVar(value="Loading\u2026")
        tk.Label(
            self._win, textvariable=self._status_var,
            anchor="w", relief=tk.SUNKEN, padx=5, font=("", 9),
        ).pack(fill=tk.X, side=tk.BOTTOM)

    # ------------------------------------------------------------------
    # Part load (background thread)
    # ------------------------------------------------------------------

    def _ui(self, fn, *args) -> None:
        if self._win.winfo_exists():
            self._win.after(0, fn, *args)

    def _load(self, part_num: str) -> None:
        try:
            part    = self._db.get_part(part_num)
            drawing = self._db.get_latest_drawing(part_num)

            if part is None:
                self._ui(self._info_var.set, f"{part_num} \u2014 no drawing on file")
                self._ui(self._canvas_message, f"No drawing on file for {part_num}")
                self._ui(self._status_var.set, "Querying Epicor\u2026")
                threading.Thread(
                    target=self._fetch_epicor, args=(part_num, None), daemon=True
                ).start()
                return

            rev  = drawing.revision if drawing else "\u2014"
            desc = part.description or ""
            self._ui(
                self._info_var.set,
                f"Part: {part.part_num}     Rev: {rev}     {desc}",
            )

            # Start Epicor query in parallel
            drawing_rev = drawing.revision if drawing else None
            self._ui(self._status_var.set, "Querying Epicor\u2026")
            threading.Thread(
                target=self._fetch_epicor, args=(part_num, drawing_rev), daemon=True
            ).start()

            if drawing and drawing.pdf_file_id:
                stream    = self._box.downloads.download_file(drawing.pdf_file_id)
                pdf_bytes = stream.read()
                self._ui(self._load_pdf, pdf_bytes, part_num, rev)
            else:
                self._ui(self._canvas_message, f"No PDF on file for {part_num}  Rev {rev}")
                self._ui(self._status_var.set, "No PDF available.")

        except Exception as exc:
            import traceback
            traceback.print_exc()
            self._ui(self._status_var.set, f"Error: {str(exc)[:120]}")

    # ------------------------------------------------------------------
    # Epicor query (runs in background thread)
    # ------------------------------------------------------------------

    def _fetch_epicor(self, part_num: str, drawing_rev: str | None) -> None:
        try:
            import json as _json
            import requests
            from config import (
                EPICOR_API_KEY, EPICOR_BASE_URL, EPICOR_COMPANY,
                EPICOR_PASSWORD, EPICOR_PLANT, EPICOR_USERNAME,
            )
            url = (
                f"{EPICOR_BASE_URL}/api/v2/odata/{EPICOR_COMPANY}"
                f"/Erp.BO.PartSvc/GetByID"
            )
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "CallSettings": _json.dumps(
                    {"Company": EPICOR_COMPANY, "Plant": EPICOR_PLANT}
                ),
            }
            resp = requests.post(
                url,
                auth=(EPICOR_USERNAME, EPICOR_PASSWORD),
                headers=headers,
                params={"api-key": EPICOR_API_KEY},
                json={"partNum": part_num},
                timeout=15,
                verify=False,
            )
            resp.raise_for_status()
            data = resp.json()
            obj  = data.get("returnObj", {})

            parts = obj.get("Part", [])
            if not parts:
                self._ui(self._show_epicor_bar, f"Epicor: part {part_num!r} not found in ERP.")
                self._ui(self._status_var.set, "Ready")
                return

            p    = parts[0]
            desc = p.get("PartDescription", "")
            cls  = p.get("ClassID", "")
            typ  = {"M": "Mfg", "P": "Purch", "R": "Raw"}.get(
                p.get("TypeCode", ""), p.get("TypeCode", "")
            )
            uom  = p.get("IUM", "") or p.get("UOMCode", "")

            part_revs  = obj.get("PartRev", [])
            epicor_rev = self._latest_epicor_rev(part_revs)

            rev_match = (
                epicor_rev is not None
                and drawing_rev is not None
                and epicor_rev.upper() == str(drawing_rev).upper()
            )
            rev_unknown = epicor_rev is None or drawing_rev is None

            if rev_unknown:
                rev_flag = f"  |  Epicor Rev: {epicor_rev or '\u2014'}"
            elif rev_match:
                rev_flag = f"  |  Epicor Rev: {epicor_rev}  \u2713"
            else:
                rev_flag = f"  |  Epicor Rev: {epicor_rev}  \u26a0 drawing is Rev {drawing_rev}"

            text = (
                f"Epicor \u2014  {desc}"
                f"     Class: {cls}     Type: {typ}     UOM: {uom}"
                f"{rev_flag}"
            )
            self._ui(self._show_epicor_bar, text)
            if not rev_unknown and not rev_match:
                self._ui(self._flag_rev_mismatch)
            self._ui(self._status_var.set, "Ready")

        except Exception as exc:
            import traceback
            print("\n=== EPICOR ERROR ===", flush=True)
            traceback.print_exc()
            if hasattr(exc, "response") and exc.response is not None:
                print(f"Status: {exc.response.status_code}")
                print(f"URL:    {exc.response.url}")
                print(f"Body:   {exc.response.text[:2000]}", flush=True)
            print("====================\n", flush=True)
            self._ui(self._show_epicor_bar, f"Epicor error: {type(exc).__name__} (see log)")
            self._ui(self._status_var.set, "Epicor error (see log)")

    def _show_epicor_bar(self, text: str) -> None:
        self._epicor_var.set(text)
        self._epicor_bar.pack(fill=tk.X)

    def _hide_epicor_bar(self) -> None:
        self._epicor_bar.pack_forget()
        self._epicor_var.set("")

    def _flag_rev_mismatch(self) -> None:
        """Turn the info bar red to signal a revision mismatch."""
        self._info_label.config(bg="#f4b8b8")

    @staticmethod
    def _latest_epicor_rev(part_revs: list) -> str | None:
        return _latest_epicor_rev(part_revs)

    # ------------------------------------------------------------------
    # PDF rendering
    # ------------------------------------------------------------------

    def _load_pdf(self, pdf_bytes: bytes, part_num: str, rev: str) -> None:
        self._win.title(f"Drawing \u2014 {part_num}  Rev {rev}")
        self._doc      = fitz.open(stream=pdf_bytes, filetype="pdf")
        self._page_idx = 0
        self._zoom     = self._fit_zoom()
        self._render_page()

    def _fit_zoom(self) -> float:
        if not self._doc:
            return self._zoom
        self._canvas.update_idletasks()
        cw = self._canvas.winfo_width()
        ch = self._canvas.winfo_height()
        if cw < 10 or ch < 10:
            return self._zoom
        page = self._doc[0]
        return max(0.2, min(round(min(cw / page.rect.width, ch / page.rect.height), 3), 6.0))

    def _render_page(self) -> None:
        if not self._doc:
            return
        page = self._doc[self._page_idx]
        mat  = fitz.Matrix(self._zoom, self._zoom)
        pix  = page.get_pixmap(matrix=mat, alpha=False)
        img  = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        self._photo = ImageTk.PhotoImage(img)
        self._canvas.delete("all")
        self._canvas.create_image(0, 0, anchor=tk.NW, image=self._photo)
        self._canvas.configure(scrollregion=(0, 0, pix.width, pix.height))
        self._canvas.yview_moveto(0)
        n = len(self._doc)
        self._page_label.config(text=f"{self._page_idx + 1} / {n}")
        self._zoom_label.config(text=f"{self._zoom:.0%}")
        self._status_var.set(
            f"Page {self._page_idx + 1} of {n}   |   zoom {self._zoom:.0%}"
            f"   |   Ctrl+scroll to zoom   |   Scroll to pan"
        )

    def _canvas_message(self, text: str) -> None:
        self._doc = None
        self._page_label.config(text="")
        self._canvas.delete("all")
        w = self._canvas.winfo_width()  or 600
        h = self._canvas.winfo_height() or 400
        self._canvas.create_text(w // 2, h // 2, text=text,
                                  font=("", 14), fill="#cccccc")
        self._canvas.configure(scrollregion=(0, 0, w, h))

    def _prev_page(self) -> None:
        if self._doc and self._page_idx > 0:
            self._page_idx -= 1
            self._render_page()

    def _next_page(self) -> None:
        if self._doc and self._page_idx < len(self._doc) - 1:
            self._page_idx += 1
            self._render_page()

    def _zoom_fit(self) -> None:
        if self._doc:
            self._zoom = self._fit_zoom()
            self._render_page()

    def _zoom_in(self) -> None:
        self._zoom = min(round(self._zoom * 1.25, 3), 6.0)
        self._render_page()

    def _zoom_out(self) -> None:
        self._zoom = max(round(self._zoom / 1.25, 3), 0.2)
        self._render_page()

    def _on_scroll(self, event: tk.Event) -> None:
        self._canvas.yview_scroll(int(-1 * event.delta / 120), "units")

    def _on_ctrl_scroll(self, event: tk.Event) -> None:
        if event.delta > 0:
            self._zoom_in()
        else:
            self._zoom_out()


# ---------------------------------------------------------------------------
# BOM viewer window
# ---------------------------------------------------------------------------

class BOMViewerWindow:
    """
    BOM Viewer — shows a Bill of Materials in an indented tree table.

    Columns (after the Part Number tree column):
      Seq | Description | Rev | Qty/Assy | UOM | Sub-Asm | Supplier |
      On Hand | Open POs | Rcvd (6mo)

    BOM structure is fetched first; stock / PO data fills in via background
    threads.  Double-click any row to open its drawing in a new window.
    """

    _COLUMNS = [
        # (column_id,   header,        width,  anchor,    stretch)
        ("seq",       "Seq",            45,   "e",       False),
        ("desc",      "Description",   220,   "w",       True),
        ("rev",       "BOM Rev",        52,   "w",       False),
        ("drw_rev",   "Drw Rev",        52,   "w",       False),
        ("epr_rev",   "ERP Rev",        52,   "w",       False),
        ("qty",       "Qty/Assy",       72,   "e",       False),
        ("uom",       "UOM",            46,   "center",  False),
        ("subasm",    "Sub-Asm",        58,   "center",  False),
        ("supplier",  "Supplier",      155,   "w",       True),
        ("onhand",    "On Hand",        72,   "e",       False),
        ("open_pos",  "Open POs",      180,   "w",       True),
        ("rcvd_6mo",  "Rcvd (6mo)",     78,   "e",       False),
    ]
    _COL_IDX = {col_id: i for i, (col_id, *_) in enumerate(_COLUMNS)}

    LOOKBACK_DAYS = 180  # ~6 months for the "Rcvd" column

    def __init__(self, parent: "DrawingViewer") -> None:
        self._parent  = parent
        self._epicor  = None          # EpicorClient — lazy
        self._iid_to_part: dict = {}  # treeview iid → part_num

        self._win = tk.Toplevel(parent.root)
        self._win.title("BOM Viewer")
        self._win.geometry("1300x700")
        self._win.minsize(900, 400)

        self._multi_level  = tk.BooleanVar(value=False)
        self._tree_parts   = tk.BooleanVar(value=False)
        self._status_var   = tk.StringVar(value="Enter a part number and press Enter.")
        self._info_var     = tk.StringVar(value="")
        self._fetch_total  = 0
        self._fetch_done   = 0

        self._build_ui()
        self._win.lift()
        self._win.focus_set()

    # ------------------------------------------------------------------
    # Lazy Epicor client
    # ------------------------------------------------------------------

    @property
    def _client(self):
        if self._epicor is None:
            self._epicor = _make_epicor_client()
        return self._epicor

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        # ── Toolbar ────────────────────────────────────────────────────
        top = tk.Frame(self._win, padx=6, pady=5)
        top.pack(fill=tk.X, side=tk.TOP)

        tk.Label(top, text="Part Number:", font=("", 11)).pack(side=tk.LEFT)
        self._part_entry = tk.Entry(top, width=12, font=("Courier", 13, "bold"))
        self._part_entry.pack(side=tk.LEFT, padx=(5, 10))
        self._part_entry.bind("<Return>", lambda _e: self._start_lookup())
        self._part_entry.focus_set()

        tk.Button(top, text="Look Up", command=self._start_lookup).pack(side=tk.LEFT)
        tk.Checkbutton(
            top, text="Multi-level (expand subassemblies)",
            variable=self._multi_level,
        ).pack(side=tk.LEFT, padx=(18, 0))

        tk.Checkbutton(top, text="w/ parts", variable=self._tree_parts).pack(side=tk.RIGHT)
        tk.Button(top, text="DrawPack",       command=self._open_draw_pack).pack(side=tk.RIGHT, padx=(0, 4))
        tk.Button(top, text="Asm Tree\u2026",   command=self._export_assembly_tree).pack(side=tk.RIGHT, padx=(0, 4))
        tk.Button(top, text="Export PDF\u2026", command=self._export_pdf).pack(side=tk.RIGHT, padx=(0, 4))
        tk.Button(top, text="Export CSV\u2026", command=self._export_csv).pack(side=tk.RIGHT)

        # ── Info bar ───────────────────────────────────────────────────
        tk.Label(
            self._win, textvariable=self._info_var,
            anchor="w", bg="#d6e8f7", padx=8, pady=2, font=("", 10),
        ).pack(fill=tk.X, side=tk.TOP)

        # ── Progress bar (shown while background threads are running) ──
        self._prog_frame = tk.Frame(self._win, pady=2)
        self._progbar = ttk.Progressbar(
            self._prog_frame, orient="horizontal", mode="determinate",
        )
        self._progbar.pack(fill=tk.X, padx=6)
        # Not packed yet — shown when fetch starts, hidden when complete

        # ── Status bar ─────────────────────────────────────────────────
        tk.Label(
            self._win, textvariable=self._status_var,
            anchor="w", relief=tk.SUNKEN, padx=5, font=("", 9),
        ).pack(fill=tk.X, side=tk.BOTTOM)

        # ── Treeview ───────────────────────────────────────────────────
        self._tv_frame = tk.Frame(self._win)
        self._tv_frame.pack(fill=tk.BOTH, expand=True)

        col_ids = [c[0] for c in self._COLUMNS]
        self._tree = ttk.Treeview(
            self._tv_frame, columns=col_ids, show="tree headings", selectmode="browse",
        )

        # Tree column = Part Number (with indentation arrows)
        self._tree.heading("#0", text="Part Number", anchor="w")
        self._tree.column("#0", width=165, minwidth=100, stretch=False, anchor="w")

        for col_id, heading, width, anchor, stretch in self._COLUMNS:
            self._tree.heading(col_id, text=heading, anchor=anchor)
            self._tree.column(col_id, width=width, minwidth=28,
                              anchor=anchor, stretch=stretch)

        self._tree.tag_configure("subasm",       background="#eaf4fb")
        self._tree.tag_configure("rev_mismatch", background="#fff0cc")  # amber — BOM rev ≠ ERP rev
        self._tree.tag_configure("rev_err",      background="#f4b8b8")  # red   — ERP rev ≠ drawing rev

        vsb = ttk.Scrollbar(self._tv_frame, orient="vertical",   command=self._tree.yview)
        hsb = ttk.Scrollbar(self._tv_frame, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT,  fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)

        self._tree.bind("<Double-1>", self._on_double_click)

    # ------------------------------------------------------------------
    # Seed from main window
    # ------------------------------------------------------------------

    def seed_part(self, part_num: str) -> None:
        """Pre-fill the entry and trigger a lookup (called from the main window)."""
        self._part_entry.delete(0, tk.END)
        self._part_entry.insert(0, part_num)
        self._start_lookup()

    # ------------------------------------------------------------------
    # CSV export
    # ------------------------------------------------------------------

    def _export_csv(self) -> None:
        """Export the current BOM tree to a CSV file chosen by the user."""
        import csv
        from tkinter import filedialog

        if not self._tree.get_children():
            return  # nothing to export

        part_num = self._part_entry.get().strip().upper() or "bom"
        default_name = f"{part_num}_bom.csv"

        path = filedialog.asksaveasfilename(
            parent=self._win,
            title="Export BOM to CSV",
            initialfile=default_name,
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return

        col_headers = ["level", "part_num"] + [c[1] for c in self._COLUMNS]

        rows: list[list] = []

        def _walk(parent_iid: str, level: int) -> None:
            for iid in self._tree.get_children(parent_iid):
                part = self._tree.item(iid, "text")
                vals = list(self._tree.item(iid, "values"))
                rows.append([level, part] + vals)
                _walk(iid, level + 1)

        _walk("", 0)

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(col_headers)
                writer.writerows(rows)
            self._status_var.set(f"Exported {len(rows)} rows to {path}")
        except Exception as exc:
            self._status_var.set(f"Export failed: {exc}")
            print(f"[BOM] CSV export failed: {exc}")

    def _export_pdf(self) -> None:
        """Export the current BOM tree to a PDF file."""
        from tkinter import filedialog

        if not self._tree.get_children():
            return

        part_num     = self._part_entry.get().strip().upper() or "bom"
        default_name = f"{part_num}_bom.pdf"

        path = filedialog.asksaveasfilename(
            parent=self._win,
            title="Export BOM to PDF",
            initialfile=default_name,
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        if not path:
            return

        rows: list = []

        def _walk(parent_iid: str, level: int) -> None:
            for iid in self._tree.get_children(parent_iid):
                part = self._tree.item(iid, "text")
                vals = list(self._tree.item(iid, "values"))
                rows.append((level, part, vals))
                _walk(iid, level + 1)

        _walk("", 0)

        try:
            _render_bom_pdf(path, part_num, self._info_var.get(), rows, self._COLUMNS)
            self._status_var.set(f"PDF saved to {path}")
        except Exception as exc:
            import traceback
            traceback.print_exc()
            self._status_var.set(f"PDF export failed: {exc}")

    def _export_assembly_tree(self) -> None:
        """Generate an assembly tree diagram PDF via d2 + ELK."""
        import shutil
        from tkinter import filedialog, messagebox

        part_num = self._part_entry.get().strip().upper()
        if not part_num:
            messagebox.showwarning("No Part", "Enter a part number first.", parent=self._win)
            return

        if not shutil.which("d2"):
            messagebox.showerror(
                "d2 Not Found",
                "The 'd2' diagram tool is not installed or not on PATH.\n\n"
                "Install it with:\n  choco install d2\n\n"
                "Or see: https://d2lang.com/tour/install",
                parent=self._win,
            )
            return

        path = filedialog.asksaveasfilename(
            parent=self._win,
            title="Save Assembly Tree PDF",
            initialfile=f"{part_num}_tree.pdf",
            defaultextension=".pdf",
            filetypes=[("PDF files", "*.pdf"), ("All files", "*.*")],
        )
        if not path:
            return

        show_parts = self._tree_parts.get()
        self._status_var.set(f"Building assembly tree for {part_num}\u2026")
        threading.Thread(
            target=self._do_assembly_tree, args=(part_num, path, show_parts), daemon=True
        ).start()

    def _do_assembly_tree(self, part_num: str, pdf_path: str, show_parts: bool = False) -> None:
        import subprocess
        try:
            from bom_lookup import build_assembly_tree, generate_d2_source

            root_bom = self._client.get_bom(part_num)
            parts    = {} if show_parts else None
            edges    = build_assembly_tree(self._client, part_num, parts=parts)

            root_id    = f"{part_num}_latest"
            root_label = (
                f"{part_num}\\n{root_bom.description or ''}"[:50]
                if root_bom.description else part_num
            )
            d2_source = generate_d2_source(edges, root_id, root_label, parts=parts)

            base    = pdf_path[:-4] if pdf_path.lower().endswith(".pdf") else pdf_path
            d2_file = base + ".d2"
            with open(d2_file, "w", encoding="utf-8") as f:
                f.write(d2_source)

            self._ui(self._status_var.set, "Running d2 renderer\u2026")
            result = subprocess.run(
                ["d2", "--layout", "elk", d2_file, pdf_path],
                capture_output=True, text=True,
            )
            if result.returncode != 0:
                print(f"[AsmTree] d2 stderr:\n{result.stderr}", flush=True)
                self._ui(self._status_var.set, "d2 render failed \u2014 see application log")
            else:
                self._ui(self._status_var.set, f"Assembly tree saved to {pdf_path}")

        except Exception as exc:
            import traceback
            traceback.print_exc()
            self._ui(self._status_var.set, f"Assembly tree failed: {str(exc)[:120]}")

    def _open_draw_pack(self) -> None:
        """Collect purchased piece parts from the BOM and open DrawPackWindow."""
        from tkinter import messagebox

        col_subasm   = self._COL_IDX["subasm"]
        col_supplier = self._COL_IDX["supplier"]
        parts: list = []

        def _walk(parent_iid: str) -> None:
            for iid in self._tree.get_children(parent_iid):
                part_num = self._tree.item(iid, "text")
                vals     = self._tree.item(iid, "values")
                if part_num and not part_num.startswith("(") and vals:
                    subasm   = vals[col_subasm]
                    supplier = vals[col_supplier]
                    if subasm != "Y":
                        parts.append((part_num, supplier))
                _walk(iid)

        _walk("")

        if not parts:
            messagebox.showinfo(
                "No Parts", "No piece parts found in the current BOM.", parent=self._win,
            )
            return

        DrawPackWindow(self._win, parts, self._parent.db, self._parent.box)

    # ------------------------------------------------------------------
    # BOM load — phase 1: collect data off main thread
    # ------------------------------------------------------------------

    def _ui(self, fn, *args) -> None:
        if self._win.winfo_exists():
            self._win.after(0, fn, *args)

    def _start_lookup(self) -> None:
        part_num = self._part_entry.get().strip().upper()
        if not part_num:
            return
        children = self._tree.get_children()
        if children:
            self._tree.delete(*children)
        self._iid_to_part.clear()
        self._fetch_total = 0
        self._fetch_done  = 0
        self._prog_frame.pack_forget()
        self._info_var.set("")
        self._status_var.set(f"Loading BOM for {part_num}\u2026")
        self._win.title(f"BOM Viewer \u2014 {part_num}")
        threading.Thread(
            target=self._do_load,
            args=(part_num, self._multi_level.get()),
            daemon=True,
        ).start()

    def _do_load(self, part_num: str, recursive: bool) -> None:
        try:
            bom = self._client.get_bom(part_num)
            rev_str  = bom.revision or "\u2014"
            approved = (
                "approved"     if bom.approved is True  else
                "not approved" if bom.approved is False else ""
            )
            info = f"{part_num}   Rev {rev_str}   {bom.description or ''}"
            if approved:
                info += f"   ({approved})"
            self._ui(self._info_var.set, info)

            if not bom.components:
                self._ui(self._status_var.set, f"No BOM components found for {part_num}.")
                return

            self._ui(self._status_var.set, "Building BOM tree\u2026")
            nodes = self._collect_nodes(part_num, recursive, set(), set(), root_bom=bom)
            self._ui(self._populate_tree, nodes)

        except Exception as exc:
            import traceback
            traceback.print_exc()
            self._ui(self._status_var.set,  f"Error: {str(exc)[:120]}")
            self._ui(self._info_var.set,    f"Error loading BOM: {str(exc)[:100]}")

    def _collect_nodes(
        self,
        part_num: str,
        recursive: bool,
        ancestors: set,
        processed: set,
        depth: int = 0,
        root_bom=None,
    ) -> list:
        """
        Recursively collect BOM data into a list of node dicts (background thread).
        Each node: {"comp": BOMComponent, "is_subasm": bool, "children": [node...]}
        Special nodes: {"special": str} for circular refs / errors.

        root_bom: if provided, used instead of fetching get_bom() again for the
        top-level part — avoids a duplicate API call from _do_load.
        """
        if depth > 20:
            return [{"special": f"(max depth at {part_num})"}]
        if part_num in ancestors:
            return [{"special": f"(circular reference: {part_num})"}]
        if part_num in processed and recursive:
            return [{"special": f"(already expanded: {part_num})"}]

        ancestors = set(ancestors)   # copy so sibling branches are independent
        ancestors.add(part_num)
        processed.add(part_num)

        try:
            bom = root_bom if (root_bom is not None and depth == 0) else self._client.get_bom(part_num)
        except Exception as exc:
            return [{"special": f"(error loading {part_num}: {exc})"}]

        nodes = []
        seen_seqs: set = set()
        for comp in bom.components:
            if comp.mtl_seq in seen_seqs:
                print(f"[BOM] duplicate mtl_seq {comp.mtl_seq} ({comp.part_num}) in {part_num} — skipped")
                continue
            seen_seqs.add(comp.mtl_seq)
            is_subasm = comp.pull_as_asm or comp.view_as_asm
            node: dict = {"comp": comp, "is_subasm": is_subasm, "children": []}
            if recursive and is_subasm:
                node["children"] = self._collect_nodes(
                    comp.part_num, recursive, ancestors, processed, depth + 1
                )
            nodes.append(node)
        return nodes

    # ------------------------------------------------------------------
    # BOM load — phase 2: populate Treeview on main thread
    # ------------------------------------------------------------------

    @staticmethod
    def _count_nodes(nodes: list) -> int:
        """Recursively count real component nodes (excludes special/error nodes)."""
        total = 0
        for node in nodes:
            if "special" not in node:
                total += 1 + BOMViewerWindow._count_nodes(node.get("children", []))
        return total

    def _populate_tree(self, nodes: list, parent_iid: str = "") -> None:
        """
        Walk collected nodes, insert Treeview rows, then fire per-part
        background threads for stock / PO data.
        """
        # On the first (top-level) call, set up the progress bar
        if parent_iid == "":
            total = self._count_nodes(nodes)
            self._fetch_total = total
            self._fetch_done  = 0
            if total > 0:
                self._progbar.config(maximum=total, value=0)
                self._prog_frame.pack(fill=tk.X, before=self._tv_frame)
                self._status_var.set(f"Fetching data: 0 / {total}")

        for node in nodes:
            if "special" in node:
                self._tree.insert(parent_iid, tk.END, text=node["special"])
                continue

            comp      = node["comp"]
            is_subasm = node["is_subasm"]
            tags      = ("subasm",) if is_subasm else ()

            iid = self._tree.insert(
                parent_iid, tk.END,
                text=comp.part_num,
                values=(
                    comp.mtl_seq,
                    comp.description or "",
                    comp.revision or "",  # BOM Rev
                    "\u2026",             # Drw Rev  — filled by background thread
                    "\u2026",             # ERP Rev  — filled by background thread
                    f"{comp.qty_per:g}",
                    comp.uom or "",
                    "Y" if is_subasm else "",
                    comp.vendor_name or "",
                    "\u2026",  # On Hand   — filled by background thread
                    "\u2026",  # Open POs  — filled by background thread
                    "\u2026",  # Rcvd 6mo  — filled by background thread
                ),
                tags=tags,
                open=True,
            )
            self._iid_to_part[iid] = comp.part_num

            if node["children"]:
                self._populate_tree(node["children"], parent_iid=iid)

            # Fire a background thread for this component's stock / PO data
            threading.Thread(
                target=self._fetch_stock_po,
                args=(iid, comp.part_num),
                daemon=True,
            ).start()

        if not parent_iid:
            n = len(self._iid_to_part)
            self._status_var.set(
                f"{n} component{'s' if n != 1 else ''} loaded."
                f"  Fetching stock / PO data\u2026"
                f"   (double-click a row to view drawing)"
            )

    # ------------------------------------------------------------------
    # Stock / PO data (background thread, one per component)
    # ------------------------------------------------------------------

    def _fetch_stock_po(self, iid: str, part_num: str) -> None:
        """Fetch on-hand qty, PO data, and drawing revision for one part."""
        from datetime import datetime, timedelta
        cutoff = datetime.now() - timedelta(days=self.LOOKBACK_DAYS)

        # — Drawing revision from local DB —
        try:
            drawing = self._parent.db.get_latest_drawing(part_num)
            drw_rev = (drawing.revision or "\u2014") if drawing else "\u2014"
        except Exception as exc:
            print(f"[BOM] drawing rev lookup failed for {part_num}: {exc}")
            drw_rev = "err"

        # — Current approved revision from Epicor part master —
        # Uses PartSvc/GetByID POST to avoid the OData $filter percent-encoding bug.
        try:
            import json as _json
            import requests as _requests
            from config import (
                EPICOR_API_KEY, EPICOR_BASE_URL, EPICOR_COMPANY,
                EPICOR_PASSWORD, EPICOR_PLANT, EPICOR_USERNAME,
            )
            _resp = _requests.post(
                f"{EPICOR_BASE_URL}/api/v2/odata/{EPICOR_COMPANY}/Erp.BO.PartSvc/GetByID",
                auth=(EPICOR_USERNAME, EPICOR_PASSWORD),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "CallSettings": _json.dumps({"Company": EPICOR_COMPANY, "Plant": EPICOR_PLANT}),
                },
                params={"api-key": EPICOR_API_KEY},
                json={"partNum": part_num},
                timeout=15,
                verify=False,
            )
            _resp.raise_for_status()
            _obj       = _resp.json().get("returnObj", {})
            epr_rev = _latest_epicor_rev(_obj.get("PartRev", [])) or "\u2014"
        except Exception as exc:
            print(f"[BOM] ERP rev lookup failed for {part_num}: {exc}")
            epr_rev = "err"

        # — On-hand qty —
        try:
            qty_summary = self._client.get_qty_on_hand(part_num)
            oh = qty_summary.total_on_hand
            onhand_str = f"{oh:g}" if oh else "0"
        except Exception as exc:
            print(f"[BOM] on-hand lookup failed for {part_num}: {exc}")
            onhand_str = "err"

        # — PO lines —
        open_pos_str = "err"
        rcvd_str     = "err"
        try:
            po_lines = self._client.get_po_lines_by_partnum(part_num)

            open_count = 0
            open_qty   = 0.0
            open_po_nums: list[int] = []
            rcvd_qty   = 0.0

            for po in po_lines:
                if po.status == "open":
                    open_count += 1
                    open_qty   += po.order_qty or 0.0
                    if po.po_num not in open_po_nums:
                        open_po_nums.append(po.po_num)
                elif po.status == "closed":
                    # Use order_date (proxy for recency; Epicor has no receipt date in POLineMatch)
                    date_str = po.order_date or po.due_date
                    if date_str:
                        try:
                            if datetime.fromisoformat(date_str[:10]) >= cutoff:
                                rcvd_qty += po.received_qty or 0.0
                        except ValueError:
                            pass

            if open_count:
                po_list = ", ".join(str(n) for n in sorted(open_po_nums))
                open_pos_str = f"{open_count} ({open_qty:g})  [{po_list}]"
            else:
                open_pos_str = "0"
            rcvd_str     = f"{rcvd_qty:g}" if rcvd_qty else "0"
        except Exception as exc:
            print(f"[BOM] PO lookup failed for {part_num}: {exc}")

        self._ui(self._update_row, iid, drw_rev, epr_rev, onhand_str, open_pos_str, rcvd_str)

    def _update_row(
        self, iid: str, drw_rev: str, epr_rev: str, onhand: str, open_pos: str, rcvd: str,
    ) -> None:
        """Update drawing rev / stock / PO columns for a Treeview row (main thread)."""
        _BAD = {"\u2014", "ERR", ""}
        try:
            vals = list(self._tree.item(iid, "values"))
            vals[self._COL_IDX["drw_rev"]]  = drw_rev
            vals[self._COL_IDX["epr_rev"]]  = epr_rev
            vals[self._COL_IDX["onhand"]]   = onhand
            vals[self._COL_IDX["open_pos"]] = open_pos
            vals[self._COL_IDX["rcvd_6mo"]] = rcvd
            self._tree.item(iid, values=vals)

            bom_norm = vals[self._COL_IDX["rev"]].strip().upper()
            drw_norm = drw_rev.strip().upper()
            epr_norm = epr_rev.strip().upper()

            # Red: ERP rev ≠ drawing rev (drawing is behind ERP)
            rev_err = (
                epr_norm not in _BAD and drw_norm not in _BAD
                and epr_norm != drw_norm
            )
            # Amber: BOM rev ≠ ERP rev (BOM references an outdated revision)
            rev_mismatch = (
                not rev_err
                and bom_norm not in _BAD and epr_norm not in _BAD
                and bom_norm != epr_norm
            )

            current = set(self._tree.item(iid, "tags"))
            current.discard("rev_err")
            current.discard("rev_mismatch")
            if rev_err:
                current.add("rev_err")
            elif rev_mismatch:
                current.add("rev_mismatch")
            self._tree.item(iid, tags=tuple(current))

            # Advance progress bar
            self._fetch_done += 1
            done  = self._fetch_done
            total = self._fetch_total
            self._progbar.config(value=done)
            if done >= total:
                self._prog_frame.pack_forget()
                self._status_var.set("Ready")
            else:
                self._status_var.set(f"Fetching data: {done} / {total}")

        except tk.TclError:
            pass  # window was closed while thread was running

    # ------------------------------------------------------------------
    # Row double-click → open drawing
    # ------------------------------------------------------------------

    def _on_double_click(self, event: tk.Event) -> None:
        iid = self._tree.identify_row(event.y)
        if not iid:
            return
        part_num = self._iid_to_part.get(iid)
        if part_num:
            DrawingViewToplevel(self._win, part_num, self._parent.db, self._parent.box)


# ---------------------------------------------------------------------------
# Where Used viewer window
# ---------------------------------------------------------------------------

class WhereUsedViewerWindow:
    """
    Where Used Viewer — shows all assemblies that use a given part, recursively
    tracing each assembly up through higher-level assemblies to final products.

    Columns (after the Assembly tree column):
      Description | BOM Rev | Qty Per | UOM | Final Product | On Hand

    The tree is built first; On Hand fills in via background threads.
    Green rows are final products (no assemblies use them further).
    Double-click any row to open its drawing.
    """

    _COLUMNS = [
        # (col_id,   header,          width,  anchor,    stretch)
        ("desc",    "Description",    220,   "w",       True),
        ("rev",     "BOM Rev",         52,   "w",       False),
        ("qty",     "Qty Per",         72,   "e",       False),
        ("uom",     "UOM",             46,   "center",  False),
        ("final",   "Final Prod",      72,   "center",  False),
        ("onhand",  "On Hand",         72,   "e",       False),
    ]
    _COL_IDX = {col_id: i for i, (col_id, *_) in enumerate(_COLUMNS)}

    def __init__(self, parent: "DrawingViewer") -> None:
        self._parent  = parent
        self._epicor  = None
        self._iid_to_part: dict = {}

        self._win = tk.Toplevel(parent.root)
        self._win.title("Where Used")
        self._win.geometry("1000x600")
        self._win.minsize(700, 300)

        self._status_var  = tk.StringVar(value="Enter a part number and press Enter.")
        self._info_var    = tk.StringVar(value="")
        self._fetch_total = 0
        self._fetch_done  = 0

        self._build_ui()
        self._win.lift()
        self._win.focus_set()

    # ------------------------------------------------------------------
    # Lazy Epicor client
    # ------------------------------------------------------------------

    @property
    def _client(self):
        if self._epicor is None:
            self._epicor = _make_epicor_client()
        return self._epicor

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        # ── Toolbar ────────────────────────────────────────────────────
        top = tk.Frame(self._win, padx=6, pady=5)
        top.pack(fill=tk.X, side=tk.TOP)

        tk.Label(top, text="Part Number:", font=("", 11)).pack(side=tk.LEFT)
        self._part_entry = tk.Entry(top, width=12, font=("Courier", 13, "bold"))
        self._part_entry.pack(side=tk.LEFT, padx=(5, 10))
        self._part_entry.bind("<Return>", lambda _e: self._start_lookup())
        self._part_entry.focus_set()

        tk.Button(top, text="Look Up", command=self._start_lookup).pack(side=tk.LEFT)
        tk.Button(top, text="Export CSV\u2026", command=self._export_csv).pack(side=tk.RIGHT)

        # ── Info bar ───────────────────────────────────────────────────
        tk.Label(
            self._win, textvariable=self._info_var,
            anchor="w", bg="#d6e8f7", padx=8, pady=2, font=("", 10),
        ).pack(fill=tk.X, side=tk.TOP)

        # ── Progress bar ───────────────────────────────────────────────
        self._prog_frame = tk.Frame(self._win, pady=2)
        self._progbar = ttk.Progressbar(
            self._prog_frame, orient="horizontal", mode="determinate",
        )
        self._progbar.pack(fill=tk.X, padx=6)

        # ── Status bar ─────────────────────────────────────────────────
        tk.Label(
            self._win, textvariable=self._status_var,
            anchor="w", relief=tk.SUNKEN, padx=5, font=("", 9),
        ).pack(fill=tk.X, side=tk.BOTTOM)

        # ── Treeview ───────────────────────────────────────────────────
        self._tv_frame = tk.Frame(self._win)
        self._tv_frame.pack(fill=tk.BOTH, expand=True)

        col_ids = [c[0] for c in self._COLUMNS]
        self._tree = ttk.Treeview(
            self._tv_frame, columns=col_ids, show="tree headings", selectmode="browse",
        )

        self._tree.heading("#0", text="Assembly", anchor="w")
        self._tree.column("#0", width=165, minwidth=100, stretch=False, anchor="w")

        for col_id, heading, width, anchor, stretch in self._COLUMNS:
            self._tree.heading(col_id, text=heading, anchor=anchor)
            self._tree.column(col_id, width=width, minwidth=28,
                              anchor=anchor, stretch=stretch)

        self._tree.tag_configure("final_product", background="#d4edda")  # green — final product
        self._tree.tag_configure("intermediate",  background="#eaf4fb")  # blue  — passes through

        vsb = ttk.Scrollbar(self._tv_frame, orient="vertical",   command=self._tree.yview)
        hsb = ttk.Scrollbar(self._tv_frame, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT,  fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)

        self._tree.bind("<Double-1>", self._on_double_click)

    # ------------------------------------------------------------------
    # Seed from main window
    # ------------------------------------------------------------------

    def seed_part(self, part_num: str) -> None:
        """Pre-fill the entry and trigger a lookup (called from the main window)."""
        self._part_entry.delete(0, tk.END)
        self._part_entry.insert(0, part_num)
        self._start_lookup()

    # ------------------------------------------------------------------
    # CSV export
    # ------------------------------------------------------------------

    def _export_csv(self) -> None:
        import csv
        from tkinter import filedialog

        if not self._tree.get_children():
            return

        part_num = self._part_entry.get().strip().upper() or "whereused"
        path = filedialog.asksaveasfilename(
            parent=self._win,
            title="Export Where Used to CSV",
            initialfile=f"{part_num}_whereused.csv",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return

        col_headers = ["level", "assembly_part"] + [c[1] for c in self._COLUMNS]
        rows: list[list] = []

        def _walk(parent_iid: str, level: int) -> None:
            for iid in self._tree.get_children(parent_iid):
                part = self._tree.item(iid, "text")
                vals = list(self._tree.item(iid, "values"))
                rows.append([level, part] + vals)
                _walk(iid, level + 1)

        _walk("", 0)

        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(col_headers)
                writer.writerows(rows)
            self._status_var.set(f"Exported {len(rows)} rows to {path}")
        except Exception as exc:
            self._status_var.set(f"Export failed: {exc}")
            print(f"[WhereUsed] CSV export failed: {exc}")

    # ------------------------------------------------------------------
    # Where-used load — phase 1: collect data off main thread
    # ------------------------------------------------------------------

    def _ui(self, fn, *args) -> None:
        if self._win.winfo_exists():
            self._win.after(0, fn, *args)

    def _start_lookup(self) -> None:
        part_num = self._part_entry.get().strip().upper()
        if not part_num:
            return
        children = self._tree.get_children()
        if children:
            self._tree.delete(*children)
        self._iid_to_part.clear()
        self._fetch_total = 0
        self._fetch_done  = 0
        self._prog_frame.pack_forget()
        self._info_var.set("")
        self._status_var.set(f"Loading where-used for {part_num}\u2026")
        self._win.title(f"Where Used \u2014 {part_num}")
        threading.Thread(
            target=self._do_load,
            args=(part_num,),
            daemon=True,
        ).start()

    def _do_load(self, part_num: str) -> None:
        try:
            desc = self._client.get_part_description(part_num) or ""
            self._ui(self._info_var.set, f"{part_num}   {desc}")

            nodes = self._collect_where_used(part_num, set())
            if not nodes:
                self._ui(
                    self._status_var.set,
                    f"{part_num} is not used in any assembly.",
                )
                return

            self._ui(self._status_var.set, "Building where-used tree\u2026")
            self._ui(self._populate_tree, nodes)

        except Exception as exc:
            import traceback
            traceback.print_exc()
            self._ui(self._status_var.set,  f"Error: {str(exc)[:120]}")
            self._ui(self._info_var.set,    f"Error: {str(exc)[:100]}")

    def _collect_where_used(self, part_num: str, ancestors: set, depth: int = 0) -> list:
        """
        Recursively collect where-used nodes (background thread).

        Each node dict:
          {"entry": WhereUsedEntry, "children": [...], "is_final": bool}
        Special nodes:
          {"special": str}  — for depth limits / circular refs / errors.

        ancestors: set of part numbers on the current path (for circular detection).
        """
        if depth > 15:
            return [{"special": f"(max depth at {part_num})"}]
        if part_num in ancestors:
            return [{"special": f"(circular: {part_num})"}]

        ancestors = set(ancestors)   # copy so sibling branches are independent
        ancestors.add(part_num)

        try:
            entries = self._client.get_where_used(part_num)
        except Exception as exc:
            return [{"special": f"(error looking up {part_num}: {exc})"}]

        nodes = []
        for entry in entries:
            children = self._collect_where_used(entry.assembly_part, ancestors, depth + 1)
            # A node is a "final product" when no real (non-special) children exist,
            # meaning no assemblies use this assembly further up the chain.
            is_final = not any("special" not in c for c in children)
            nodes.append({
                "entry": entry,
                "children": children,
                "is_final": is_final,
            })
        return nodes

    # ------------------------------------------------------------------
    # Where-used load — phase 2: populate Treeview on main thread
    # ------------------------------------------------------------------

    @staticmethod
    def _count_nodes(nodes: list) -> int:
        total = 0
        for node in nodes:
            if "special" not in node:
                total += 1 + WhereUsedViewerWindow._count_nodes(node.get("children", []))
        return total

    def _populate_tree(self, nodes: list, parent_iid: str = "") -> None:
        """Walk collected nodes, insert Treeview rows, fire On Hand background threads."""
        if parent_iid == "":
            total = self._count_nodes(nodes)
            self._fetch_total = total
            self._fetch_done  = 0
            if total > 0:
                self._progbar.config(maximum=total, value=0)
                self._prog_frame.pack(fill=tk.X, before=self._tv_frame)
                self._status_var.set(f"Fetching data: 0 / {total}")

        for node in nodes:
            if "special" in node:
                self._tree.insert(parent_iid, tk.END, text=node["special"])
                continue

            entry    = node["entry"]
            is_final = node["is_final"]
            tags     = ("final_product",) if is_final else ("intermediate",)

            iid = self._tree.insert(
                parent_iid, tk.END,
                text=entry.assembly_part,
                values=(
                    entry.assembly_description or "",
                    entry.revision or "",
                    f"{entry.qty_per:g}",
                    entry.uom or "",
                    "Y" if is_final else "",
                    "\u2026",  # On Hand — filled by background thread
                ),
                tags=tags,
                open=True,
            )
            self._iid_to_part[iid] = entry.assembly_part

            if node["children"]:
                self._populate_tree(node["children"], parent_iid=iid)

            threading.Thread(
                target=self._fetch_onhand,
                args=(iid, entry.assembly_part),
                daemon=True,
            ).start()

        if not parent_iid:
            n = len(self._iid_to_part)
            self._status_var.set(
                f"{n} assembl{'ies' if n != 1 else 'y'} found."
                "  Fetching stock data\u2026"
                "   (double-click a row to view drawing)"
            )

    # ------------------------------------------------------------------
    # On Hand data (background thread, one per assembly row)
    # ------------------------------------------------------------------

    def _fetch_onhand(self, iid: str, part_num: str) -> None:
        try:
            qty_summary = self._client.get_qty_on_hand(part_num)
            oh = qty_summary.total_on_hand
            onhand_str = f"{oh:g}" if oh else "0"
        except Exception as exc:
            print(f"[WhereUsed] on-hand lookup failed for {part_num}: {exc}")
            onhand_str = "err"

        self._ui(self._update_row, iid, onhand_str)

    def _update_row(self, iid: str, onhand: str) -> None:
        try:
            vals = list(self._tree.item(iid, "values"))
            vals[self._COL_IDX["onhand"]] = onhand
            self._tree.item(iid, values=vals)

            self._fetch_done += 1
            done  = self._fetch_done
            total = self._fetch_total
            self._progbar.config(value=done)
            if done >= total:
                self._prog_frame.pack_forget()
                self._status_var.set("Ready")
            else:
                self._status_var.set(f"Fetching data: {done} / {total}")
        except tk.TclError:
            pass  # window was closed while thread was running

    # ------------------------------------------------------------------
    # Row double-click → open drawing
    # ------------------------------------------------------------------

    def _on_double_click(self, event: tk.Event) -> None:
        iid = self._tree.identify_row(event.y)
        if not iid:
            return
        part_num = self._iid_to_part.get(iid)
        if part_num:
            DrawingViewToplevel(self._win, part_num, self._parent.db, self._parent.box)


# ---------------------------------------------------------------------------
# Job Summary viewer window
# ---------------------------------------------------------------------------

def _render_bom_pdf(path: str, part_num: str, header_text: str, rows: list, col_defs: list) -> None:
    """
    Write a BOM PDF to *path* using PyMuPDF.

    rows     — list of (level:int, part_num:str, values:list[str])
    col_defs — BOMViewerWindow._COLUMNS  (col_id, header, width, anchor, stretch)
    """
    from datetime import datetime as _dt2

    # ── Page / layout constants ──────────────────────────────────────
    PAGE_W, PAGE_H = 792, 612          # landscape letter
    ML, MR, MT, MB = 28, 28, 50, 36   # margins
    USABLE_W = PAGE_W - ML - MR

    ROW_H   = 13       # body row height (pts)
    HDR_H   = 16       # column-header row height
    FS      = 6.5      # body font size
    FS_HDR  = 7.0      # column-header font size
    FS_TTL  = 10.0     # page title font size
    INDENT  = 10       # extra x-indent per hierarchy level (first column only)

    HDR_BG  = (0.18, 0.38, 0.60)   # dark blue header background
    WHITE   = (1.00, 1.00, 1.00)
    ALT_BG  = (0.94, 0.97, 1.00)   # alternating row tint
    LINE_C  = (0.78, 0.78, 0.82)   # grid line colour
    TITLE_C = (0.10, 0.10, 0.10)
    SUB_C   = (0.45, 0.45, 0.45)

    # ── Column layout ───────────────────────────────────────────────
    # Prepend the tree "#0" column (Part Number) to the data columns.
    all_cols = [("part_num", "Part Number", 165, "w", False)] + list(col_defs)
    total_tw = sum(c[2] for c in all_cols)
    col_ws   = [c[2] / total_tw * USABLE_W for c in all_cols]
    col_hdrs = [c[1] for c in all_cols]
    col_alns = [
        fitz.TEXT_ALIGN_LEFT   if c[3] == "w"      else
        fitz.TEXT_ALIGN_RIGHT  if c[3] == "e"      else
        fitz.TEXT_ALIGN_CENTER
        for c in all_cols
    ]

    avail_body   = PAGE_H - MT - MB - HDR_H
    rows_per_page = max(1, int(avail_body / ROW_H))
    total_pages   = max(1, (len(rows) + rows_per_page - 1) // rows_per_page)
    now_str       = _dt2.now().strftime("Generated %Y-%m-%d  %H:%M")

    doc = fitz.open()

    def _page_chrome(page, pnum: int) -> float:
        """Draw title / footer; return y coordinate for content start."""
        page.insert_text(
            (ML, MT - 22),
            f"Bill of Materials \u2014 {part_num}",
            fontname="helvb", fontsize=FS_TTL, color=TITLE_C,
        )
        if header_text:
            page.insert_text(
                (ML, MT - 9),
                header_text,
                fontname="helv", fontsize=6.5, color=SUB_C,
            )
        page.insert_text(
            (ML, PAGE_H - MB + 14),
            now_str,
            fontname="helv", fontsize=6, color=SUB_C,
        )
        page.insert_text(
            (PAGE_W - MR - 72, PAGE_H - MB + 14),
            f"Page {pnum} of {total_pages}",
            fontname="helv", fontsize=6, color=SUB_C,
        )
        return MT

    def _header_row(page, y: float) -> float:
        page.draw_rect(fitz.Rect(ML, y, ML + USABLE_W, y + HDR_H), color=None, fill=HDR_BG)
        x = ML
        for hdr, w, aln in zip(col_hdrs, col_ws, col_alns):
            cell = fitz.Rect(x + 1, y + 1, x + w - 1, y + HDR_H - 1)
            page.insert_textbox(cell, hdr, fontname="helvb", fontsize=FS_HDR,
                                color=WHITE, align=aln)
            x += w
        return y + HDR_H

    def _data_row(page, y: float, vals: list, level: int, row_idx: int) -> None:
        bg = ALT_BG if row_idx % 2 == 1 else WHITE
        page.draw_rect(fitz.Rect(ML, y, ML + USABLE_W, y + ROW_H), color=None, fill=bg)
        page.draw_line((ML, y + ROW_H), (ML + USABLE_W, y + ROW_H), color=LINE_C, width=0.3)
        x = ML
        for i, (val, w, aln) in enumerate(zip(vals, col_ws, col_alns)):
            indent = level * INDENT if i == 0 else 0
            cell = fitz.Rect(x + 2 + indent, y + 1, x + w - 1, y + ROW_H - 1)
            page.insert_textbox(cell, str(val) if val is not None else "",
                                fontname="helv", fontsize=FS, color=(0, 0, 0), align=aln)
            x += w

    # ── Render ──────────────────────────────────────────────────────
    row_idx  = 0
    page_num = 0
    while True:
        page_num += 1
        page = doc.new_page(width=PAGE_W, height=PAGE_H)
        y = _page_chrome(page, page_num)
        y = _header_row(page, y)

        count = 0
        while row_idx < len(rows) and count < rows_per_page:
            level, part, tvals = rows[row_idx]
            _data_row(page, y, [part] + list(tvals), level, row_idx)
            y += ROW_H
            row_idx += 1
            count   += 1

        if row_idx >= len(rows):
            break

    doc.save(path)
    doc.close()


def _sanitize_folder_name(name: str) -> str:
    """Return a filesystem-safe version of *name* for use as a subfolder."""
    import re
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip(". ")
    return safe or "No Supplier"


def _fmt_date(s) -> str:
    """Format an ISO date string as MM/DD/YYYY."""
    if not s:
        return ""
    try:
        from datetime import datetime as _dt2
        return _dt2.fromisoformat(s.replace("Z", "+00:00")).strftime("%m/%d/%Y")
    except Exception:
        return s[:10] if len(s) >= 10 else s


def _fmt_qty(v) -> str:
    if v is None:
        return "?"
    f = float(v)
    return f"{int(f)}" if f == int(f) else f"{f:g}"


class JobSummaryWindow:
    """
    Job Summary — shows job header, materials, and purchase orders
    for a given Epicor job number.

    Header bar: part, description, qty, start/due dates, status
    Tab 1 (Materials): treeview of job BOM — double-click opens drawing
    Tab 2 (Purchase Orders): treeview of POs, colour-coded by status
    """

    _MTL_COLUMNS = [
        # (col_id,   header,         width,  anchor,   stretch)
        ("asm",    "Asm Seq",          55,   "e",      False),
        ("mtl",    "Mtl Seq",          55,   "e",      False),
        ("desc",   "Description",     220,   "w",      True),
        ("req",    "Req Qty",           72,   "e",      False),
        ("issued", "Issued",            72,   "e",      False),
        ("uom",    "UOM",               46,   "center", False),
        ("buy",    "B/M",               36,   "center", False),
        ("po_job",    "Job PO",         170,   "w",      False),
        ("po_due",    "PO Due",          90,   "w",      False),
        ("po_vendor", "Vendor",         160,   "w",      False),
    ]
    _MTL_COL_IDX = {col_id: i for i, (col_id, *_) in enumerate(_MTL_COLUMNS)}

    _PO_COLUMNS = [
        # (col_id,   header,     width,  anchor,   stretch)
        ("status", "Status",      65,   "center", False),
        ("po",     "PO #",        70,   "e",      False),
        ("line",   "Line",        45,   "e",      False),
        ("part",   "Part",       160,   "w",      False),
        ("qty",    "Ordered",     72,   "e",      False),
        ("rcvd",   "Received",    72,   "e",      False),
        ("due",    "Due Date",    90,   "w",      False),
        ("vendor", "Vendor",     200,   "w",      True),
    ]

    def __init__(self, parent: "DrawingViewer") -> None:
        self._parent = parent
        self._epicor = None

        self._win = tk.Toplevel(parent.root)
        self._win.title("Job Summary")
        self._win.geometry("1050x650")
        self._win.minsize(700, 400)

        self._status_var  = tk.StringVar(value="Enter a job number and press Enter.")
        self._header_var  = tk.StringVar(value="")
        self._mtl_iid_map: dict[str, str] = {}  # treeview iid → part_num
        self._lookup_gen  = 0   # incremented on each new lookup; stale threads check this

        self._build_ui()
        self._win.lift()
        self._win.focus_set()

    # ------------------------------------------------------------------
    # Lazy Epicor client
    # ------------------------------------------------------------------

    @property
    def _client(self):
        if self._epicor is None:
            self._epicor = _make_epicor_client()
        return self._epicor

    def _ui(self, fn, *args) -> None:
        if self._win.winfo_exists():
            self._win.after(0, fn, *args)

    def _ui_gen(self, gen: int, fn, *args) -> None:
        """Schedule fn(*args) on the main thread, but no-op if the lookup
        generation has changed by the time the callback actually runs."""
        def _guarded():
            if gen == self._lookup_gen and self._win.winfo_exists():
                fn(*args)
        if self._win.winfo_exists():
            self._win.after(0, _guarded)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        # ── Toolbar ────────────────────────────────────────────────────
        top = tk.Frame(self._win, padx=6, pady=5)
        top.pack(fill=tk.X, side=tk.TOP)

        tk.Label(top, text="Job Number:", font=("", 11)).pack(side=tk.LEFT)
        self._job_entry = tk.Entry(top, width=12, font=("Courier", 13, "bold"))
        self._job_entry.pack(side=tk.LEFT, padx=(5, 10))
        self._job_entry.bind("<Return>", lambda _e: self._start_lookup())
        self._job_entry.focus_set()

        tk.Button(top, text="Look Up", command=self._start_lookup).pack(side=tk.LEFT)
        tk.Button(top, text="DrawPack", command=self._open_draw_pack).pack(side=tk.LEFT, padx=(10, 0))

        # ── Header info bar ────────────────────────────────────────────
        self._header_label = tk.Label(
            self._win, textvariable=self._header_var,
            anchor="w", bg="#d6e8f7", padx=8, pady=5,
            font=("", 10), justify="left",
        )
        self._header_label.pack(fill=tk.X, side=tk.TOP)

        # ── Status bar ─────────────────────────────────────────────────
        tk.Label(
            self._win, textvariable=self._status_var,
            anchor="w", relief=tk.SUNKEN, padx=5, font=("", 9),
        ).pack(fill=tk.X, side=tk.BOTTOM)

        # ── Progress bar (shown while fetching POs) ────────────────────
        self._prog_frame = tk.Frame(self._win, pady=2)
        self._progbar = ttk.Progressbar(
            self._prog_frame, orient="horizontal", mode="indeterminate", length=300,
        )
        self._progbar.pack(fill=tk.X, padx=6)
        # Not packed until a lookup starts

        # ── Notebook (Materials | Purchase Orders) ─────────────────────
        self._notebook = ttk.Notebook(self._win)
        self._notebook.pack(fill=tk.BOTH, expand=True)

        # Tab 1 — Materials
        mtl_frame = tk.Frame(self._notebook)
        self._notebook.add(mtl_frame, text="Materials")

        self._mtl_tree = ttk.Treeview(
            mtl_frame,
            columns=[c[0] for c in self._MTL_COLUMNS],
            show="tree headings",
            selectmode="browse",
        )
        self._mtl_tree.heading("#0", text="Part Number", anchor="w")
        self._mtl_tree.column("#0", width=160, minwidth=80, stretch=False, anchor="w")
        for col_id, heading, width, anchor, stretch in self._MTL_COLUMNS:
            self._mtl_tree.heading(col_id, text=heading, anchor=anchor)
            self._mtl_tree.column(col_id, width=width, minwidth=28, anchor=anchor, stretch=stretch)

        self._mtl_tree.tag_configure("buy",  background="#eaf4fb")   # blue  — buy
        self._mtl_tree.tag_configure("make", background="#fff9e6")   # amber — make

        vsb1 = ttk.Scrollbar(mtl_frame, orient="vertical",   command=self._mtl_tree.yview)
        hsb1 = ttk.Scrollbar(mtl_frame, orient="horizontal", command=self._mtl_tree.xview)
        self._mtl_tree.configure(yscrollcommand=vsb1.set, xscrollcommand=hsb1.set)
        vsb1.pack(side=tk.RIGHT,  fill=tk.Y)
        hsb1.pack(side=tk.BOTTOM, fill=tk.X)
        self._mtl_tree.pack(fill=tk.BOTH, expand=True)
        self._mtl_tree.bind("<Double-1>", self._on_mtl_double_click)

        # Tab 2 — Purchase Orders
        po_frame = tk.Frame(self._notebook)
        self._notebook.add(po_frame, text="Purchase Orders")

        self._po_tree = ttk.Treeview(
            po_frame,
            columns=[c[0] for c in self._PO_COLUMNS],
            show="headings",
            selectmode="browse",
        )
        for col_id, heading, width, anchor, stretch in self._PO_COLUMNS:
            self._po_tree.heading(col_id, text=heading, anchor=anchor)
            self._po_tree.column(col_id, width=width, minwidth=28, anchor=anchor, stretch=stretch)

        self._po_tree.tag_configure("open",   background="#d4edda")  # green
        self._po_tree.tag_configure("closed", background="#f8f9fa")  # light grey
        self._po_tree.tag_configure("void",   background="#f4b8b8")  # red

        vsb2 = ttk.Scrollbar(po_frame, orient="vertical",   command=self._po_tree.yview)
        hsb2 = ttk.Scrollbar(po_frame, orient="horizontal", command=self._po_tree.xview)
        self._po_tree.configure(yscrollcommand=vsb2.set, xscrollcommand=hsb2.set)
        vsb2.pack(side=tk.RIGHT,  fill=tk.Y)
        hsb2.pack(side=tk.BOTTOM, fill=tk.X)
        self._po_tree.pack(fill=tk.BOTH, expand=True)

    # ------------------------------------------------------------------
    # Seed from main window (future use)
    # ------------------------------------------------------------------

    def seed_job(self, job_num: str) -> None:
        self._job_entry.delete(0, tk.END)
        self._job_entry.insert(0, job_num)
        self._start_lookup()

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def _start_lookup(self) -> None:
        job_num = self._job_entry.get().strip()
        if not job_num:
            return
        self._lookup_gen += 1          # invalidate any in-flight requests
        gen = self._lookup_gen
        for row in self._mtl_tree.get_children():
            self._mtl_tree.delete(row)
        for row in self._po_tree.get_children():
            self._po_tree.delete(row)
        self._mtl_iid_map.clear()
        self._header_var.set("")
        self._notebook.tab(0, text="Materials")
        self._notebook.tab(1, text="Purchase Orders")
        self._status_var.set(f"Loading job {job_num}\u2026")
        self._win.title(f"Job Summary \u2014 {job_num}")
        self._prog_frame.pack(fill=tk.X, before=self._notebook)
        self._progbar.start(12)
        threading.Thread(target=self._do_load, args=(job_num, gen), daemon=True).start()

    def _do_load(self, job_num: str, gen: int) -> None:
        def ui(fn, *args):
            self._ui_gen(gen, fn, *args)

        try:
            # ── Header + materials ──────────────────────────────────────
            header, materials = self._client.get_job(job_num)

            if gen != self._lookup_gen:
                return  # superseded before we even hit the UI

            # Build header text
            parts = [f"Job: {job_num}"]
            if header:
                if header.part_num:
                    parts.append(f"Part: {header.part_num}")
                if header.description:
                    parts.append(f"Desc: {header.description}")
                if header.prod_qty is not None:
                    parts.append(f"Qty: {_fmt_qty(header.prod_qty)} {header.uom or ''}")
                if header.start_date:
                    parts.append(f"Start: {_fmt_date(header.start_date)}")
                if header.due_date:
                    parts.append(f"Due: {_fmt_date(header.due_date)}")
                flags = []
                if header.released: flags.append("Released")
                if header.complete: flags.append("Complete")
                if header.closed:   flags.append("Closed")
                if flags:
                    parts.append(f"Status: {', '.join(flags)}")

            ui(self._header_var.set, "    |    ".join(parts))
            ui(self._populate_materials, materials)

            # ── POs ─────────────────────────────────────────────────────
            ui(self._status_var.set, f"Fetching POs for {job_num}\u2026")
            po_lines = self._client.get_job_pos(job_num)

            if gen != self._lookup_gen:
                return

            ui(self._populate_pos, po_lines)
            ui(self._update_mtl_pos, po_lines)
            ui(self._hide_progress)
            ui(self._status_var.set, "Ready")

        except Exception as exc:
            import traceback
            traceback.print_exc()
            ui(self._hide_progress)
            ui(self._status_var.set, f"Error: {str(exc)[:120]}")
            ui(self._header_var.set, f"Error loading job: {str(exc)[:100]}")

    # ------------------------------------------------------------------
    # Populate tabs (main thread)
    # ------------------------------------------------------------------

    def _populate_materials(self, materials) -> None:
        for row in self._mtl_tree.get_children():
            self._mtl_tree.delete(row)
        self._mtl_iid_map.clear()

        if not materials:
            self._mtl_tree.insert("", tk.END, text="(no materials found)")
            self._notebook.tab(0, text="Materials (0)")
            return

        # Group by assembly_seq so multi-assembly jobs display cleanly
        by_asm: dict = {}
        for m in materials:
            by_asm.setdefault(m.assembly_seq, []).append(m)

        for asm_seq in sorted(by_asm):
            mtls = by_asm[asm_seq]
            parent_iid = ""
            if len(by_asm) > 1:
                parent_iid = self._mtl_tree.insert(
                    "", tk.END, text=f"Assembly {asm_seq}", open=True,
                )
            for m in mtls:
                tag = "buy" if m.buy_it else "make"
                iid = self._mtl_tree.insert(
                    parent_iid, tk.END,
                    text=m.part_num,
                    values=(
                        m.assembly_seq,
                        m.mtl_seq,
                        m.description or "",
                        _fmt_qty(m.required_qty),
                        _fmt_qty(m.issued_qty),
                        m.uom or "",
                        "B" if m.buy_it else "M",
                        "\u2026",  # po_job    — filled after POs load
                        "\u2026",  # po_due    — filled after POs load
                        "\u2026",  # po_vendor — filled after POs load
                    ),
                    tags=(tag,),
                )
                self._mtl_iid_map[iid] = m.part_num

        self._notebook.tab(0, text=f"Materials ({len(materials)})")

    def _hide_progress(self) -> None:
        self._progbar.stop()
        self._prog_frame.pack_forget()

    def _update_mtl_pos(self, po_lines) -> None:
        """Fill the Job PO and PO Due columns in the Materials tab (main thread)."""
        by_part: dict[str, list] = {}
        for p in po_lines:
            if p.part_num:
                by_part.setdefault(p.part_num, []).append(p)

        idx_po     = self._MTL_COL_IDX["po_job"]
        idx_due    = self._MTL_COL_IDX["po_due"]
        idx_vendor = self._MTL_COL_IDX["po_vendor"]
        for iid, part_num in self._mtl_iid_map.items():
            try:
                pos  = by_part.get(part_num, [])
                vals = list(self._mtl_tree.item(iid, "values"))
                vals[idx_po]     = self._summarize_job_pos(pos)
                vals[idx_due]    = self._earliest_open_due(pos)
                vals[idx_vendor] = self._vendor_for_pos(pos)
                self._mtl_tree.item(iid, values=vals)
            except tk.TclError:
                pass  # window closed while running

    @staticmethod
    def _earliest_open_due(pos: list) -> str:
        """Return the earliest due date among open PO lines, or '—'."""
        dates = [p.due_date for p in pos if p.status == "open" and p.due_date]
        if not dates:
            return "\u2014"
        return _fmt_date(min(dates))

    @staticmethod
    def _vendor_for_pos(pos: list) -> str:
        """Return the vendor name for open PO lines, or '—'."""
        vendors = [p.vendor_name for p in pos if p.status == "open" and p.vendor_name]
        if not vendors:
            return "\u2014"
        unique = list(dict.fromkeys(vendors))   # deduplicate, preserve order
        if len(unique) == 1:
            return unique[0]
        return f"{unique[0]} (+{len(unique) - 1})"

    @staticmethod
    def _summarize_job_pos(pos: list) -> str:
        """Return a compact PO summary string for one material's PO lines."""
        if not pos:
            return "\u2014"  # —
        open_pos = [p for p in pos if p.status == "open"]
        if not open_pos:
            return "closed"
        if len(open_pos) == 1:
            p = open_pos[0]
            return f"PO {p.po_num}  ({_fmt_qty(p.order_qty)})"
        po_nums = sorted({p.po_num for p in open_pos})
        return f"{len(open_pos)} open  [{'  '.join(str(n) for n in po_nums)}]"

    def _populate_pos(self, po_lines) -> None:
        for row in self._po_tree.get_children():
            self._po_tree.delete(row)

        if not po_lines:
            self._notebook.tab(1, text="Purchase Orders (0)")
            return

        order = {"open": 0, "closed": 1, "void": 2}
        for p in sorted(po_lines, key=lambda p: (order.get(p.status, 3), p.po_num)):
            tag = p.status if p.status in ("open", "closed", "void") else ""
            self._po_tree.insert(
                "", tk.END,
                values=(
                    (p.status or "").capitalize(),
                    p.po_num,
                    p.po_line,
                    p.part_num or "",
                    _fmt_qty(p.order_qty),
                    _fmt_qty(p.received_qty) if p.received_qty is not None else "",
                    _fmt_date(p.due_date),
                    (p.vendor_name or "")[:50],
                ),
                tags=(tag,) if tag else (),
            )

        n       = len(po_lines)
        open_n  = sum(1 for p in po_lines if p.status == "open")
        label   = f"Purchase Orders ({n})"
        if open_n:
            label += f"  [{open_n} open]"
        self._notebook.tab(1, text=label)

    def _open_draw_pack(self) -> None:
        """Collect buy-it materials from the job and open DrawPackWindow."""
        from tkinter import messagebox

        idx_buy    = self._MTL_COL_IDX["buy"]
        idx_vendor = self._MTL_COL_IDX["po_vendor"]
        parts: list = []

        for iid, part_num in self._mtl_iid_map.items():
            try:
                vals = self._mtl_tree.item(iid, "values")
            except tk.TclError:
                continue
            if not vals:
                continue
            if vals[idx_buy] == "B":
                vendor = vals[idx_vendor]
                if vendor in ("\u2026", "\u2014"):
                    vendor = ""
                parts.append((part_num, vendor))

        if not parts:
            messagebox.showinfo(
                "No Parts",
                "No purchased (buy-it) materials found for this job.\n"
                "Load a job first and wait for PO data to finish.",
                parent=self._win,
            )
            return

        DrawPackWindow(self._win, parts, self._parent.db, self._parent.box)

    # ------------------------------------------------------------------
    # Double-click material row → open drawing
    # ------------------------------------------------------------------

    def _on_mtl_double_click(self, event: tk.Event) -> None:
        iid = self._mtl_tree.identify_row(event.y)
        if not iid:
            return
        part_num = self._mtl_tree.item(iid, "text")
        if part_num and not part_num.startswith("(") and not part_num.startswith("Assembly"):
            DrawingViewToplevel(self._win, part_num, self._parent.db, self._parent.box)


# ---------------------------------------------------------------------------
# Revision History window
# ---------------------------------------------------------------------------

def _rev_sort_key(rev):
    """Sort revision codes: X-revisions first (X1 < X2 …), then letter revisions (A < B …)."""
    if rev is None:
        return (0, 0, "")
    if len(rev) == 1 and rev.isalpha():
        return (2, 0, rev)
    if rev.upper().startswith("X") and rev[1:].isdigit():
        return (1, int(rev[1:]), "")
    return (2, 0, rev)


class RevisionHistoryWindow:
    """
    Revision History — joined table of all revisions from Box DB and Epicor.

    Columns:
      Revision | Epicor Approved | Epicor Eff. Date | PDF | STEP | SolidWorks | Latest | Released

    Color coding:
      - Epicor approved   → green background
      - Epicor unapproved → amber background
      - Only in one source (DB or Epicor, not both) → italic tag

    This lets you spot revisions that exist in Epicor but have no drawing file,
    or drawings that were never registered in Epicor, and see at a glance whether
    the latest rev has been formally approved/released.
    """

    _COLUMNS = [
        # (col_id,       header,           width, anchor,   stretch)
        ("rev",         "Revision",          80,  "center", False),
        ("epi_approved","Epicor Approved",   120,  "center", False),
        ("epi_date",    "Epicor Eff. Date",  120,  "center", False),
        ("pdf",         "PDF",                50,  "center", False),
        ("step",        "STEP",               50,  "center", False),
        ("solidworks",  "SolidWorks",         90,  "center", False),
        ("db_latest",   "Latest",             60,  "center", False),
        ("db_released", "Released",           70,  "center", False),
    ]

    def __init__(self, parent: "DrawingViewer") -> None:
        self._parent  = parent
        self._epicor  = None

        self._win = tk.Toplevel(parent.root)
        self._win.title("Revision History")
        self._win.geometry("760x400")
        self._win.minsize(500, 250)

        self._status_var = tk.StringVar(value="Enter a part number and press Enter.")
        self._build_ui()
        self._win.lift()
        self._win.focus_set()

    @property
    def _client(self):
        if self._epicor is None:
            self._epicor = _make_epicor_client()
        return self._epicor

    def _ui(self, fn, *args) -> None:
        if self._win.winfo_exists():
            self._win.after(0, fn, *args)

    def _build_ui(self) -> None:
        # Toolbar
        top = tk.Frame(self._win, padx=6, pady=5)
        top.pack(fill=tk.X, side=tk.TOP)
        tk.Label(top, text="Part Number:", font=("", 11)).pack(side=tk.LEFT)
        self._part_entry = tk.Entry(top, width=12, font=("Courier", 13, "bold"))
        self._part_entry.pack(side=tk.LEFT, padx=(5, 10))
        self._part_entry.bind("<Return>", lambda _e: self._start_lookup())
        self._part_entry.focus_set()
        tk.Button(top, text="Look Up", command=self._start_lookup).pack(side=tk.LEFT)

        # Status bar
        tk.Label(
            self._win, textvariable=self._status_var,
            anchor="w", relief=tk.SUNKEN, padx=5, font=("", 9),
        ).pack(fill=tk.X, side=tk.BOTTOM)

        # Treeview
        frame = tk.Frame(self._win)
        frame.pack(fill=tk.BOTH, expand=True)

        self._tree = ttk.Treeview(
            frame,
            columns=[c[0] for c in self._COLUMNS],
            show="headings",
            selectmode="browse",
        )
        for col_id, heading, width, anchor, stretch in self._COLUMNS:
            self._tree.heading(col_id, text=heading, anchor=anchor)
            self._tree.column(col_id, width=width, minwidth=30, anchor=anchor, stretch=stretch)

        # Row tags
        self._tree.tag_configure("approved",   background="#d4edda")  # green  — approved in Epicor
        self._tree.tag_configure("unapproved", background="#fff3cd")  # amber  — in Epicor, not approved
        self._tree.tag_configure("no_epicor",  background="#f8f9fa")  # grey   — not in Epicor at all

        vsb = ttk.Scrollbar(frame, orient="vertical",   command=self._tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT,  fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)

    def seed_part(self, part_num: str) -> None:
        self._part_entry.delete(0, tk.END)
        self._part_entry.insert(0, part_num)
        self._start_lookup()

    def _start_lookup(self) -> None:
        part_num = self._part_entry.get().strip()
        if not part_num:
            return
        for row in self._tree.get_children():
            self._tree.delete(row)
        self._win.title(f"Revision History \u2014 {part_num}")
        self._status_var.set(f"Loading revisions for {part_num}\u2026")
        threading.Thread(target=self._do_load, args=(part_num,), daemon=True).start()

    def _do_load(self, part_num: str) -> None:
        try:
            # Fetch DB drawings
            db_drawings = self._parent.db.get_all_drawings_for_part(part_num)
            db_by_rev = {(d.revision or "").upper(): d for d in db_drawings}

            # Fetch Epicor revisions
            epi_revs = self._client.get_part_revisions(part_num)
            epi_by_rev = {(r.get("RevisionNum") or "").strip().upper(): r for r in epi_revs}

            # Union of all known revision codes
            all_revs = sorted(
                db_by_rev.keys() | epi_by_rev.keys(),
                key=_rev_sort_key,
            )

            rows = []
            for rev in all_revs:
                db  = db_by_rev.get(rev)
                epi = epi_by_rev.get(rev)

                # Epicor columns
                if epi is not None:
                    approved_bool = epi.get("Approved", False)
                    epi_approved  = "Yes" if approved_bool else "No"
                    epi_date      = _fmt_date(epi.get("EffectiveDate") or epi.get("ApprovedDate") or "")
                    tag           = "approved" if approved_bool else "unapproved"
                else:
                    epi_approved = "\u2014"
                    epi_date     = "\u2014"
                    tag          = "no_epicor"

                # DB file columns
                pdf        = "\u2713" if db and db.pdf_file_id  else "\u2014"
                step       = "\u2713" if db and db.step_file_id else "\u2014"
                solidworks = "\u2713" if db and (db.slddrw_file_id or db.sldprt_file_id or db.sldasm_file_id) else "\u2014"
                db_latest  = "\u2605" if db and db.is_latest   else "\u2014"   # ★
                db_released = "\u2713" if db and db.is_released else "\u2014"

                rows.append((rev or "(none)", epi_approved, epi_date, pdf, step, solidworks, db_latest, db_released, tag))

            self._ui(self._populate, part_num, rows)

        except Exception as exc:
            import traceback
            traceback.print_exc()
            self._ui(self._status_var.set, f"Error: {str(exc)[:120]}")

    def _populate(self, part_num: str, rows: list) -> None:
        for row in self._tree.get_children():
            self._tree.delete(row)
        for *values, tag in rows:
            self._tree.insert("", tk.END, values=values, tags=(tag,))
        n_epi = sum(1 for *v, t in rows if t != "no_epicor")
        n_pdf  = sum(1 for *v, t in rows if v[3] == "\u2713")
        self._status_var.set(
            f"{part_num}  |  {len(rows)} revision(s)  |  "
            f"{n_epi} in Epicor  |  {n_pdf} with PDF drawing"
        )


# ---------------------------------------------------------------------------
# Drawing Package window
# ---------------------------------------------------------------------------

class DrawPackWindow:
    """
    Drawing Package — downloads PDF and STEP files for a list of purchased parts,
    grouped into per-supplier subfolders within a chosen output folder.

    parts: list of (part_num, supplier_name) — supplier may be empty / "—" / "…"
    """

    _BLANK = {"", "\u2014", "\u2026"}   # values treated as "no supplier"

    def __init__(self, parent: tk.Misc, parts: list, db, box) -> None:
        # Deduplicate by part_num (keep first supplier seen)
        seen: dict = {}
        for part_num, supplier in parts:
            if part_num and part_num not in seen:
                sup = supplier.strip() if supplier and supplier.strip() not in self._BLANK else ""
                seen[part_num] = sup
        self._parts = list(seen.items())   # [(part_num, supplier), ...]
        self._db    = db
        self._box   = box

        self._win = tk.Toplevel(parent)
        self._win.title("Drawing Package")
        self._win.geometry("740x580")
        self._win.minsize(520, 420)

        self._status_var = tk.StringVar(value="Choose an output folder to begin.")
        self._build_ui()
        self._win.lift()
        self._win.focus_set()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        # Group by supplier for the preview
        by_sup: dict = {}
        for pn, sup in self._parts:
            by_sup.setdefault(sup or "No Supplier", []).append(pn)

        n_parts = len(self._parts)
        n_sups  = len(by_sup)

        # ── Info bar ───────────────────────────────────────────────────
        tk.Label(
            self._win,
            text=f"{n_parts} part(s) across {n_sups} supplier folder(s).",
            anchor="w", bg="#d6e8f7", padx=8, pady=4, font=("", 10),
        ).pack(fill=tk.X, side=tk.TOP)

        # ── Preview tree ───────────────────────────────────────────────
        preview_frame = tk.Frame(self._win, padx=6, pady=4)
        preview_frame.pack(fill=tk.BOTH, expand=True)

        self._tv = ttk.Treeview(
            preview_frame,
            columns=("parts",),
            show="tree headings",
            selectmode="none",
        )
        self._tv.heading("#0",     text="Supplier / Part Number", anchor="w")
        self._tv.heading("parts",  text="# Parts",                anchor="center")
        self._tv.column("#0",    width=380, anchor="w")
        self._tv.column("parts", width=70,  anchor="center", stretch=False)

        for sup_name in sorted(by_sup):
            pns = sorted(by_sup[sup_name])
            sup_iid = self._tv.insert(
                "", tk.END, text=f"\U0001f4c1  {sup_name}",
                values=(len(pns),), open=False,
            )
            for pn in pns:
                self._tv.insert(sup_iid, tk.END, text=f"    {pn}", values=("",))

        vsb = ttk.Scrollbar(preview_frame, orient="vertical", command=self._tv.yview)
        self._tv.configure(yscrollcommand=vsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        self._tv.pack(fill=tk.BOTH, expand=True)

        # ── Progress ───────────────────────────────────────────────────
        prog_frame = tk.Frame(self._win, padx=6, pady=2)
        prog_frame.pack(fill=tk.X)
        self._prog_label = tk.Label(prog_frame, text="", anchor="w", font=("", 9))
        self._prog_label.pack(fill=tk.X)
        self._progbar = ttk.Progressbar(
            prog_frame, orient="horizontal", mode="determinate",
            maximum=len(self._parts), value=0,
        )
        self._progbar.pack(fill=tk.X)

        # ── Report ─────────────────────────────────────────────────────
        rep_frame = tk.Frame(self._win, padx=6, pady=4)
        rep_frame.pack(fill=tk.X)

        rep_top = tk.Frame(rep_frame)
        rep_top.pack(fill=tk.X)
        tk.Label(rep_top, text="Report:", font=("", 9, "bold"), anchor="w").pack(side=tk.LEFT)
        tk.Button(rep_top, text="Copy", command=self._copy_report, font=("", 8)).pack(side=tk.RIGHT)

        self._report = ScrolledText(
            rep_frame, height=7, font=("Courier", 8),
            state="disabled", wrap="word",
            bg="#f8f8f8",
        )
        self._report.pack(fill=tk.X)

        # ── Bottom buttons ─────────────────────────────────────────────
        bottom = tk.Frame(self._win, padx=6, pady=6)
        bottom.pack(fill=tk.X, side=tk.BOTTOM)

        self._start_btn = tk.Button(
            bottom, text="Choose Folder & Start",
            command=self._start, font=("", 10),
        )
        self._start_btn.pack(side=tk.LEFT)
        tk.Button(bottom, text="Close", command=self._win.destroy).pack(side=tk.RIGHT)
        tk.Label(
            bottom, textvariable=self._status_var, anchor="w", font=("", 9),
        ).pack(side=tk.LEFT, padx=(10, 0))

    # ------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------

    def _start(self) -> None:
        from tkinter import filedialog
        folder = filedialog.askdirectory(
            parent=self._win, title="Choose output folder for drawing package",
        )
        if not folder:
            return
        self._start_btn.config(state="disabled")
        self._status_var.set("Downloading\u2026")
        threading.Thread(target=self._run, args=(folder,), daemon=True).start()

    # ------------------------------------------------------------------
    # Background worker
    # ------------------------------------------------------------------

    def _run(self, folder: str) -> None:
        import os

        missing_pdf:  list = []
        missing_step: list = []
        not_in_db:    list = []
        done = 0

        for part_num, supplier in self._parts:
            self._ui(self._prog_label.config, text=f"Downloading {part_num}\u2026")

            # Drawing DB lookup
            try:
                drawing = self._db.get_latest_drawing(part_num)
            except Exception as exc:
                print(f"[DrawPack] DB error for {part_num}: {exc}")
                drawing = None

            if drawing is None:
                not_in_db.append(part_num)
                done += 1
                self._ui(self._progbar.config, value=done)
                continue

            # Destination subfolder
            sup_folder = _sanitize_folder_name(supplier) if supplier else "No Supplier"
            dest_dir   = os.path.join(folder, sup_folder)
            os.makedirs(dest_dir, exist_ok=True)

            rev  = drawing.revision or ""
            stem = f"{part_num}_Rev{rev}" if rev else part_num

            # PDF
            if drawing.pdf_file_id:
                try:
                    stream = self._box.downloads.download_file(drawing.pdf_file_id)
                    with open(os.path.join(dest_dir, f"{stem}.pdf"), "wb") as f:
                        f.write(stream.read())
                except Exception as exc:
                    print(f"[DrawPack] PDF download failed for {part_num}: {exc}")
                    missing_pdf.append(part_num)
            else:
                missing_pdf.append(part_num)

            # STEP
            if drawing.step_file_id:
                try:
                    stream = self._box.downloads.download_file(drawing.step_file_id)
                    with open(os.path.join(dest_dir, f"{stem}.step"), "wb") as f:
                        f.write(stream.read())
                except Exception as exc:
                    print(f"[DrawPack] STEP download failed for {part_num}: {exc}")
                    missing_step.append(part_num)
            else:
                missing_step.append(part_num)

            done += 1
            self._ui(self._progbar.config, value=done)

        self._ui(self._finish, folder, missing_pdf, missing_step, not_in_db)

    # ------------------------------------------------------------------
    # Finish (main thread)
    # ------------------------------------------------------------------

    def _finish(
        self, folder: str,
        missing_pdf: list, missing_step: list, not_in_db: list,
    ) -> None:
        self._prog_label.config(text="Complete.")
        self._status_var.set(f"Done \u2014 {folder}")

        lines = []
        if not_in_db:
            lines.append(f"NOT IN DRAWING DATABASE ({len(not_in_db)}):")
            lines.extend(f"  {p}" for p in sorted(not_in_db))
            lines.append("")
        if missing_pdf:
            lines.append(f"NO PDF AVAILABLE ({len(missing_pdf)}):")
            lines.extend(f"  {p}" for p in sorted(missing_pdf))
            lines.append("")
        if missing_step:
            lines.append(f"NO STEP FILE AVAILABLE ({len(missing_step)}):")
            lines.extend(f"  {p}" for p in sorted(missing_step))
            lines.append("")

        report = "\n".join(lines) if lines else "All PDF and STEP files downloaded successfully."

        self._report.config(state="normal")
        self._report.delete("1.0", "end")
        self._report.insert("1.0", report)
        self._report.config(state="disabled")

    def _copy_report(self) -> None:
        text = self._report.get("1.0", "end")
        self._win.clipboard_clear()
        self._win.clipboard_append(text)

    def _ui(self, fn, *args, **kwargs) -> None:
        if self._win.winfo_exists():
            self._win.after(0, lambda: fn(*args, **kwargs))


# ---------------------------------------------------------------------------
# Order Info window
# ---------------------------------------------------------------------------

class OrderInfoWindow:
    """
    Order Info — all PO lines for a given part number.

    Columns: Status | PO # | Line | Vendor | Ordered | Received | Order Date | Due Date | Job #

    Color-coded by status:
      green  = open
      grey   = closed
      red    = void

    Pre-seeded from the main window's current part.  Export to CSV supported.
    """

    _COLUMNS = [
        # (col_id,    header,        width,  anchor,    stretch)
        ("status",  "Status",         65,   "center",  False),
        ("po",      "PO #",           70,   "e",       False),
        ("line",    "Line",           45,   "e",       False),
        ("vendor",  "Vendor",        210,   "w",       True),
        ("qty",     "Ordered",        72,   "e",       False),
        ("rcvd",    "Received",       72,   "e",       False),
        ("order_dt","Order Date",     90,   "w",       False),
        ("due_dt",  "Due Date",       90,   "w",       False),
        ("job",     "Job #",         110,   "w",       False),
    ]

    def __init__(self, parent: "DrawingViewer") -> None:
        self._parent = parent
        self._epicor = None

        self._win = tk.Toplevel(parent.root)
        self._win.title("Order Info")
        self._win.geometry("1050x500")
        self._win.minsize(700, 300)

        self._status_var = tk.StringVar(value="Enter a part number and press Enter.")
        self._info_var   = tk.StringVar(value="")

        self._build_ui()
        self._win.lift()
        self._win.focus_set()

    @property
    def _client(self):
        if self._epicor is None:
            self._epicor = _make_epicor_client()
        return self._epicor

    def _ui(self, fn, *args) -> None:
        if self._win.winfo_exists():
            self._win.after(0, fn, *args)

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------

    def _build_ui(self) -> None:
        top = tk.Frame(self._win, padx=6, pady=5)
        top.pack(fill=tk.X, side=tk.TOP)

        tk.Label(top, text="Part Number:", font=("", 11)).pack(side=tk.LEFT)
        self._part_entry = tk.Entry(top, width=12, font=("Courier", 13, "bold"))
        self._part_entry.pack(side=tk.LEFT, padx=(5, 10))
        self._part_entry.bind("<Return>", lambda _e: self._start_lookup())
        self._part_entry.focus_set()

        tk.Button(top, text="Look Up", command=self._start_lookup).pack(side=tk.LEFT)
        tk.Button(top, text="Export CSV\u2026", command=self._export_csv).pack(side=tk.RIGHT)

        # Info bar
        tk.Label(
            self._win, textvariable=self._info_var,
            anchor="w", bg="#d6e8f7", padx=8, pady=2, font=("", 10),
        ).pack(fill=tk.X, side=tk.TOP)

        # Status bar
        tk.Label(
            self._win, textvariable=self._status_var,
            anchor="w", relief=tk.SUNKEN, padx=5, font=("", 9),
        ).pack(fill=tk.X, side=tk.BOTTOM)

        # Treeview
        frame = tk.Frame(self._win)
        frame.pack(fill=tk.BOTH, expand=True)

        self._tree = ttk.Treeview(
            frame,
            columns=[c[0] for c in self._COLUMNS],
            show="headings",
            selectmode="browse",
        )
        for col_id, heading, width, anchor, stretch in self._COLUMNS:
            self._tree.heading(col_id, text=heading, anchor=anchor)
            self._tree.column(col_id, width=width, minwidth=28, anchor=anchor, stretch=stretch)

        self._tree.tag_configure("open",   background="#d4edda")  # green
        self._tree.tag_configure("closed", background="#f8f9fa")  # light grey
        self._tree.tag_configure("void",   background="#f4b8b8")  # red

        vsb = ttk.Scrollbar(frame, orient="vertical",   command=self._tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=self._tree.xview)
        self._tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT,  fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        self._tree.pack(fill=tk.BOTH, expand=True)

    # ------------------------------------------------------------------
    # Seed / lookup
    # ------------------------------------------------------------------

    def seed_part(self, part_num: str) -> None:
        self._part_entry.delete(0, tk.END)
        self._part_entry.insert(0, part_num)
        self._start_lookup()

    def _start_lookup(self) -> None:
        part_num = self._part_entry.get().strip().upper()
        if not part_num:
            return
        for row in self._tree.get_children():
            self._tree.delete(row)
        self._info_var.set("")
        self._status_var.set(f"Loading orders for {part_num}\u2026")
        self._win.title(f"Order Info \u2014 {part_num}")
        threading.Thread(target=self._do_load, args=(part_num,), daemon=True).start()

    def _do_load(self, part_num: str) -> None:
        try:
            po_lines = self._client.get_po_lines_by_partnum(part_num)
            self._ui(self._populate, part_num, po_lines)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            self._ui(self._status_var.set, f"Error: {str(exc)[:120]}")

    # ------------------------------------------------------------------
    # Populate (main thread)
    # ------------------------------------------------------------------

    def _populate(self, part_num: str, po_lines: list) -> None:
        for row in self._tree.get_children():
            self._tree.delete(row)

        if not po_lines:
            self._info_var.set(f"{part_num} \u2014 no purchase orders found.")
            self._status_var.set("Ready")
            return

        order = {"open": 0, "closed": 1, "void": 2}
        for p in sorted(po_lines, key=lambda p: (order.get(p.status, 3), p.po_num, p.po_line)):
            tag = p.status if p.status in ("open", "closed", "void") else ""
            self._tree.insert(
                "", tk.END,
                values=(
                    (p.status or "").capitalize(),
                    p.po_num,
                    p.po_line,
                    (p.vendor_name or "")[:60],
                    _fmt_qty(p.order_qty),
                    _fmt_qty(p.received_qty) if p.received_qty is not None else "\u2014",
                    _fmt_date(p.order_date),
                    _fmt_date(p.due_date),
                    p.job_num or "\u2014",
                ),
                tags=(tag,) if tag else (),
            )

        n        = len(po_lines)
        open_n   = sum(1 for p in po_lines if p.status == "open")
        closed_n = sum(1 for p in po_lines if p.status == "closed")
        void_n   = sum(1 for p in po_lines if p.status == "void")

        parts = [part_num, f"{n} PO line{'s' if n != 1 else ''}"]
        if open_n:   parts.append(f"{open_n} open")
        if closed_n: parts.append(f"{closed_n} closed")
        if void_n:   parts.append(f"{void_n} void")
        self._info_var.set("  |  ".join(parts))
        self._status_var.set("Ready")

    # ------------------------------------------------------------------
    # CSV export
    # ------------------------------------------------------------------

    def _export_csv(self) -> None:
        import csv
        from tkinter import filedialog

        if not self._tree.get_children():
            return

        part_num = self._part_entry.get().strip().upper() or "orders"
        path = filedialog.asksaveasfilename(
            parent=self._win,
            title="Export Order Info to CSV",
            initialfile=f"{part_num}_orders.csv",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if not path:
            return

        col_headers = [c[1] for c in self._COLUMNS]
        rows = [list(self._tree.item(iid, "values")) for iid in self._tree.get_children()]
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(col_headers)
                writer.writerows(rows)
            self._status_var.set(f"Exported {len(rows)} rows to {path}")
        except Exception as exc:
            self._status_var.set(f"Export failed: {exc}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    _install_log_capture()   # must be first — captures all subsequent print/traceback output
    root = tk.Tk()
    DrawingViewer(root)
    root.mainloop()


if __name__ == "__main__":
    main()
