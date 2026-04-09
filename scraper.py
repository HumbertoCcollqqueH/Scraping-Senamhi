import csv
import re
import time
import traceback
import unicodedata
from pathlib import Path
from threading import RLock
from urllib.parse import urljoin, urlparse, parse_qs

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchFrameException

try:
    from webdriver_manager.chrome import ChromeDriverManager
    USE_MANAGER = True
except Exception:
    USE_MANAGER = False

BASE_URL = "https://www.senamhi.gob.pe"
MAIN_URL = BASE_URL + "/main.php?dp={region}&p=estaciones"
MAP_URL_PART = "mapa-estaciones-2"

REGION_NAME = {
    "amazonas": "AMAZONAS",
    "ancash": "ANCASH",
    "apurimac": "APURIMAC",
    "arequipa": "AREQUIPA",
    "ayacucho": "AYACUCHO",
    "cajamarca": "CAJAMARCA",
    "cusco": "CUSCO",
    "huancavelica": "HUANCAVELICA",
    "huanuco": "HUANUCO",
    "ica": "ICA",
    "junin": "JUNIN",
    "la-libertad": "LA LIBERTAD",
    "lambayeque": "LAMBAYEQUE",
    "lima": "LIMA",
    "loreto": "LORETO",
    "madre-de-dios": "MADRE DE DIOS",
    "moquegua": "MOQUEGUA",
    "pasco": "PASCO",
    "piura": "PIURA",
    "puno": "PUNO",
    "san-martin": "SAN MARTIN",
    "tacna": "TACNA",
    "tumbes": "TUMBES",
    "ucayali": "UCAYALI",
}


def norm(text: str) -> str:
    t = unicodedata.normalize("NFD", text or "")
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    t = re.sub(r"\s+", " ", t).strip().lower()
    return t


def safe_name(text: str, maxlen: int = 140) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    t = re.sub(r'[<>:"/\\|?*\n\r\t]', "_", t).strip(" ._")
    return t[:maxlen] or "sin_nombre"


def infer_type_from_url(station_url: str) -> str:
    try:
        q = parse_qs(urlparse(station_url).query)
    except Exception:
        q = {}
    tipo_esta = ((q.get("tipo_esta") or [""])[0] or "").upper()
    cate = ((q.get("cate") or [""])[0] or "").upper()
    estado = ((q.get("estado") or [""])[0] or "").upper()
    if tipo_esta == "H" and "AUTOMATICA" in estado:
        return "Automática - Hidrológica"
    if tipo_esta == "H":
        return "Convencional - Hidrológica"
    if tipo_esta == "M" and cate in {"EMA", "EAMA"}:
        return "Automática - Meteorológica"
    if tipo_esta == "M":
        return "Convencional - Meteorológica"
    return "Sin clasificar"


class SenamhiScraper:
    """
    Proyecto Scraping V12

    Flujo manual:
      1. Inventaria todas las estaciones de una región.
      2. El usuario selecciona una estación desde la interfaz.
      3. El scraper abre la estación elegida en el navegador.
      4. El usuario cambia manualmente a Tabla, resuelve CAPTCHA y verifica los datos.
      5. Cuando el usuario confirma, recién se descarga todo el histórico de esa estación.
    """

    def __init__(self, region, output_dir, headless, state, log_fn):
        self.region = region
        self.output_dir = Path(output_dir)
        self.headless = headless
        self.state = state
        self.log = log_fn
        self.driver = None
        self.wait = None
        self._stop = False
        self._closed = False
        self._lock = RLock()

        self.region_dir = self.output_dir / region
        self.debug_dir = self.output_dir / "_debug"
        self.profile_dir = self.output_dir / "_profile"

        self.station_urls = []
        self.station_entries = []
        self.station_seen_urls = set()
        self.station_seen_codes = set()
        self.selected_station_url = ""
        self.selected_station_entry = None
        self._debug_n = 0
        self._last_table_signature = None
        self._station_fast_mode = False

    # ───────────────────────── control ─────────────────────────
    def stop(self):
        self._stop = True

    def close(self):
        self._stop = True
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
        self.driver = None
        self._closed = True

    def _set(self, **kwargs):
        for k, v in kwargs.items():
            if k in self.state:
                self.state[k] = v

    def _pause(self, msg: str) -> bool:
        self.state["awaiting_human"] = True
        self._set(current_task=msg)
        self.log(msg, "warning")
        while not self._stop and self.state.get("awaiting_human"):
            time.sleep(0.35)
        return not self._stop

    # ───────────────────────── driver ─────────────────────────
    def _init_driver(self):
        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1600,1100")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--lang=es-PE")
        opts.page_load_strategy = "eager"
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        self.profile_dir.mkdir(parents=True, exist_ok=True)
        opts.add_argument(f"--user-data-dir={self.profile_dir.resolve()}")
        opts.add_experimental_option("prefs", {
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.cookies": 1,
            "profile.block_third_party_cookies": False,
        })

        if USE_MANAGER:
            svc = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=svc, options=opts)
        else:
            self.driver = webdriver.Chrome(options=opts)

        self.driver.set_page_load_timeout(60)
        self.driver.set_script_timeout(20)
        self.driver.execute_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        self.wait = WebDriverWait(self.driver, 25)
        self.log("Navegador Chrome iniciado correctamente.", "success")

    # ───────────────────────── debug ─────────────────────────
    def _dump(self, prefix="dbg"):
        self._debug_n += 1
        tag = f"{prefix}_{self._debug_n:03d}"
        try:
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            (self.debug_dir / f"{tag}.html").write_text(
                self.driver.page_source,
                encoding="utf-8",
                errors="ignore",
            )
            self.driver.save_screenshot(str(self.debug_dir / f"{tag}.png"))
        except Exception:
            pass

    # ───────────────────────── captcha y disponibilidad ─────────────────────────
    def _has_verification(self) -> bool:
        patterns = [
            "//iframe[contains(@src,'turnstile') or contains(@src,'recaptcha')][not(contains(@style,'display: none'))]",
            "//*[contains(@class,'cf-turnstile')][not(contains(@style,'display: none'))]",
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ', 'abcdefghijklmnopqrstuvwxyzáéíóú'),'no eres un robot')]",
            "//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZÁÉÍÓÚ', 'abcdefghijklmnopqrstuvwxyzáéíóú'),'verifica que no eres un robot')]",
        ]
        for xp in patterns:
            try:
                if any(el.is_displayed() for el in self.driver.find_elements(By.XPATH, xp)):
                    return True
            except Exception:
                pass
        return False

    def _verification_response_present(self) -> bool:
        selectors = [
            "input[name='cf-turnstile-response']",
            "textarea[name='cf-turnstile-response']",
            "textarea[name='g-recaptcha-response']",
            "input[name='g-recaptcha-response']",
        ]
        for css in selectors:
            try:
                for el in self.driver.find_elements(By.CSS_SELECTOR, css):
                    value = (el.get_attribute("value") or "").strip()
                    if value:
                        return True
            except Exception:
                pass
        return False

    def _form_or_container_ready(self) -> bool:
        try:
            if self.driver.find_elements(By.ID, "frmData"):
                return True
            if self.driver.find_elements(By.CSS_SELECTOR, "iframe#contenedor, iframe[name='contenedor']"):
                return True
        except Exception:
            pass
        return False

    def _wait_manual_ready(self) -> bool:
        deadline = time.time() + 600
        while time.time() < deadline:
            if self._stop:
                return False
            if self._form_or_container_ready():
                return True
            if self._verification_response_present():
                return True
            time.sleep(0.5)
        return False

    # ───────────────────────── navegación base ─────────────────────────
    def _open_main(self):
        url = MAIN_URL.format(region=self.region)
        self._set(current_task=f"Cargando región {self.region}...")
        self.log(f"Cargando: {url}", "info")
        self.driver.get(url)
        time.sleep(3)

    def _into_map(self):
        self.driver.switch_to.default_content()
        iframe = self.wait.until(
            EC.presence_of_element_located((By.XPATH, f"//iframe[contains(@src,'{MAP_URL_PART}')]"))
        )
        self.driver.switch_to.frame(iframe)

    def _map_ready(self):
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".leaflet-container")))
        time.sleep(1.2)
        self._activate_all_filters()
        time.sleep(1.0)
        self._activate_all_filters()
        time.sleep(1.0)

    def _activate_all_filters(self):
        try:
            for cb in self.driver.find_elements(By.CSS_SELECTOR, "input[type='checkbox']"):
                if cb.is_displayed() and not cb.is_selected():
                    self.driver.execute_script("arguments[0].click();", cb)
                    time.sleep(0.15)
        except Exception:
            pass

    def _close_popup(self):
        for css in [".leaflet-popup-close-button", "button.close", ".close"]:
            try:
                for el in self.driver.find_elements(By.CSS_SELECTOR, css):
                    if el.is_displayed():
                        self.driver.execute_script("arguments[0].click();", el)
                        time.sleep(0.35)
                        return
            except Exception:
                pass

    # ───────────────────────── inventario del mapa ─────────────────────────
    def _visible_marker_count(self) -> int:
        js = """
        const els = Array.from(document.querySelectorAll('.leaflet-marker-pane img.leaflet-marker-icon, .leaflet-marker-pane div.leaflet-marker-icon'));
        return els.filter(el => {
            const cs = getComputedStyle(el);
            return cs.display !== 'none' && cs.visibility !== 'hidden' && parseFloat(cs.opacity || '1') > 0;
        }).length;
        """
        try:
            return int(self.driver.execute_script(js) or 0)
        except Exception:
            return 0

    def _wait_marker_count_stable(self, attempts: int = 8, delay: float = 1.0) -> int:
        last = None
        stable = 0
        best = 0
        for _ in range(attempts):
            if self._stop:
                break
            count = self._visible_marker_count()
            best = max(best, count)
            self.log(f"Marcadores visibles detectados: {count}", "info")
            if count == last and count > 0:
                stable += 1
                if stable >= 2:
                    return count
            else:
                stable = 0
            last = count
            time.sleep(delay)
        return best

    def _marker_descriptors(self):
        js = """
        const els = Array.from(document.querySelectorAll('.leaflet-marker-pane img.leaflet-marker-icon, .leaflet-marker-pane div.leaflet-marker-icon'));
        const vis = [];
        for (const el of els) {
            const cs = getComputedStyle(el);
            if (cs.display === 'none' || cs.visibility === 'hidden' || parseFloat(cs.opacity || '1') <= 0) continue;
            const r = el.getBoundingClientRect();
            vis.push({
                dom_index: vis.length,
                cx: Math.round(r.left + r.width/2),
                cy: Math.round(r.top + r.height/2),
                title: el.getAttribute('title') || '',
                src: el.getAttribute('src') || ''
            });
        }
        return vis;
        """
        try:
            return self.driver.execute_script(js) or []
        except Exception:
            return []

    def _click_marker_by_dom_index(self, dom_index: int) -> bool:
        js = """
        const idx = arguments[0];
        const els = Array.from(document.querySelectorAll('.leaflet-marker-pane img.leaflet-marker-icon, .leaflet-marker-pane div.leaflet-marker-icon'));
        const vis = els.filter(el => {
            const cs = getComputedStyle(el);
            return cs.display !== 'none' && cs.visibility !== 'hidden' && parseFloat(cs.opacity || '1') > 0;
        });
        const el = vis[idx];
        if (!el) return false;
        ['mouseover','mousedown','mouseup','click'].forEach(evt => {
            el.dispatchEvent(new MouseEvent(evt, {bubbles:true, cancelable:true, view:window}));
        });
        return true;
        """
        try:
            ok = self.driver.execute_script(js, dom_index)
        except Exception:
            return False
        if not ok:
            return False
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                iframe = self.driver.find_element(By.CSS_SELECTOR, ".leaflet-popup-content iframe, .leaflet-popup iframe")
                if iframe.is_displayed():
                    return True
            except Exception:
                pass
            time.sleep(0.25)
        return False

    def _get_popup_iframe_src(self) -> str:
        iframe = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".leaflet-popup-content iframe, .leaflet-popup iframe"))
        )
        src = (iframe.get_attribute("src") or "").strip()
        return urljoin(BASE_URL, src) if src else ""

    def _extract_code_from_station_url(self, station_url: str) -> str:
        try:
            q = parse_qs(urlparse(station_url).query)
            code = (q.get("cod") or [""])[0].strip()
            code_old = (q.get("cod_old") or [""])[0].strip()
            return code or code_old
        except Exception:
            return ""

    def _station_entry_from_url(self, station_url: str, idx: int) -> dict:
        code = self._extract_code_from_station_url(station_url)
        type_guess = infer_type_from_url(station_url)
        label = f"{idx}. {code or 'sin código'} · {type_guess}"
        return {
            "index": idx,
            "url": station_url,
            "code": code,
            "type_guess": type_guess,
            "name": code or f"estacion_{idx}",
            "label": label,
        }

    def _inventory_station_urls(self):
        self._into_map()
        self._map_ready()
        visible = self._wait_marker_count_stable()
        if not visible:
            raise RuntimeError("No se encontraron marcadores visibles en el mapa")

        markers = self._marker_descriptors()
        if not markers:
            raise RuntimeError("No se pudo describir los marcadores del mapa")

        self.log(f"Marcadores visibles estabilizados: {len(markers)}", "success")
        self.log("Inventariando estaciones por índice real del DOM del mapa...", "info")

        for idx in range(len(markers)):
            if self._stop:
                return
            self._set(current_task=f"Inventariando estación {idx+1}/{len(markers)}")
            try:
                self.driver.switch_to.default_content()
                self._into_map()
                self._close_popup()
                time.sleep(0.2)

                current = self._marker_descriptors()
                if idx >= len(current):
                    self.log(f"  Índice {idx} fuera de rango tras refrescar marcadores.", "warning")
                    continue

                if not self._click_marker_by_dom_index(idx):
                    self.log(f"  No se pudo abrir el marcador DOM {idx}.", "warning")
                    continue

                src = self._get_popup_iframe_src()
                if not src:
                    self.log(f"  Marcador DOM {idx} sin iframe src.", "warning")
                    continue

                if src in self.station_seen_urls:
                    self.log(f"  Duplicado omitido: {src}", "info")
                    continue

                self.station_seen_urls.add(src)
                self.station_urls.append(src)
                entry = self._station_entry_from_url(src, len(self.station_urls))
                self.station_entries.append(entry)
                self.log(f"  Nueva estación detectada ({len(self.station_urls)}/{len(markers)}): {src}", "info")

            except Exception as exc:
                self.log(f"  Error inventariando marcador {idx+1}: {exc}", "warning")
                self._dump(f"inventario_marker_{idx+1}")
            finally:
                try:
                    self.driver.switch_to.default_content()
                    self._into_map()
                    self._close_popup()
                except Exception:
                    pass
                time.sleep(0.25)

        self.state["stations"] = self.station_entries
        self.log(f"Estaciones únicas inventariadas: {len(self.station_entries)}", "success")
        if not self.station_entries:
            raise RuntimeError("No se pudo inventariar ninguna estación única")

    # ───────────────────────── página estación ─────────────────────────
    def _open_station(self, station_url: str):
        self.driver.switch_to.default_content()
        self.driver.get(station_url)
        time.sleep(2)

    def _click_tabla(self, force: bool = False) -> bool:
        try:
            active = self.driver.find_elements(By.CSS_SELECTOR, "#tabla-tab.active, a[href='#tabla'].active")
            if active and not force:
                return True
        except Exception:
            pass

        xpaths = [
            "//a[@id='tabla-tab']",
            "//a[normalize-space(.)='Tabla']",
            "//button[normalize-space(.)='Tabla']",
            "//a[contains(normalize-space(.),'Tabla')]",
            "//li//a[contains(@href,'tabla')]",
        ]

        for _ in range(3):
            for xp in xpaths:
                try:
                    for el in self.driver.find_elements(By.XPATH, xp):
                        if el.is_displayed():
                            self.driver.execute_script("arguments[0].click();", el)
                            time.sleep(0.35)
                            if self._form_or_container_ready():
                                return True
                            active = self.driver.find_elements(By.CSS_SELECTOR, "#tabla-tab.active, a[href='#tabla'].active")
                            if active:
                                return True
                except Exception:
                    pass
            time.sleep(0.3)
        return self._form_or_container_ready()

    def _wait_form_ready(self, log_tab_activation: bool = True):
        self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        deadline = time.time() + 35
        tabla_logged = False

        while time.time() < deadline:
            if self._stop:
                raise RuntimeError("Proceso detenido por el usuario")

            if self._click_tabla(force=True) and not tabla_logged:
                if log_tab_activation:
                    self.log("Pestaña Tabla activada o lista para usarse.", "info")
                tabla_logged = True

            if self._form_or_container_ready():
                return

            if self._has_verification() and not self._verification_response_present():
                self._handle_verification_during_auto()
                continue

            time.sleep(0.5 if self._station_fast_mode else 0.8)

        self._click_tabla(force=True)
        if self._form_or_container_ready():
            return

        if not self.driver.find_elements(By.ID, "frmData"):
            raise RuntimeError("No apareció el formulario frmData en la página de la estación")
        if not self.driver.find_elements(By.CSS_SELECTOR, "iframe#contenedor, iframe[name='contenedor']"):
            raise RuntimeError("No apareció el iframe contenedor en la página de la estación")

    def _handle_verification_during_auto(self) -> bool:
        if self._form_or_container_ready() or self._verification_response_present() or not self._has_verification():
            return True

        ok = self._pause(
            "⚠️  Verificación CAPTCHA/Turnstile detectada. Resuélvela manualmente en Chrome y luego pulsa 'Reanudar'."
        )
        if not ok:
            return False

        deadline = time.time() + 300
        while time.time() < deadline:
            if self._stop:
                return False
            self._click_tabla(force=True)
            if self._form_or_container_ready() or self._verification_response_present() or not self._has_verification():
                self.log("✅ Verificación resuelta. Continuando...", "success")
                time.sleep(0.8)
                return True
            time.sleep(1)

        raise RuntimeError("Tiempo agotado esperando la verificación CAPTCHA/Turnstile")

    def _station_meta(self) -> dict:
        try:
            body_text = self.driver.execute_script(
                "return (document.body && (document.body.innerText || document.body.textContent)) || '';"
            ) or ""
        except Exception:
            try:
                body_text = self.driver.find_element(By.TAG_NAME, "body").text or ""
            except Exception:
                body_text = ""

        body_text = body_text.replace("\xa0", " ")

        name_matches = re.findall(r"Estaci[oó]n\s*:\s*([^\n\r:]+)", body_text, re.I)
        type_matches = re.findall(r"Tipo\s*:\s*([^\n\r]+(?:\n[^\n\r]+)?)", body_text, re.I)
        code_matches = re.findall(r"C[oó]digo\s*:\s*([^\n\r]+)", body_text, re.I)
        dept_matches = re.findall(r"Departamento\s*:\s*([^\n\r]+)", body_text, re.I)

        name = ""
        for candidate in reversed(name_matches):
            candidate = re.sub(r"\s+", " ", candidate).strip(" .:-")
            if candidate and len(candidate) <= 120:
                name = candidate
                break

        raw_type = ""
        for candidate in reversed(type_matches):
            candidate = candidate.replace("\n", " ")
            candidate = re.sub(r"\s+", " ", candidate).strip(" .:-")
            if candidate:
                raw_type = candidate
                break

        code = ""
        for candidate in reversed(code_matches):
            candidate = re.sub(r"\s+", " ", candidate).strip(" .:-")
            if candidate:
                code = candidate
                break

        dept = ""
        for candidate in reversed(dept_matches):
            candidate = re.sub(r"\s+", " ", candidate).strip(" .:-")
            if candidate:
                dept = candidate
                break

        if not code:
            code = self._extract_code_from_station_url(self.driver.current_url)

        if not raw_type:
            raw_type = infer_type_from_url(self.driver.current_url)

        folder = safe_name(raw_type or "Sin clasificar")
        station_name = safe_name(name or code or "Estacion")
        return {
            "name": station_name,
            "type": raw_type,
            "code": code,
            "dept": dept,
            "folder": folder,
        }

    def _region_matches(self, dept: str) -> bool:
        expected = norm(REGION_NAME.get(self.region, self.region))
        got = norm(dept)
        return not dept or expected in got

    def _extract_periods_from_ir_section(self):
        try:
            html = self.driver.page_source or ""
        except Exception:
            html = ""
        html = html.replace("&nbsp;", " ")
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"\s+", " ", text)
        m = re.search(r"\bIr\s*:\s*(.+?)(?:\b(?:Exportar|Descargar|Gr[aá]fico|Tabla|Estaci[oó]n|Tipo|C[oó]digo)\b|$)", text, re.I)
        scope = m.group(1) if m else text
        found = re.findall(r"\b(20\d{2}-\d{2}|20\d{4}|20\d{2})\b", scope)
        opts = []
        seen = set()
        for item in found:
            if item in seen:
                continue
            seen.add(item)
            if re.fullmatch(r"20\d{2}-\d{2}", item):
                opts.append((item, item.replace("-", "")))
            elif re.fullmatch(r"20\d{4}", item):
                opts.append((f"{item[:4]}-{item[4:6]}", item))
            elif re.fullmatch(r"20\d{2}", item):
                opts.append((item, item))
        return sorted(dict(opts).items())

    def _period_options(self):
        for css in ["select#CBOFiltro", "select[name='CBOFiltro']", "select"]:
            try:
                selects = self.driver.find_elements(By.CSS_SELECTOR, css)
                for select_el in selects:
                    opts = []
                    for opt in Select(select_el).options:
                        txt = (opt.text or "").strip()
                        val = (opt.get_attribute("value") or "").strip()
                        if re.fullmatch(r"20\d{2}-\d{2}", txt):
                            opts.append((txt, val or txt.replace("-", "")))
                        elif re.fullmatch(r"20\d{4}", val):
                            opts.append((f"{val[:4]}-{val[4:6]}", val))
                        elif re.fullmatch(r"20\d{2}", txt):
                            opts.append((txt, val or txt))
                    if opts:
                        return sorted(dict(opts).items())
            except Exception:
                pass

        return self._extract_periods_from_ir_section()

    def _into_contenedor(self):
        iframe = self.driver.find_element(By.CSS_SELECTOR, "iframe#contenedor, iframe[name='contenedor']")
        self.driver.switch_to.frame(iframe)

    def _back_to_parent(self):
        try:
            self.driver.switch_to.parent_frame()
        except Exception:
            pass

    def _submit_period(self, period: str, value: str):
        selected = False
        for css in ["select#CBOFiltro", "select[name='CBOFiltro']", "select"]:
            try:
                selects = self.driver.find_elements(By.CSS_SELECTOR, css)
                for select_el in selects:
                    select = Select(select_el)
                    try:
                        if value and re.fullmatch(r"20\d{4,6}", value):
                            select.select_by_value(value)
                        else:
                            select.select_by_visible_text(period)
                        selected = True
                        break
                    except Exception:
                        for opt in select.options:
                            opt_val = (opt.get_attribute("value") or "").strip()
                            opt_txt = (opt.text or "").strip()
                            if opt_txt == period or opt_val == value or opt_val == period.replace("-", ""):
                                self.driver.execute_script("arguments[0].selected = true;", opt)
                                selected = True
                                break
                    if selected:
                        break
                if selected:
                    break
            except Exception:
                pass

        try:
            self.driver.execute_script("document.getElementById('frmData').submit();")
        except Exception:
            try:
                btn = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "#frmData button[type='submit'], #frmData input[type='submit']",
                )
                btn.click()
            except Exception:
                pass
        time.sleep(0.25)

    def _contenedor_snapshot(self):
        try:
            self._into_contenedor()
            snap = self.driver.execute_script(
                """
                const tables = Array.from(document.querySelectorAll('table'));
                let best = [];
                for (const tbl of tables) {
                    const rows = Array.from(tbl.querySelectorAll('tr')).map(tr =>
                        Array.from(tr.querySelectorAll('th,td')).map(td => (td.innerText || td.textContent || '').trim())
                    ).filter(row => row.some(v => v));
                    if (rows.length > best.length) best = rows;
                }
                const bodyText = (document.body?.innerText || '').trim();
                const sampleFirst = best.length ? JSON.stringify(best[0]).slice(0, 200) : '';
                const sampleLast = best.length ? JSON.stringify(best[best.length - 1]).slice(0, 200) : '';
                const signature = `${best.length}|${sampleFirst}|${sampleLast}|${bodyText.slice(0, 120)}`;
                return {row_count: best.length, signature, body_text: bodyText.slice(0, 500)};
                """
            ) or {}
            self._back_to_parent()
            return snap
        except NoSuchFrameException:
            self._back_to_parent()
            return {"row_count": 0, "signature": "", "body_text": ""}
        except Exception:
            self._back_to_parent()
            return {"row_count": 0, "signature": "", "body_text": ""}

    def _wait_contenedor_data(self, timeout: int = 15, previous_signature: str = None) -> bool:
        deadline = time.time() + timeout
        last_snapshot = None
        while time.time() < deadline:
            if self._stop:
                return False

            snap = self._contenedor_snapshot()
            last_snapshot = snap
            row_count = int(snap.get("row_count") or 0)
            signature = (snap.get("signature") or "").strip()
            body = (snap.get("body_text") or "").strip().lower()

            if row_count > 1:
                if not previous_signature:
                    self._last_table_signature = signature
                    return True
                if signature and signature != previous_signature:
                    self._last_table_signature = signature
                    return True

            if body and any(msg in body for msg in ["error", "no hay", "sin datos"]):
                return False

            time.sleep(0.25 if self._station_fast_mode else 0.45)

        if last_snapshot and int(last_snapshot.get("row_count") or 0) > 1:
            self._last_table_signature = (last_snapshot.get("signature") or "").strip()
            return True
        return False

    def _table_to_csv(self, csv_path: Path):
        try:
            rows = self.driver.execute_script(
                """
                const tables = Array.from(document.querySelectorAll('table'));
                let best = [];
                for (const tbl of tables) {
                    const rows = Array.from(tbl.querySelectorAll('tr')).map(tr =>
                        Array.from(tr.querySelectorAll('th,td')).map(td => (td.innerText || td.textContent || '').trim())
                    ).filter(row => row.some(v => v));
                    if (rows.length > best.length) best = rows;
                }
                return best;
                """
            ) or []
        except Exception:
            rows = []

        if len(rows) < 2:
            raise RuntimeError("Tabla vacía o sin suficientes filas")

        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

    # ───────────────────────── flujo manual público ─────────────────────────
    def prepare_manual_mode(self):
        with self._lock:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.region_dir.mkdir(parents=True, exist_ok=True)
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            self._init_driver()
            self.log(f"Iniciando análisis manual — región: {self.region}", "info")
            self.log(
                "Proyecto Scraping V12: primero se inventarian todas las estaciones. Luego tú eliges una, la abres manualmente, pasas a Tabla, resuelves CAPTCHA y confirmas la descarga.",
                "info",
            )
            self._open_main()
            self._inventory_station_urls()
            self.state["inventory_complete"] = True
            self.state["browser_ready"] = True
            self.state["manual_mode"] = True
            self.state["selection_required"] = True
            self._set(current_task="Inventario completo. Selecciona una estación y ábrela manualmente.")
            self.log("Inventario completo. Ya puedes seleccionar una estación desde la interfaz.", "success")

    def open_station_for_manual_review(self, station_url: str):
        with self._lock:
            if not self.driver:
                raise RuntimeError("El navegador no está listo. Primero analiza la región.")
            entry = next((e for e in self.station_entries if e["url"] == station_url), None)
            if not entry:
                raise RuntimeError("No se encontró la estación seleccionada en el inventario.")

            self.selected_station_url = station_url
            self.selected_station_entry = entry
            self._open_station(station_url)
            self._set(
                current_station=entry.get("code") or entry.get("name") or "—",
                current_category=entry.get("type_guess") or "—",
                current_period="",
                current_task="Estación abierta. Cambia manualmente a Tabla, resuelve CAPTCHA y verifica los datos. Luego pulsa Confirmar descarga.",
            )
            self.state["selection_required"] = False
            self.state["manual_ready_confirmation"] = True
            self.log(f"Estación abierta para revisión manual: {entry['label']}", "info")
            self.log("Ahora cambia manualmente a Tabla, resuelve la validación si aparece y cuando veas los datos pulsa Confirmar descarga.", "warning")
            return entry

    def download_selected_station_manual(self):
        with self._lock:
            if not self.driver:
                raise RuntimeError("El navegador no está listo.")
            if not self.selected_station_url:
                raise RuntimeError("Primero abre manualmente una estación seleccionada.")
            if not self._wait_manual_ready():
                raise RuntimeError("No se detectó frmData/tabla lista. Asegúrate de estar en la pestaña Tabla y de ver los datos antes de confirmar.")
            self.state["manual_ready_confirmation"] = False
            self._process_current_station_manual()
            self._set(current_task="Descarga completada. Puedes abrir otra estación manualmente.")
            self.state["selection_required"] = True
            self.selected_station_url = ""
            self.selected_station_entry = None

    # ───────────────────────── descarga de estación actual ─────────────────────────
    def _process_current_station_manual(self):
        self._station_fast_mode = False
        self._last_table_signature = None

        meta = self._station_meta()
        if not meta["code"] and self.selected_station_entry:
            meta["code"] = self.selected_station_entry.get("code", "")
        if not meta["type"] and self.selected_station_entry:
            meta["type"] = self.selected_station_entry.get("type_guess", "")
            meta["folder"] = safe_name(meta["type"] or "Sin clasificar")
        if not meta["name"] and self.selected_station_entry:
            meta["name"] = safe_name(self.selected_station_entry.get("name") or meta["code"] or "Estacion")

        if not self._region_matches(meta["dept"]):
            raise RuntimeError(f"Estación fuera de la región objetivo (dept={meta['dept']})")

        self._set(current_category=meta["type"], current_station=meta["name"], current_period="")
        self.log(f"Estación: {meta['name']} | Tipo: {meta['type']} | Código: {meta['code']}", "info")

        station_folder = self.region_dir / meta["folder"] / meta["name"]
        station_folder.mkdir(parents=True, exist_ok=True)

        periods = self._period_options()
        if not periods:
            self.log("Sin selector de periodo → extrayendo tabla única", "info")
            csv_path = station_folder / "datos.csv"
            try:
                self._into_contenedor()
                self._table_to_csv(csv_path)
                self._back_to_parent()
            except Exception:
                self._back_to_parent()
                try:
                    self.driver.execute_script("document.getElementById('frmData').submit();")
                except Exception:
                    pass
                time.sleep(1.2)
                if not self._wait_contenedor_data(timeout=12):
                    raise RuntimeError("No se encontraron datos en la tabla única")
                self._into_contenedor()
                self._table_to_csv(csv_path)
                self._back_to_parent()

            self.state["downloaded"].append(str(csv_path))
            self.state["progress"] += 1
            self.log(f"✅ {csv_path.name}", "success")
            return

        self.state["total"] += len(periods)
        self.log(f"Periodos encontrados: {len(periods)}", "info")

        for period, value in periods:
            if self._stop:
                return

            self._set(current_period=period, current_task=f"Descargando {meta['name']} – {period}")
            self.log(f"→ {period}", "info")

            csv_path = station_folder / f"{period}.csv"
            if csv_path.exists() and csv_path.stat().st_size > 50:
                self.log(f"↩ Ya existe {csv_path.name}", "info")
                continue

            try:
                previous_signature = self._last_table_signature
                self._submit_period(period, value)
                timeout = 6 if self._station_fast_mode else 15
                if not self._wait_contenedor_data(timeout=timeout, previous_signature=previous_signature):
                    raise RuntimeError(f"Sin datos en la tabla para {period}")

                self._into_contenedor()
                self._table_to_csv(csv_path)
                self._back_to_parent()
                self._station_fast_mode = True
                self.state["downloaded"].append(str(csv_path))
                self.state["progress"] += 1
                self.log(f"✅ {csv_path.name}", "success")
            except Exception as exc:
                self._back_to_parent()
                msg = f"{meta['name']} | {period} | {exc}"
                self.state["errors"].append(msg)
                self.log(f"❌ {period}: {exc}", "error")
                self._dump(f"station_error_{safe_name(meta['name'])}")
