from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import threading
import os
from scraper import SenamhiScraper

app = Flask(__name__)
CORS(app)

scraping_state = {
    "running": False,
    "progress": 0,
    "total": 0,
    "current_task": "",
    "current_category": "",
    "current_station": "",
    "current_period": "",
    "awaiting_human": False,
    "inventory_complete": False,
    "browser_ready": False,
    "manual_mode": False,
    "selection_required": False,
    "manual_ready_confirmation": False,
    "stations": [],
    "log": [],
    "downloaded": [],
    "errors": [],
}

scraper_instance = None
worker_thread = None

REGIONES = {
    "amazonas": "Amazonas",
    "ancash": "Áncash",
    "apurimac": "Apurímac",
    "arequipa": "Arequipa",
    "ayacucho": "Ayacucho",
    "cajamarca": "Cajamarca",
    "cusco": "Cusco",
    "huancavelica": "Huancavelica",
    "huanuco": "Huánuco",
    "ica": "Ica",
    "junin": "Junín",
    "la-libertad": "La Libertad",
    "lambayeque": "Lambayeque",
    "lima": "Lima / Callao",
    "loreto": "Loreto",
    "madre-de-dios": "Madre de Dios",
    "moquegua": "Moquegua",
    "pasco": "Pasco",
    "piura": "Piura",
    "puno": "Puno",
    "san-martin": "San Martín",
    "tacna": "Tacna",
    "tumbes": "Tumbes",
    "ucayali": "Ucayali",
}


def log_message(msg: str, level: str = "info"):
    import datetime
    entry = {
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "msg": msg,
        "level": level,
    }
    scraping_state["log"].append(entry)
    print(f"[{level.upper()}] {msg}")


def reset_state():
    scraping_state.update({
        "running": False,
        "progress": 0,
        "total": 0,
        "current_task": "Esperando acción...",
        "current_category": "",
        "current_station": "",
        "current_period": "",
        "awaiting_human": False,
        "inventory_complete": False,
        "browser_ready": False,
        "manual_mode": False,
        "selection_required": False,
        "manual_ready_confirmation": False,
        "stations": [],
        "log": [],
        "downloaded": [],
        "errors": [],
    })


@app.route("/")
def index():
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()


@app.route("/api/regiones")
def get_regiones():
    return jsonify(REGIONES)


@app.route("/api/start", methods=["POST"])
def start_manual_inventory():
    global scraper_instance, worker_thread

    if scraping_state["running"]:
        return jsonify({"error": "Ya hay un proceso en curso"}), 400

    data = request.json or {}
    region = data.get("region")
    output_dir = data.get("output_dir", "descargas_senamhi")
    headless = bool(data.get("headless", False))

    if not region:
        return jsonify({"error": "Selecciona una región"}), 400

    reset_state()
    scraping_state["running"] = True
    scraping_state["current_task"] = "Iniciando navegador e inventariando estaciones..."

    scraper_instance = SenamhiScraper(region, output_dir, headless, scraping_state, log_message)

    def runner():
        global scraper_instance
        try:
            scraper_instance.prepare_manual_mode()
        except Exception as exc:
            scraping_state["errors"].append(str(exc))
            log_message(f"Error preparando V12: {exc}", "error")
        finally:
            scraping_state["running"] = False
            scraping_state["awaiting_human"] = False
            if scraping_state["inventory_complete"]:
                scraping_state["current_task"] = "Inventario completado. Selecciona una estación."
            elif not scraping_state["errors"]:
                scraping_state["current_task"] = "Proceso finalizado."

    worker_thread = threading.Thread(target=runner, daemon=True)
    worker_thread.start()
    return jsonify({"status": "started"})


@app.route("/api/stations")
def list_stations():
    return jsonify(scraping_state.get("stations", []))


@app.route("/api/open_station", methods=["POST"])
def open_station():
    global scraper_instance, worker_thread

    if not scraper_instance or not scraping_state.get("browser_ready"):
        return jsonify({"error": "Primero debes analizar la región."}), 400
    if scraping_state["running"]:
        return jsonify({"error": "Hay otra tarea en ejecución."}), 400

    data = request.json or {}
    station_url = data.get("station_url")
    if not station_url:
        return jsonify({"error": "Selecciona una estación."}), 400

    scraping_state["running"] = True

    def runner():
        try:
            scraper_instance.open_station_for_manual_review(station_url)
        except Exception as exc:
            scraping_state["errors"].append(str(exc))
            log_message(f"Error abriendo estación: {exc}", "error")
        finally:
            scraping_state["running"] = False

    worker_thread = threading.Thread(target=runner, daemon=True)
    worker_thread.start()
    return jsonify({"status": "opening"})


@app.route("/api/confirm_download", methods=["POST"])
def confirm_download():
    global scraper_instance, worker_thread

    if not scraper_instance or not scraping_state.get("browser_ready"):
        return jsonify({"error": "No hay navegador listo."}), 400
    if scraping_state["running"]:
        return jsonify({"error": "Hay otra tarea en ejecución."}), 400

    scraping_state["running"] = True

    def runner():
        try:
            scraper_instance.download_selected_station_manual()
            log_message("Descarga manual completada para la estación seleccionada.", "success")
        except Exception as exc:
            scraping_state["errors"].append(str(exc))
            log_message(f"Error en descarga manual: {exc}", "error")
        finally:
            scraping_state["running"] = False

    worker_thread = threading.Thread(target=runner, daemon=True)
    worker_thread.start()
    return jsonify({"status": "downloading"})


@app.route("/api/resume", methods=["POST"])
def resume_scraping():
    scraping_state["awaiting_human"] = False
    log_message("Proceso reanudado por el usuario.", "info")
    return jsonify({"status": "resumed"})


@app.route("/api/stop", methods=["POST"])
def stop_scraping():
    global scraper_instance
    if scraper_instance:
        scraper_instance.close()
        scraper_instance = None
    scraping_state["running"] = False
    scraping_state["awaiting_human"] = False
    scraping_state["browser_ready"] = False
    scraping_state["manual_ready_confirmation"] = False
    scraping_state["selection_required"] = False
    scraping_state["current_task"] = "Detenido por el usuario"
    log_message("Proceso detenido por el usuario.", "warning")
    return jsonify({"status": "stopped"})


@app.route("/api/status")
def get_status():
    return jsonify(scraping_state)


@app.route("/api/files")
def list_files():
    output_dir = request.args.get("dir", "descargas_senamhi")
    files = []
    if os.path.exists(output_dir):
        for root, _, filenames in os.walk(output_dir):
            for fname in filenames:
                if fname.endswith(".csv"):
                    full_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(full_path, output_dir)
                    size = os.path.getsize(full_path)
                    files.append({
                        "name": fname,
                        "path": full_path,
                        "rel_path": rel_path,
                        "size": size,
                    })
    files.sort(key=lambda x: x["rel_path"])
    return jsonify(files)


@app.route("/api/download")
def download_file():
    path = request.args.get("path")
    if path and os.path.exists(path) and path.endswith(".csv"):
        return send_file(path, as_attachment=True)
    return jsonify({"error": "Archivo no encontrado"}), 404


if __name__ == "__main__":
    print("=" * 60)
    print("  Proyecto Scraping V12 - Iniciando servidor")
    print("  Abre tu navegador en: http://localhost:5000")
    print("=" * 60)
    app.run(debug=False, port=5000)
