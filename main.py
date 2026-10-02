"""
TikTok Downloader API · calidad original

Recibe un link de TikTok y devuelve el .mp4 directamente a quien lo pidió.
No recodifica nada: entrega el archivo tal cual lo sirve TikTok (sin marca de agua cuando es posible).

Variables de entorno (todas opcionales):
  API_KEY           clave que deben mandar los clientes en el header X-API-Key (vacía = API pública)
  MAX_CONCURRENT    descargas simultáneas máximas (default 2)
  MIN_SHORT_SIDE    si la fuente principal ya da este lado corto (px) sin marca, se entrega al instante (default 1080)
  PARALLEL_TIMEOUT  segundos máximos esperando al resto de fuentes (default 60)
  ALLOWED_ORIGINS   orígenes CORS separados por coma (default *)
"""
import os
import re
import json
import time
import shutil
import secrets
import logging
import tempfile
import threading
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutTimeout
from urllib.parse import urlparse

import httpx
import cloudscraper
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from starlette.background import BackgroundTask

try:
    import tiktok_downloader as td
except Exception as e:  # la API sigue funcionando solo con tikwm-hd
    td = None
    print("⚠️ No pude importar tiktok_downloader:", e)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tiktok-api")

# ───────── Configuración ─────────
API_KEY = os.getenv("API_KEY", "kayro")
MAX_CONCURRENT = int(os.getenv("MAX_CONCURRENT", "2"))
MIN_SHORT_SIDE = int(os.getenv("MIN_SHORT_SIDE", "1080"))
PARALLEL_TIMEOUT = int(os.getenv("PARALLEL_TIMEOUT", "60"))
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"}
EXPOSED = ["X-Source", "X-Resolution", "X-FPS", "X-Bitrate-Kbps", "X-Watermark", "X-Video-Id"]


# ───────── Utilidades ─────────
def stream_to(link, path):
    """Guarda el archivo tal cual lo sirve TikTok (sin recodificar)."""
    with httpx.stream("GET", link, headers=UA, follow_redirects=True, timeout=60) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_bytes(1 << 16):
                f.write(chunk)


def video_id(url):
    try:
        r = httpx.get(url, headers=UA, follow_redirects=True, timeout=15)
        m = re.search(r"/(?:video|photo)/(\d+)", str(r.url))
        if m:
            return m.group(1)
    except Exception:
        pass
    return str(int(time.time()))


def probe(path):
    size = os.path.getsize(path)
    try:  # ffprobe (preciso)
        j = json.loads(subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name,width,height,r_frame_rate:format=duration",
             "-of", "json", path], capture_output=True, text=True, check=True).stdout)
        s, dur = j["streams"][0], float(j["format"].get("duration") or 0)
        a, b = s["r_frame_rate"].split("/")
        return dict(w=int(s["width"]), h=int(s["height"]), codec=s["codec_name"],
                    fps=round(int(a) / int(b), 2) if int(b) else 0, size=size,
                    kbps=int(size * 8 / dur / 1000) if dur else 0)
    except Exception:
        try:  # respaldo: moviepy (solo lee metadatos, no recodifica)
            from moviepy.editor import VideoFileClip
            with VideoFileClip(path) as c:
                w, h = c.size
                return dict(w=int(w), h=int(h), codec="?", fps=round(c.fps or 0, 2), size=size,
                            kbps=int(size * 8 / c.duration / 1000) if c.duration else 0)
        except Exception:
            return dict(w=0, h=0, codec="?", fps=0, size=size, kbps=0)


def is_tiktok_url(url):
    """Solo acepta links de TikTok (evita que usen la API para otras webs)."""
    try:
        u = urlparse(url)
        h = (u.hostname or "").lower()
        return u.scheme in ("http", "https") and (h == "tiktok.com" or h.endswith(".tiktok.com"))
    except Exception:
        return False


# ───────── Fuentes: tikwm HD directo + todos los descargadores del repo ─────────
def from_tikwm_hd(url, path):
    sc = cloudscraper.create_scraper()
    d = sc.get("https://www.tikwm.com/api/", params={"url": url, "hd": 1}, timeout=30).json()
    data = d.get("data") or {}
    link = data.get("hdplay") or data.get("play")
    if not link:
        raise RuntimeError(d.get("msg", "sin enlace"))
    if link.startswith("/"):
        link = "https://www.tikwm.com" + link
    stream_to(link, path)
    return False  # sin marca de agua


def make_repo_source(func):
    def _run(url, path):
        items = list(func(url))
        items.sort(key=lambda i: bool(getattr(i, "watermark", False)))  # sin marca primero
        last = RuntimeError("sin resultados")
        for it in items:
            try:
                if os.path.exists(path):
                    os.remove(path)
                try:
                    it.download(path)
                except Exception:
                    link = getattr(it, "url", None)
                    if not link:
                        raise
                    stream_to(link, path)
                if os.path.exists(path) and os.path.getsize(path) > 50_000:
                    return bool(getattr(it, "watermark", False))
            except Exception as e:
                last = e
        raise last
    return _run


SOURCES = [("tikwm-hd", from_tikwm_hd)]
if td:
    for n, a in [("tikwm", "tikwm"), ("snaptik", "snaptik"), ("ttdownloader", "ttdownloader"),
                 ("mdown", "mdown"), ("tikmate", "Tikmate"), ("ssstik", "ssstik"), ("tikdown", "tikdown")]:
        if hasattr(td, a):
            SOURCES.append((n, make_repo_source(getattr(td, a))))
    if hasattr(td, "VideoInfo"):
        SOURCES.append(("tiktok", make_repo_source(td.VideoInfo.service)))


# ───────── Descarga: la mejor calidad disponible ─────────
def run_source(name, fn, url, workdir, base):
    path = os.path.join(workdir, f"{base}__{name}.mp4")
    try:
        wm = fn(url, path)
        if not (os.path.exists(path) and os.path.getsize(path) > 50_000):
            raise RuntimeError("archivo vacío")
        c = dict(name=name, path=path, wm=wm, **probe(path))
        log.info("✓ %s: %sx%s · %.1f MB · marca=%s", name, c["w"], c["h"], c["size"] / 1e6,
                 "sí" if wm else "no")
        return c
    except Exception as e:
        log.info("✗ %s: %s", name, str(e)[:90])
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
        return None


def fetch_best(url, workdir):
    """
    1) Prueba tikwm-hd. Si ya da HD sin marca (lado corto >= MIN_SHORT_SIDE) lo entrega al instante.
    2) Si no, lanza el resto de fuentes en paralelo y se queda con la mejor.
    Orden de "mejor": sin marca de agua > más resolución > más peso.
    """
    base = f"tiktok_{video_id(url)}"
    cands = []

    name, fn = SOURCES[0]
    first = run_source(name, fn, url, workdir, base)
    if first:
        cands.append(first)
        if not first["wm"] and min(first["w"], first["h"]) >= MIN_SHORT_SIDE:
            best = first
            return _finalize(best, workdir, base)

    rest = SOURCES[1:]
    if rest:
        pool = ThreadPoolExecutor(max_workers=len(rest))
        futs = [pool.submit(run_source, n, f, url, workdir, base) for n, f in rest]
        try:
            for fut in as_completed(futs, timeout=PARALLEL_TIMEOUT):
                c = fut.result()
                if c:
                    cands.append(c)
        except FutTimeout:
            log.warning("tiempo agotado esperando fuentes, uso lo que haya")
        pool.shutdown(wait=False, cancel_futures=True)

    if not cands:
        return None
    cands.sort(key=lambda c: (not c["wm"], c["w"] * c["h"], c["size"]), reverse=True)
    return _finalize(cands[0], workdir, base)


def _finalize(best, workdir, base):
    final = os.path.join(workdir, f"{base}.mp4")
    os.replace(best["path"], final)
    best["path"] = final
    best["base"] = base
    return best


# ───────── API ─────────
app = FastAPI(title="TikTok Downloader API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "X-API-Key"],
    expose_headers=EXPOSED,
)
gate = threading.BoundedSemaphore(MAX_CONCURRENT)


class DownloadBody(BaseModel):
    url: str


def _auth(x_api_key, key):
    if not API_KEY:
        return
    given = (x_api_key or key or "").encode()
    if not secrets.compare_digest(given, API_KEY.encode()):
        raise HTTPException(401, "API key inválida")


def _serve(url: str):
    url = url.strip()
    if not is_tiktok_url(url):
        raise HTTPException(400, "Manda un link válido de TikTok")
    if not gate.acquire(timeout=30):
        raise HTTPException(503, "Servidor ocupado, intenta de nuevo en unos segundos")

    workdir = tempfile.mkdtemp(prefix="tt_")
    try:
        best = fetch_best(url, workdir)
    except Exception:
        shutil.rmtree(workdir, ignore_errors=True)
        log.exception("fallo inesperado descargando %s", url)
        raise HTTPException(500, "Error interno al descargar el video")
    finally:
        gate.release()

    if not best:
        shutil.rmtree(workdir, ignore_errors=True)
        raise HTTPException(502, "Ningún descargador pudo con ese link. Revisa que sea público.")

    headers = {
        "X-Source": best["name"],
        "X-Resolution": f"{best['w']}x{best['h']}",
        "X-FPS": str(best["fps"]),
        "X-Bitrate-Kbps": str(best["kbps"]),
        "X-Watermark": "yes" if best["wm"] else "no",
        "X-Video-Id": best["base"].replace("tiktok_", ""),
    }
    return FileResponse(
        best["path"],
        media_type="video/mp4",
        filename=f"{best['base']}.mp4",
        headers=headers,
        background=BackgroundTask(shutil.rmtree, workdir, True),  # borra el temporal al terminar de enviar
    )


@app.get("/")
def home():
    return {
        "status": "ok",
        "uso": {
            "GET": "/download?url=<link de TikTok>",
            "POST": '/download con JSON {"url": "<link de TikTok>"}',
            "auth": "header X-API-Key (solo si el servidor tiene API_KEY)",
            "respuesta": "archivo .mp4 + headers X-Source, X-Resolution, X-FPS, X-Bitrate-Kbps, X-Watermark",
        },
        "fuentes": [n for n, _ in SOURCES],
        "docs": "/docs",
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/download")
def download_get(
    url: str = Query(..., description="Link del video de TikTok"),
    x_api_key: str | None = Header(default=None),
    key: str | None = Query(default=None),
):
    _auth(x_api_key, key)
    return _serve(url)


@app.post("/download")
def download_post(
    body: DownloadBody,
    x_api_key: str | None = Header(default=None),
    key: str | None = Query(default=None),
):
    _auth(x_api_key, key)
    return _serve(body.url)
