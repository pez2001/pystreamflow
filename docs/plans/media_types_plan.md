# Plan: Bilder, Video und Audio in PyStreamFlow

Stand: 2026-09-25 · Basis: Commit `6808ee1` (Version 1.0)

## 1. Ausgangslage

Was heute schon funktioniert, und wo es hakt:

| Bereich | Heute | Problem bei Medien |
|---|---|---|
| Items in Pipes (`core/stream.py`) | Beliebige Python-Objekte, `asyncio.Queue` | Grundsätzlich geeignet, `bytes` passen durch |
| `Pipe` Standardgröße | `maxsize=0` (unbegrenzt) | Bei Video-Frames wächst der Speicher ohne Grenze, wenn ein Consumer langsamer ist |
| `BaseNode.emit()` (`core/node.py:829`) | `len(str(item).encode())` für `bytes_out` | `str(bytes)` erzeugt ein `repr` mit etwa 4× Größe. Bei einem 10-MB-Bild entstehen pro Emit rund 40 MB Müll |
| `_last_items` (`core/node.py:108`) | Hält die letzten 100 Items im RAM | 100 Full-HD-Frames sind rund 600 MB pro Node |
| Live-View `GET /nodes/{id}/last` (`api/server.py:408`) | JSON-Antwort, `editor.js:3077` zeigt `JSON.stringify` | FastAPI kodiert `bytes` per `.decode()`. Binärdaten lösen einen `UnicodeDecodeError` aus, die Antwort ist ein HTTP 500 |
| `FileInputNode` (`nodes/input_file.py`) | Liefert eine Datei wie `tail -f` als Byte-Stücke | Bei Binärdateien kommen Fragmente statt eines ganzen Bildes an. `read_bytes()` blockiert außerdem den Event-Loop |
| `FileOutputNode` | Schreibt `bytes` schon korrekt | Hängt aber an und schreibt nicht eine Datei pro Item |
| `Base64Encode/Decode` | Kann schon `bytes` verarbeiten | Nutzbar als Brücke zu JSON, MQTT und HTTP |
| `port_schema.py` | Nur Port-Namen, keine Datentypen | Der Editor kann nicht verhindern, dass ein Video-Ausgang an `TextUpperNode` hängt |
| Dockerfile | `python:3.11-slim`, kein ffmpeg | Ohne ffmpeg gibt es keine Video- und Audio-Dekodierung |

Fazit: Die Engine-Architektur mit async Pipes und Fan-out/Fan-in trägt Medien. Die Arbeit liegt in **einem gemeinsamen Datentyp**, im **Speicher- und Backpressure-Verhalten**, in der **Live-View** und in **neuen Nodes**.

## 2. Leitentscheidungen

1. **Ein Medien-Envelope statt roher `bytes`.** Ein Item trägt Metadaten (MIME-Typ, Maße, Zeitstempel) mit sich, damit Downstream-Nodes nicht raten müssen.
2. **Referenz statt Kopie bei großen Daten.** Kleine Payloads (unter etwa 2 MB, konfigurierbar) liegen inline im Speicher. Große Payloads liegen in einem Blob-Store auf Disk unter `PSF_DATA_DIR/blobs`, und nur ein Handle wandert durch die Pipe. Das hält `_last_items`, Fan-out und die Live-View billig.
3. **Schwere Abhängigkeiten sind optional.** Pillow, numpy, PyAV/ffmpeg und soundfile kommen als Extras (`pip install pystreamflow[image]`, `[video]`, `[audio]`, `[media]`). Nodes registrieren sich nur, wenn der Import klappt, damit die Kerninstallation schlank bleibt.
4. **Kein Blockieren im Event-Loop.** Dekodieren, Encodieren und Resizen laufen über `asyncio.to_thread()`. ffmpeg läuft als Subprozess über das vorhandene `core/subprocess_exec.py`.
5. **Rückwärtskompatibel.** Bestehende Nodes, die Strings oder Dicts verarbeiten, ändern sich nicht. `str(MediaItem)` liefert eine kurze Beschreibung wie `<image/png 1920x1080 3.1MB>`, damit `DisplayNode` und Text-Nodes nicht mit Binärmüll überflutet werden.

## 3. Phasen

### Phase 1: Fundament (Kern, ohne neue Abhängigkeiten)

> **Status: umgesetzt.** Code in `core/media.py`, `core/blob_store.py`, `core/node.py`, `core/stream.py`, `core/engine.py`, `core/metrics.py`, `api/server.py` und `mcp/server.py`; Tests in `tests/test_media_phase1.py`; Beschreibung im Abschnitt „Media Items“ von `docs/developer_guide.md`. Abweichungen und Ergänzungen zum Plan:
> - Lebensdauer der Blobs per TTL (ab letztem Lesen oder Schreiben) plus LRU-Obergrenze, keine Referenzzählung.
> - Medien-Ports deklariert eine Node-Klasse über `MEDIA_OUTPUT_PORTS = {port: drop_policy}`. Port-`dtype`s kommen erst in Phase 6b.
> - Neue Drop-Policy `drop_oldest` (behält die neuesten Frames). `drop` verwirft jetzt sofort, wenn der Puffer voll ist, und nicht erst nach einem Timeout.
> - `BaseNode.emit_wait()` wartet auf die Puts, damit Decoder in Phase 5 echten Gegendruck bekommen. `emit()` plant die Puts weiterhin im Hintergrund ein.
> - `GET /nodes/{id}` gibt es nicht. Die zentrale Serialisierung greift stattdessen bei `/nodes/{id}/last`, `/nodes/{id}/emit`, `/reflection/nodes[/{id}]`, `/sessions/{id}/nodes/{id}` und allen MCP-Tool-Ergebnissen.
> - `GET /media/{ref}` liefert nur Bild-, Audio- und Video-Typen mit ihrem MIME-Typ aus, alles andere als `application/octet-stream` mit `nosniff`. Weil `<img>` keinen Auth-Header senden kann, muss die Live-View-Vorschau in Phase 6a die Datei per `fetch()` mit API-Key laden.

**1.1 `core/media.py`: Datentyp `MediaItem`**
```python
@dataclass
class MediaItem:
    kind: Literal["image", "video", "audio", "video_frame", "audio_chunk", "binary"]
    mime: str                     # "image/png", "audio/wav", "video/mp4", ...
    data: bytes | None = None     # inline
    ref: str | None = None        # Blob-ID, wenn ausgelagert
    meta: dict = field(default_factory=dict)  # width/height/fps/sample_rate/channels/duration/pts/source_path
    def get_bytes(self) -> bytes: ...          # lädt bei ref aus dem Blob-Store
    def size(self) -> int: ...
    def summary(self) -> dict: ...             # JSON-sicher, ohne Payload
    def __str__(self) -> str: ...              # "<image/png 1920x1080 3.1MB>"
```
Dazu kommt ein Helfer `sniff_mime(bytes)` über Magic Bytes für PNG, JPEG, GIF, WebP, WAV, MP3, OGG, FLAC, MP4 und WebM.

**1.2 `core/blob_store.py`**: Dateibasiert unter `PSF_DATA_DIR/blobs/<sha256>`. Er bekommt Referenzzählung oder eine TTL (z. B. 10 min) und eine Größenobergrenze (`PSF_BLOB_MAX_MB`), und eine Aufräum-Task im Engine-Lifecycle leert ihn. Der Content-Hash dedupliziert Fan-out gleich mit.

**1.3 Fixes in `BaseNode`**
- `emit()`: `bytes_out` über `item.size()` bzw. `len(item)` bei `bytes`, **nie** über `str(bytes)`.
- `_last_items`: `MediaItem` ist durch seine Blob-Referenz schon klein. Rohe `bytes` über einer Schwelle werden als `{'kind': 'binary', 'size': N}` plus ein kurzer Hex-Kopf gespeichert, nicht die ganze Payload. `manual_emit()` muss dafür die echte Payload behalten: Pro Port wird nur das **letzte** echte Item gehalten, die History enthält nur Zusammenfassungen.
- `_clean_attribute_value()` (raw-Edge) reicht `MediaItem` unverändert durch.

**1.4 Live-View JSON-sicher machen**
- Eine zentrale Serialisierungsfunktion für `/nodes/{id}/last`, `/nodes/{id}` und die MCP-Tools. Sie wandelt `bytes` in `{"$binary": size, "head": "89504e47..."}` um und `MediaItem` in `summary()` plus `preview_url`.
- Ein neuer Endpoint `GET /media/{ref}` liefert den Blob mit dem richtigen `Content-Type` aus, inklusive `Range`-Header für Audio und Video. Er läuft über dieselbe Auth wie die restliche API (`core/auth.py`).

**1.5 Backpressure**: Die Engine setzt für Edges, deren Quelle einen Medien-Port hat, ein Default-`maxsize` (z. B. 8) mit `drop_policy: drop` für Live-Frames. Das lässt sich pro Edge in der YAML überschreiben (`edge.buffer: {maxsize, drop_policy}`). Die Drops erscheinen in `stats()` und in Prometheus.

**Tests:** Roundtrip von `MediaItem` (inline und ref), die Blob-Store-TTL, ein Binär-Item über `/nodes/{id}/last` ergibt kein 500, `emit()` erzeugt bei 50 MB keinen `repr`, und ein voller Pipe mit `drop` zählt die Drops hoch.

### Phase 2: Datei-Ein-/Ausgabe für Medien

> **Status: umgesetzt.** Neue Nodes in `nodes/media_file_input.py` und `nodes/media_file_output.py`, HTTP-Teil in `core/media_http.py`, Tests in `tests/test_media_phase2.py`, Doku im Abschnitt „Media Nodes“ von `docs/nodes/index.md`. Details und Abweichungen:
> - `MediaFileInputNode`: `emit_on: change` sendet beim Start und bei jeder Änderung von Größe oder mtime, `start` nur einmal pro Pfad. Der Ausgang ist über `MEDIA_OUTPUT_PORTS` als `block` deklariert (ganze Dateien gehen nie verloren), gesendet wird per `emit_wait()`.
> - `DirectoryInputNode`: neue Config `extensions`, `media_only` und `emit_as: path|media`. Ohne diese Optionen verhält er sich wie bisher.
> - `MediaFileOutputNode`: Muster-Felder `{node}`, `{index}`, `{ext}`, `{kind}`, `{stem}`, `{timestamp}`. Bestehende Dateien werden standardmäßig nie überschrieben. Der Node gibt zusätzlich `{path, mime, size}` auf `out` aus, damit die geschriebene Datei weiterverdrahtet werden kann.
> - Web-/API-Eingang: neben JSON jetzt auch `multipart/form-data` (Formularfelder landen in `meta.form`), rohe `image/*`-, `audio/*`-, `video/*`- und `application/octet-stream`-Bodies sowie normale urlencoded-Formulare. Obergrenze per `max_upload_mb` bzw. `PSF_MAX_UPLOAD_MB` (100), darüber HTTP 413. Neue Abhängigkeit `python-multipart`.
> - Web-/API-Ausgang: `<path>/media` bzw. `/api/<uri>/media` liefern das neueste Medium mit echtem Content-Type und Range-Support aus, ein abgelaufener Blob ergibt 410. Ohne SSE liefern auch `<path>` bzw. `/api/<uri>/raw` ein Medium direkt aus. Die JSON-Wege tragen Zusammenfassungen, keine Bytes.
> - Base64: `data_url: true` beim Encode. Beim Decode wird aus einer Data-URL ein `MediaItem`, `output: auto|text|bytes|media` erzwingt den Ergebnistyp. Normales Base64 verhält sich wie bisher.
> - Palette: neue Kategorie „Media“ mit den beiden neuen Nodes (vorgezogen aus Phase 6).

- **`MediaFileInputNode`**: Liest eine **ganze** Datei als ein `MediaItem` (nicht tailend) und schließt den MIME-Typ aus Endung und Magic Bytes. Gelesen wird in `to_thread`. Config: `path`, `emit_on: start|change`.
- **`DirectoryInputNode`** erweitern: optionaler Filter `media_only`/`extensions` und Option `emit_as: path|media`.
- **`MediaFileOutputNode`**: Schreibt eine Datei pro Item nach Pattern, z. B. `files/out/{node}_{index:05d}.{ext}`. `record_output()` enthält den Pfad plus eine Vorschau-Ref.
- **`WebInputNode` und `ApiInputNode`**: Nehmen `multipart/form-data` an (Datei-Upload) und `application/octet-stream`, das Ergebnis ist ein `MediaItem`. Das deckt Upload aus dem Browser und `curl -F` ab.
- **`WebOutputNode` und `ApiOutputNode`**: Liefern ein `MediaItem` mit korrektem `Content-Type` aus (Bild direkt, Audio und Video als Stream).
- **`Base64EncodeNode` und `Base64DecodeNode`**: Verstehen `MediaItem` und liefern optional eine Data-URL (`data:image/png;base64,...`) für LLM-, HTTP- und MQTT-Pfade.

### Phase 3: Bilder (Extra `[image]`: Pillow, optional numpy)

> **Status: umgesetzt.** Code in `core/media_transform.py` (Basis `MediaTransformNode`) und `nodes/image_nodes.py`, Vision in `nodes/llm_lmstudio.py`, Tests in `tests/test_media_phase3.py`, Doku im Abschnitt „Image Nodes“ von `docs/nodes/index.md`. Details und Abweichungen:
> - Pillow ist optional (`pip install 'pystreamflow[image]'`, auch in `[media]` und `[dev]`). Fehlt es, werden die Bild-Nodes nicht registriert. `GET /node-availability` meldet sie mit Installationshinweis, der Editor graut sie aus (auch im Suchfeld gesperrt), und `Engine.validate()` lehnt Workflows mit ihnen ab, statt still einen No-op-Node einzusetzen. Das ist ein vorgezogener Teil von Phase 6.
> - `ImageDecodeNode` legt kein Pillow-Objekt in `meta`. Das wäre nicht JSON-fähig und würde bei jedem Fan-out mitkopiert. Stattdessen prüft der Node das Bild, trägt `width`/`height`/`mode`/`format` ein und korrigiert den MIME-Typ. Optional richtet er Fotos per EXIF auf (`auto_orient`). Jeder Bild-Node dekodiert selbst, was bei Bildgrößen im Megabyte-Bereich vertretbar ist.
> - Alle Nodes arbeiten auf `image` und `video_frame`, laufen in einem Worker-Thread und reichen alles andere unverändert durch. Bei kaputten Bildern geben sie das Original weiter und zählen den Fehler am Node.
> - `ImageFilterNode` bietet zusätzlich Sättigung und Schärfe; `ImageRotateNode` kann `angle: exif`.
> - `ImageThumbnailNode` wird noch nicht intern für die Live-View genutzt; die Vorschau zeigt weiterhin das Originalbild. `ImageComposeNode` bleibt wie geplant für später.
> - LM-Studio-Vision: neuer Eingang `prompt` und Config `image_prompt`. Bilder gehen als `image_url` mit Data-URL raus, mehrere Bilder in einer Liste oder einem Dict sind möglich.
> - Das Docker-Image enthält Pillow noch nicht (kommt mit Phase 7); dort sind die Bild-Nodes bis dahin ausgegraut.

| Node | Funktion |
|---|---|
| `ImageDecodeNode` | Wandelt ein `MediaItem` in ein Pillow-Bild in `meta` und prüft das Format |
| `ImageResizeNode` | Skaliert auf Breite/Höhe/max_side mit einstellbarem Resampling |
| `ImageCropNode`, `ImageRotateNode`, `ImageFlipNode` | Geometrie |
| `ImageConvertNode` | Wandelt Formate (PNG/JPEG/WebP) mit Qualitätsstufe, Farbraum RGB/L |
| `ImageFilterNode` | Blur, Sharpen, Kanten, Helligkeit und Kontrast |
| `ImageInfoNode` | Gibt Metadaten und EXIF als Dict aus, damit Logik-, Vergleichs- und JSON-Nodes damit arbeiten können |
| `ImageThumbnailNode` | Erzeugt eine kleine Vorschau, wird auch intern für die Live-View genutzt |
| `ImageComposeNode` (später) | Overlay, Text-Wasserzeichen, Grid aus mehreren Eingängen |

Alle bauen auf einer neuen Basis `MediaTransformNode` auf, analog zu `SingleInputTransformNode`, die `transform()` in `to_thread` ausführt.

**LLM-Integration:** `LMStudioNode` nimmt ein `MediaItem` (image) als Vision-Input an und schickt es als `image_url` mit Data-URL. Der Prompt kommt über Config oder einen zweiten Port `prompt`.

### Phase 4: Audio (Extra `[audio]`: soundfile und numpy, ffmpeg für MP3/AAC)

| Node | Funktion |
|---|---|
| `AudioDecodeNode` | Datei zu PCM; Ausgabe als ganzes Stück oder in Chunks (`chunk_ms`) als `audio_chunk` mit `sample_rate`, `channels` und `pts` |
| `AudioEncodeNode` | Chunks zu WAV/FLAC/OGG/MP3, gesammelt pro Segment oder bis zum Trigger |
| `AudioResampleNode` | Setzt Samplerate und Mono/Stereo |
| `AudioGainNode`, `AudioNormalizeNode` | Pegel |
| `AudioLevelNode` | Liefert RMS/Peak als Zahl pro Chunk, das ergibt den Anschluss an `logic_compare` und `TriggerThresholdNode`, z. B. für Stille-Erkennung |
| `AudioSegmentNode` | Schneidet nach Zeit oder an Stille |
| `SpeechToTextNode` (optional, Extra `[stt]`) | faster-whisper lokal oder ein OpenAI-kompatibler Endpoint, passend zum LM-Studio-Ansatz. Ausgabe ist Text, damit die ganze Text-Node-Familie weiterverwendet werden kann |

### Phase 5: Video (Extra `[video]`: PyAV oder ffmpeg-Subprozess)

Entscheidung: **PyAV** als Standard, weil es Frame-genauen Zugriff bietet und keine Pipe-Parser braucht. Wenn PyAV fehlt, wird auf einen **ffmpeg-Subprozess** über `subprocess_exec.py` zurückgefallen.

| Node | Funktion |
|---|---|
| `VideoDecodeNode` | Macht aus einer Datei oder URL (RTSP/HTTP) einen Strom von `video_frame` (JPEG- oder Raw-kodiert, konfigurierbar) mit `pts`, `fps` und `index`. Optional wird die Audiospur als `audio_chunk` auf einem zweiten Port `audio` ausgegeben |
| `VideoFrameSampleNode` | Reduziert die Frames auf jedes n-te, x fps oder nur Keyframes |
| `VideoEncodeNode` | Macht aus Frames (plus optional Audio) eine MP4/WebM-Datei als `MediaItem`, segmentiert per Dauer oder Trigger |
| `VideoInfoNode` | Liefert Dauer, Codec, Auflösung und fps |
| `VideoThumbnailNode` | Erzeugt ein Frame bei t=x s |
| `CameraInputNode` (optional) | Liest eine lokale Webcam oder ein RTSP-Gerät |

Weil Frames auch Bilder sind, lassen sich alle Bild-Nodes aus Phase 3 direkt auf `video_frame` anwenden. Eine typische Pipeline ist Video über Decode, Sample (1 fps), Resize und LMStudio-Vision zu Text, dann weiter über Grep und MQTT.

**Performance-Leitplanken:**
- Frames werden standardmäßig als JPEG im Blob-Store gehalten, nicht roh (1080p roh ≈ 6 MB, JPEG ≈ 200 KB).
- Die Decoder-Nodes respektieren die Backpressure aus Phase 1: Sie blockieren oder verwerfen, statt vorzulaufen.
- Die Metriken bekommen Frames/s, Drop-Rate und Blob-Store-Größe in Prometheus.

### Phase 6: Editor und UI

> **Status 6a (Live-View-Vorschau): umgesetzt** in `api/static/editor.js` und `api/ui.html`.
> - Seitenpanel: Über dem JSON erscheint das neueste Medium der Node-History als `<img>`, `<audio controls>` oder `<video controls>`, andere Typen als Download-Link. Die Beschriftung zeigt Art, MIME-Typ, Maße, Dauer, pts und Größe. Ein Medium mit derselben Ref wird nicht neu gerendert, damit laufendes Audio oder Video beim 1-s-Poll nicht neu startet.
> - „Letztes Frame“-Modus: `video_frame` und `audio_chunk` werden mit dem normalen 1-s-Poll aktualisiert und lassen sich per „⏸ Pause“ einfrieren. MJPEG ist nicht umgesetzt, der Poll reicht für die Vorschau.
> - Das Live-View-Modal (Rechtsklick → „Live view…“) zeigt eine Galerie der bis zu 12 neuesten unterschiedlichen Medien.
> - Auth: Medien werden per `fetch()` über den vorhandenen API-Key-Wrapper geladen und als `blob:`-URL angezeigt, `/media` bleibt geschützt. Die Object-URLs liegen in einem LRU-Cache (32 Einträge) und werden beim Verdrängen freigegeben. Ein abgelaufener Blob wird als „expired“ markiert.
> - Offen: Vorschau direkt auf der Node-Kachel und die „Media“-Kategorie in der Palette. Beides braucht die Medien-Nodes aus Phase 2 bis 5.

- **Port-Datentypen**: `port_schema.py` bekommt optional pro Port einen `dtype` (`any`, `text`, `number`, `json`, `image`, `audio`, `video`, `media`). `validate_edge()` warnt nur und blockiert nicht, damit es rückwärtskompatibel bleibt. Im Editor werden Ports nach `dtype` eingefärbt.
- **Live-View-Vorschau** (`editor.js` rund um Zeile 3077): Wenn ein Eintrag `preview_url` hat, zeigt die Live-View `<img>`, `<audio controls>` oder `<video controls>` statt JSON-Text an. Für Video-Frame-Ströme gibt es einen „letztes Frame“-Modus mit Poll oder MJPEG.
- **Vorschau direkt auf der Node-Kachel**: ein Thumbnail des letzten Bildes oder Frames, optional pro Node.
- **Palette**: neue Kategorie „Media“ mit Bild, Audio und Video. Nodes, deren Extra fehlt, erscheinen ausgegraut mit einem Hinweis wie `pip install pystreamflow[video]`.

### Phase 7: MCP, CLI, Deployment, Doku

- **MCP-Tools**: `send_to_node` nimmt `{"$media": {"path": ...}}` oder Base64 an. `node_last` liefert Zusammenfassungen plus URLs, keine Payloads, damit Tokens und Kontext geschont werden. Ein neues Tool `get_media(ref)` gibt Bilder als MCP-Image-Content zurück, damit eine KI Frames tatsächlich sehen kann.
- **Docker**: Ein zweites Image-Target `runtime-media` installiert `ffmpeg` und `.[media]`. Das Standard-Image bleibt schlank. In `docker-compose.prod.yml` wählt eine Variable das Target. Beim Dockerfile fällt nebenbei auf, dass es die Abhängigkeiten aus `pyproject.toml` teilweise doppelt per Hand nachinstalliert (`mcp` und `paho-mqtt` fehlen dort). Das sollte beim Umbau mit aufgeräumt werden.
- **Doku**: `docs/nodes/index.md` bekommt einen Abschnitt „Media“ und `docs/user_guide.md` ein Kapitel „Mit Bildern, Audio und Video arbeiten“. Unter `workflows/` kommen drei Beispiel-Workflows:
  1. `image_thumbnails.yaml`: Ordner, Resize, WebP, Ausgabeordner
  2. `audio_transcribe.yaml`: WAV-Upload, Resample auf 16 kHz, STT, Datei und MQTT
  3. `video_vision.yaml`: MP4, 1 fps, Resize, LM-Studio-Vision, JSON-Log

## 4. Reihenfolge und Aufwand (grob)

| Phase | Inhalt | Abhängig von | Aufwand |
|---|---|---|---|
| 1 | Fundament, Fixes, Live-View-Absicherung | – | M (das ist die wichtigste Phase, sie behebt auch heutige Bugs mit `bytes`) |
| 2 | Datei, Web und API-I/O | 1 | S–M |
| 3 | Bild-Nodes und Vision im LLM | 1, 2 | M |
| 6a | Live-View-Vorschau (img/audio/video) | 1 | S |
| 4 | Audio | 1, 2 | M |
| 5 | Video | 1, 3, 4 | L |
| 6b | Port-dtypes und Einfärbung | 3–5 | M |
| 7 | MCP, Docker und Doku | laufend | S–M |

Empfehlung: Phase 1 und 6a zuerst. Danach kann man heute schon Bilder durch den Graphen schicken und sehen, bevor irgendein Bild-Node existiert.

## 5. Risiken und offene Fragen

- **Speicher und Disk**: Der Blob-Store braucht harte Limits. Sonst füllt ein Video-Workflow im Dauerbetrieb die Platte.
- **Windows und ffmpeg**: `subprocess_exec.kill_proc_tree` nutzt `os.killpg`, das es unter Windows nicht gibt. Vor dem ffmpeg-Fallback außerhalb von Docker braucht es einen Windows-Pfad (z. B. `proc.kill()` oder `taskkill /T`).
- **Lizenzen**: PyAV und ffmpeg sind LGPL/GPL-abhängig vom Build. Das muss zur Projektlizenz (siehe `LICENSE`) geprüft werden.
- **Offen**: Sollen Echtzeit-Quellen (Webcam, RTSP, Mikrofon) in den Scope, oder zunächst nur Dateien und Uploads? Echtzeit erhöht den Aufwand von Phase 5 deutlich.
- **Offen**: Soll die Spracherkennung lokal laufen (faster-whisper, GPU) oder über einen Server wie bei LM Studio?
