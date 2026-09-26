"""Image nodes (media plan phase 3), built on Pillow.

Pillow is an optional dependency (``pip install pystreamflow[image]``).
``pystreamflow/nodes/__init__.py`` imports this module inside a
``try``/``except ImportError``: without Pillow these node types are simply
not registered, and ``GET /node-availability`` tells the editor to grey
them out with an install hint instead of offering nodes that can't run.

Every node here is a ``MediaTransformNode`` (``core/media_transform.py``):
it works on ``MediaItem``s of kind ``image`` and ``video_frame`` (so they
apply unchanged to decoded video later), runs Pillow in a worker thread,
passes anything else through untouched, and on a corrupt/unsupported
image passes the original through while recording the error.

Output encoding: a transformed image keeps its source format where that
format round-trips well (PNG, JPEG, WebP); GIF, BMP, TIFF and anything
else become PNG. ``ImageConvertNode`` picks a format explicitly.
Results keep the source item's ``kind`` and ``meta`` (``filename``,
``source_path``, ``pts``, ...), with ``width``/``height`` updated.
"""
from __future__ import annotations

import io
from typing import Any

from PIL import ExifTags, Image, ImageEnhance, ImageFilter, ImageOps

from ..core.media import MediaItem
from ..core.media_transform import MediaTransformNode

FORMAT_MIME = {
    "PNG": "image/png", "JPEG": "image/jpeg", "WEBP": "image/webp",
    "GIF": "image/gif", "BMP": "image/bmp", "TIFF": "image/tiff",
}
MIME_FORMAT = {v: k for k, v in FORMAT_MIME.items()}
_KEEP_FORMATS = ("PNG", "JPEG", "WEBP")
_FORMAT_ALIASES = {"jpg": "JPEG", "jpeg": "JPEG", "png": "PNG", "webp": "WEBP", "gif": "GIF", "bmp": "BMP", "tiff": "TIFF", "tif": "TIFF"}

_EXIF_ORIENTATION = 0x0112

RESAMPLE = {
    "nearest": Image.Resampling.NEAREST,
    "bilinear": Image.Resampling.BILINEAR,
    "bicubic": Image.Resampling.BICUBIC,
    "lanczos": Image.Resampling.LANCZOS,
}


# --- helpers -------------------------------------------------------------------

def open_image(item: MediaItem) -> Image.Image:
    """Decode a MediaItem's payload (fully loaded, so the bytes can go)."""
    img = Image.open(io.BytesIO(item.get_bytes()))
    img.load()
    return img


def source_format(item: MediaItem, img: Image.Image | None = None) -> str:
    fmt = (img.format if img is not None and img.format else None) or MIME_FORMAT.get(item.mime)
    return fmt if fmt in _KEEP_FORMATS else "PNG"


def parse_format(value: Any) -> str | None:
    """``"keep"``/empty -> None, otherwise a Pillow format name."""
    v = str(value or "keep").strip().lower()
    if v == "keep":
        return None
    if v not in _FORMAT_ALIASES:
        raise ValueError(f"unsupported image format {value!r} (use keep, png, jpeg, webp, gif, bmp or tiff)")
    return _FORMAT_ALIASES[v]


def _flatten_alpha(img: Image.Image, background=(255, 255, 255)) -> Image.Image:
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        rgba = img.convert("RGBA")
        bg = Image.new("RGB", rgba.size, background)
        bg.paste(rgba, mask=rgba.getchannel("A"))
        return bg
    return img


def encode_image(img: Image.Image, fmt: str, quality: int = 90) -> bytes:
    kwargs: dict[str, Any] = {}
    if fmt == "JPEG":
        img = _flatten_alpha(img)
        if img.mode not in ("RGB", "L", "CMYK"):
            img = img.convert("RGB")
        kwargs = {"quality": quality, "optimize": True}
    elif fmt == "WEBP":
        kwargs = {"quality": quality}
    elif fmt == "BMP" and img.mode not in ("1", "L", "P", "RGB"):
        img = _flatten_alpha(img).convert("RGB")
    elif fmt == "GIF" and img.mode not in ("P", "L"):
        img = img.convert("P", palette=Image.Palette.ADAPTIVE)
    elif img.mode == "CMYK" and fmt in ("PNG", "WEBP"):
        img = img.convert("RGB")
    buf = io.BytesIO()
    img.save(buf, format=fmt, **kwargs)
    return buf.getvalue()


def to_item(src: MediaItem, img: Image.Image, fmt: str | None = None, quality: int = 90, **meta) -> MediaItem:
    fmt = fmt or source_format(src)
    data = encode_image(img, fmt, quality)
    return src.with_data(data, mime=FORMAT_MIME[fmt], meta={"width": img.width, "height": img.height, **meta})


def _int(config: dict, key: str, default: int | None = None, minimum: int | None = None) -> int | None:
    value = config.get(key, default)
    if value in (None, ""):
        return None
    value = int(value)
    if minimum is not None and value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return value


def _choice(config: dict, key: str, options, default: str) -> str:
    value = str(config.get(key, default) or default).strip().lower()
    if value not in options:
        raise ValueError(f"{key} must be one of {', '.join(options)}, not {value!r}")
    return value


def _json_safe(value: Any) -> Any:
    """EXIF values -> JSON-friendly Python values."""
    if isinstance(value, bytes):
        text = value.rstrip(b"\x00")
        try:
            return text.decode("ascii") if text.isascii() and len(text) <= 256 else value[:32].hex()
        except Exception:
            return value[:32].hex()
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    try:
        return float(value)  # IFDRational and friends
    except (TypeError, ValueError):
        return str(value)


def exif_dict(img: Image.Image) -> dict:
    try:
        exif = img.getexif()
    except Exception:
        return {}
    out = {}
    for tag, value in exif.items():
        out[ExifTags.TAGS.get(tag, str(tag))] = _json_safe(value)
    try:
        for tag, value in exif.get_ifd(ExifTags.IFD.Exif).items():
            out[ExifTags.TAGS.get(tag, str(tag))] = _json_safe(value)
    except Exception:
        pass
    return out


# --- nodes ---------------------------------------------------------------------

class ImageDecodeNode(MediaTransformNode):
    """Checks that an item really is a decodable image and fills in its
    metadata: ``width``, ``height``, ``mode`` (RGB, RGBA, L, ...) and
    ``format``; the MIME type is corrected to what the bytes actually
    are. The payload itself is left untouched unless ``auto_orient`` is
    set, in which case a photo carrying an EXIF rotation is rotated
    upright (and re-encoded). A non-image is passed through with the
    error recorded - put this node first to catch bad uploads early.

    Config: ``auto_orient`` (bool, default false).
    """

    async def configure(self):
        self.auto_orient = bool(self.config.get("auto_orient", False))

    def transform_media(self, item):
        img = open_image(item)
        fmt = img.format
        info = {"width": img.width, "height": img.height, "mode": img.mode, "format": fmt}
        if self.auto_orient and img.getexif().get(_EXIF_ORIENTATION, 1) not in (1, None):
            fmt = source_format(item, img)
            upright = ImageOps.exif_transpose(img)
            return to_item(item, upright, fmt, mode=upright.mode, format=fmt)
        mime = FORMAT_MIME.get(fmt, item.mime)
        return MediaItem(kind=item.kind, mime=mime, data=item.data, ref=item.ref,
                         meta={**item.meta, **info}, _size=item.size())


class ImageResizeNode(MediaTransformNode):
    """Scales an image.

    Config:
      max_side: scale so the longer side is this many pixels (wins over
        width/height).
      width / height: target size. With both and ``keep_aspect`` (default
        true) the image is fitted *inside* width x height; without
        ``keep_aspect`` it is stretched to exactly that size. With only one
        of them the other follows the aspect ratio.
      resample: nearest, bilinear, bicubic or lanczos (default).
      upscale: allow enlarging (default false - smaller images are passed
        on unchanged).
    """

    async def configure(self):
        c = self.config
        self.max_side = _int(c, "max_side", minimum=1)
        self.width = _int(c, "width", minimum=1)
        self.height = _int(c, "height", minimum=1)
        self.keep_aspect = bool(c.get("keep_aspect", True))
        self.resample = RESAMPLE[_choice(c, "resample", RESAMPLE, "lanczos")]
        self.upscale = bool(c.get("upscale", False))
        if not (self.max_side or self.width or self.height):
            raise ValueError("ImageResizeNode needs max_side, width or height")

    def target_size(self, w: int, h: int) -> tuple[int, int]:
        if self.max_side:
            scale = self.max_side / max(w, h)
            size = (w * scale, h * scale)
        elif self.width and self.height:
            if self.keep_aspect:
                scale = min(self.width / w, self.height / h)
                size = (w * scale, h * scale)
            else:
                size = (self.width, self.height)
        elif self.width:
            size = (self.width, h * self.width / w)
        else:
            size = (w * self.height / h, self.height)
        size = (max(1, round(size[0])), max(1, round(size[1])))
        if not self.upscale and (size[0] > w or size[1] > h):
            return (w, h)
        return size

    def transform_media(self, item):
        img = open_image(item)
        size = self.target_size(img.width, img.height)
        if size == img.size:
            return item
        return to_item(item, img.resize(size, self.resample), source_format(item, img))


class ImageCropNode(MediaTransformNode):
    """Cuts out a rectangle.

    Config: ``width``, ``height`` (required) and either ``x``/``y`` (top-left
    corner, default 0) or ``center: true`` for a centered crop. The box is
    clamped to the image.
    """

    async def configure(self):
        c = self.config
        self.width = _int(c, "width", minimum=1)
        self.height = _int(c, "height", minimum=1)
        if not (self.width and self.height):
            raise ValueError("ImageCropNode needs width and height")
        self.x = _int(c, "x", 0, minimum=0)
        self.y = _int(c, "y", 0, minimum=0)
        self.center = bool(c.get("center", False))

    def transform_media(self, item):
        img = open_image(item)
        w, h = min(self.width, img.width), min(self.height, img.height)
        if self.center:
            x, y = (img.width - w) // 2, (img.height - h) // 2
        else:
            x, y = min(self.x, img.width - w), min(self.y, img.height - h)
        return to_item(item, img.crop((x, y, x + w, y + h)), source_format(item, img))


class ImageRotateNode(MediaTransformNode):
    """Rotates by ``angle`` degrees counter-clockwise (default 90).
    Multiples of 90 are lossless transposes; other angles are resampled
    with ``expand`` (default true - grow the canvas so nothing is cut off)
    and the new corners filled with ``fill`` (default ``#000000``).
    ``angle: exif`` rotates a photo upright according to its EXIF tag."""

    async def configure(self):
        angle = self.config.get("angle", 90)
        self.from_exif = str(angle).strip().lower() == "exif"
        self.angle = 0.0 if self.from_exif else float(angle) % 360
        self.expand = bool(self.config.get("expand", True))
        self.fill = str(self.config.get("fill", "#000000"))

    def transform_media(self, item):
        img = open_image(item)
        fmt = source_format(item, img)
        if self.from_exif:
            out = ImageOps.exif_transpose(img)
        elif self.angle in (90.0, 180.0, 270.0):
            out = img.transpose({90.0: Image.Transpose.ROTATE_90, 180.0: Image.Transpose.ROTATE_180,
                                 270.0: Image.Transpose.ROTATE_270}[self.angle])
        elif self.angle == 0.0:
            return item
        else:
            src = img if img.mode in ("RGB", "RGBA", "L") else img.convert("RGBA")
            out = src.rotate(self.angle, resample=Image.Resampling.BICUBIC, expand=self.expand, fillcolor=self.fill)
        return to_item(item, out, fmt)


class ImageFlipNode(MediaTransformNode):
    """Mirrors an image. Config: ``direction`` horizontal (default),
    vertical or both."""

    async def configure(self):
        self.direction = _choice(self.config, "direction", ("horizontal", "vertical", "both"), "horizontal")

    def transform_media(self, item):
        img = open_image(item)
        if self.direction in ("horizontal", "both"):
            img = ImageOps.mirror(img)
        if self.direction in ("vertical", "both"):
            img = ImageOps.flip(img)
        return to_item(item, img)


class ImageConvertNode(MediaTransformNode):
    """Re-encodes an image.

    Config:
      format: keep (default), png, jpeg, webp, gif, bmp or tiff.
      quality: 1-100 for JPEG/WebP (default 85).
      mode: keep (default), RGB, RGBA or L (grayscale). Transparency is
        flattened onto white for formats/modes without alpha. WebP has no
        grayscale mode - an L image comes out of WebP as RGB.
    """

    async def configure(self):
        self.format = parse_format(self.config.get("format", "keep"))
        self.quality = _int(self.config, "quality", 85, minimum=1)
        if self.quality > 100:
            raise ValueError("quality must be <= 100")
        mode = str(self.config.get("mode", "keep") or "keep").strip()
        if mode.lower() == "keep":
            self.mode = None
        elif mode.upper() in ("RGB", "RGBA", "L"):
            self.mode = mode.upper()
        else:
            raise ValueError(f"mode must be keep, RGB, RGBA or L, not {mode!r}")

    def transform_media(self, item):
        img = open_image(item)
        fmt = self.format or MIME_FORMAT.get(item.mime) or source_format(item, img)
        if self.mode:
            img = _flatten_alpha(img).convert(self.mode) if self.mode != "RGBA" else img.convert("RGBA")
        return to_item(item, img, fmt, self.quality)


_FILTERS = {
    "none": None,
    "blur": ImageFilter.BLUR,
    "gaussian_blur": None,  # uses radius
    "box_blur": None,       # uses radius
    "sharpen": ImageFilter.SHARPEN,
    "unsharp_mask": None,   # uses radius
    "edges": ImageFilter.FIND_EDGES,
    "edge_enhance": ImageFilter.EDGE_ENHANCE,
    "contour": ImageFilter.CONTOUR,
    "emboss": ImageFilter.EMBOSS,
    "smooth": ImageFilter.SMOOTH,
    "detail": ImageFilter.DETAIL,
}


class ImageFilterNode(MediaTransformNode):
    """Applies a filter and/or tonal adjustments.

    Config:
      filter: none (default), blur, gaussian_blur, box_blur, sharpen,
        unsharp_mask, edges, edge_enhance, contour, emboss, smooth, detail.
      radius: for gaussian_blur/box_blur/unsharp_mask (default 2).
      brightness, contrast, saturation, sharpness: factors, 1.0 = unchanged
        (0.5 = half, 2.0 = double).
    """

    async def configure(self):
        c = self.config
        self.filter = _choice(c, "filter", _FILTERS, "none")
        self.radius = float(c.get("radius", 2))
        self.factors = {}
        for key in ("brightness", "contrast", "saturation", "sharpness"):
            value = float(c.get(key, 1.0))
            if value < 0:
                raise ValueError(f"{key} must be >= 0")
            if value != 1.0:
                self.factors[key] = value

    def transform_media(self, item):
        img = open_image(item)
        fmt = source_format(item, img)
        if img.mode not in ("RGB", "RGBA", "L"):
            img = img.convert("RGBA" if "A" in img.getbands() or "transparency" in img.info else "RGB")
        if self.filter == "gaussian_blur":
            img = img.filter(ImageFilter.GaussianBlur(self.radius))
        elif self.filter == "box_blur":
            img = img.filter(ImageFilter.BoxBlur(self.radius))
        elif self.filter == "unsharp_mask":
            img = img.filter(ImageFilter.UnsharpMask(radius=self.radius))
        elif _FILTERS[self.filter] is not None:
            img = img.filter(_FILTERS[self.filter])
        enhancers = {"brightness": ImageEnhance.Brightness, "contrast": ImageEnhance.Contrast,
                     "saturation": ImageEnhance.Color, "sharpness": ImageEnhance.Sharpness}
        for key, factor in self.factors.items():
            img = enhancers[key](img).enhance(factor)
        return to_item(item, img, fmt)


class ImageInfoNode(MediaTransformNode):
    """Emits a plain dict describing an image instead of the image itself -
    so logic, compare, template and JSON nodes can work with it: ``mime``,
    ``kind``, ``size``, ``width``, ``height``, ``mode``, ``format``,
    ``frames`` (animation frames), ``has_alpha``, ``exif`` (tag name ->
    value; omit with ``exif: false``) and the item's own ``meta``."""

    async def configure(self):
        self.include_exif = bool(self.config.get("exif", True))

    def transform_media(self, item):
        img = open_image(item)
        info = {
            "mime": item.mime, "kind": item.kind, "size": item.size(),
            "width": img.width, "height": img.height, "mode": img.mode, "format": img.format,
            "frames": getattr(img, "n_frames", 1),
            "has_alpha": "A" in img.getbands() or "transparency" in img.info,
            "meta": _json_safe({k: v for k, v in item.meta.items() if k != "form"}),
        }
        if self.include_exif:
            info["exif"] = exif_dict(img)
        return info


class ImageThumbnailNode(MediaTransformNode):
    """Makes a small preview: fits the image into ``max_side`` x
    ``max_side`` pixels (default 256) and encodes it compactly - WebP by
    default (``format``: webp, jpeg or png; ``quality`` default 80).
    Never enlarges."""

    async def configure(self):
        self.max_side = _int(self.config, "max_side", 256, minimum=1)
        self.format = parse_format(self.config.get("format", "webp")) or "WEBP"
        if self.format not in ("WEBP", "JPEG", "PNG"):
            raise ValueError("ImageThumbnailNode format must be webp, jpeg or png")
        self.quality = _int(self.config, "quality", 80, minimum=1)

    def transform_media(self, item):
        img = open_image(item)
        img.thumbnail((self.max_side, self.max_side), Image.Resampling.LANCZOS)
        return to_item(item, img, self.format, self.quality, thumbnail=True)


IMAGE_NODE_CLASSES = (
    ImageDecodeNode, ImageResizeNode, ImageCropNode, ImageRotateNode, ImageFlipNode,
    ImageConvertNode, ImageFilterNode, ImageInfoNode, ImageThumbnailNode,
)
