"""Per-field config schema overrides for the node editor's inline config
editor (see ``pystreamflow/api/ui.html``).

Why this exists
----------------
Before this module, the node editor had two disconnected ways to edit a
node's config: a raw JSON textarea in the Inspector panel (``showInspector``
/ ``updateNode``) and a separate per-field auto-form in the expanded-node
"details" view (``toggleNodeExpand``), which guessed a widget purely from
the live value's own JSON type (bool -> checkbox, number -> number input,
list -> comma-separated text, else -> text, with `mode`/`action` singled
out as a hardcoded three-option dropdown). Editing in one place didn't
update the other until you closed and reopened it, and neither surface
knew that e.g. ``TriggerIfNode.condition`` only accepts four specific
strings, or that a trigger node's ``target_node_id`` is supposed to name
another node in the same graph rather than accept arbitrary hand-typed
text - the exact "typed node ID field" gap 4.3 flagged and Phase 2's
edge-based trigger targeting (``core/trigger_targets.py``) made obsolete
in the backend, but the editor UI never caught up to.

This module is the single field-level schema that lets the editor merge
those two surfaces into one and render the right widget for those known
special cases; `GET /config-schema` exposes it to the UI at load time,
the same way `core/port_schema.py` exposes wiring shape via
`GET /node-schema`.

What this deliberately does NOT do
-----------------------------------
Hand-cataloguing every config field of all ~90 node types (its type,
valid range, docstring) would be a large, separate content effort with
its own scope - and guessing at fields I haven't verified against the
node's actual source would risk asserting schema that's simply wrong.
Rather than fake that completeness, this module only lists the two kinds
of field a generic type-introspecting editor cannot correctly infer from
a live config value alone:

1. ``kind: "enum"`` - a fixed set of legal string values, verified by
   reading the node's own source for a literal comparison
   (``self.condition == 'truthy'`` etc.) - not guessed.
2. ``kind: "node_ref"`` - a config key that names another node in the
   same graph, so the editor can offer a "pick a node from the canvas"
   dropdown instead of a free-text field for a value that's supposed to
   be a valid node id.

Every other field falls back to the same live-value-type inference the
old expanded-node form already did (now implemented once, in
``api/ui.html``'s ``fieldWidgetHtml()``, instead of duplicated).
"""
from __future__ import annotations

# Every node type whose class source (`pystreamflow/nodes/trigger.py`,
# `trigger_advanced.py`) reads `self.config.get('target_node_id')` and
# feeds it into `TriggerActionMixin` (`core/trigger_targets.py`) as the
# fallback target when no control edge is drawn - verified by reading
# each file, not inferred from the class name alone (e.g. `TriggerIfNode`
# and `TimerTriggerNode` both qualify; a hypothetical `TriggerXyzNode`
# would not automatically be assumed to).
_TRIGGER_TARGET_NODE_TYPES = [
    "TriggerNode",
    "TriggerOnNode",
    "TriggerOffNode",
    "TriggerPauseNode",
    "TriggerIfNode",
    "TriggerThresholdNode",
    "TriggerDebounceNode",
    "TriggerPulseNode",
    "TriggerToggleNode",
    "TimerTriggerNode",
]

# type name -> {field name: {"kind": ..., ...}}
FIELD_OVERRIDES: dict[str, dict[str, dict]] = {
    t: {"target_node_id": {"kind": "node_ref"}} for t in _TRIGGER_TARGET_NODE_TYPES
}
# TriggerIfNode.condition (trigger_advanced.py): compared against exactly
# these four string literals.
FIELD_OVERRIDES["TriggerIfNode"]["condition"] = {
    "kind": "enum",
    "options": ["truthy", "equals", "contains", "regex"],
}

# `mode`/`action` used to have a GLOBAL enum override here of
# ["auto", "manual", "off"] - inherited from the old UI's hardcoded
# three-option dropdown without ever being checked against what any node
# actually does with those keys (an omission this module's own docstring
# above warns against: "guessing at fields I haven't verified against the
# node's actual source would risk asserting schema that's simply wrong").
# Checked now, by reading every node that reads a top-level `mode` or
# `action` config key: none of them use "auto"/"manual"/"off" at all.
# `mode` means "single"/"multi" line-emission (line_buffer.py) or
# "push_pop"/"peek"/"clear" stack behavior (stack_node.py); `action` means
# a lifecycle command - "start"/"stop"/"pause"/"resume", handled by
# BaseNode.handle_control() - for every Trigger* node type
# (trigger.py/trigger_advanced.py). The old global override would have
# rendered a combo box offering only the three wrong options for every one
# of these real fields, making the *correct* values impossible to select
# through the UI widget at all (only reachable via "+ field / advanced
# JSON..."). Replaced with accurate per-type overrides below instead of a
# blanket (and wrong) global guess.
GLOBAL_FIELD_OVERRIDES: dict[str, dict] = {}

# Verified against pystreamflow/nodes/line_buffer.py and stack_node.py.
FIELD_OVERRIDES["LineBufferNode"] = {
    "mode": {"kind": "enum", "options": ["single", "multi"]},
}
FIELD_OVERRIDES["StackNode"] = {
    "mode": {"kind": "enum", "options": ["push_pop", "peek", "clear"]},
}

# Verified against pystreamflow/core/node.py's BaseNode.handle_control():
# only these four control commands do anything.
_CONTROL_ACTIONS = ["start", "stop", "pause", "resume"]
for _t in _TRIGGER_TARGET_NODE_TYPES:
    # .update(), not assignment - every one of these types already has a
    # target_node_id override from the dict comprehension above (and
    # TriggerIfNode also has 'condition'); reassigning the whole per-type
    # dict here would silently delete those instead of adding to them.
    FIELD_OVERRIDES[_t].update({"action": {"kind": "enum", "options": _CONTROL_ACTIONS}})
FIELD_OVERRIDES["TriggerToggleNode"].update({
    "action_on": {"kind": "enum", "options": _CONTROL_ACTIONS},
    "action_off": {"kind": "enum", "options": _CONTROL_ACTIONS},
})
# TriggerToggleNode has no top-level `action` key at all (it uses
# action_on/action_off instead) - the loop above still added a harmless,
# unused "action" override for it since it's also a target_node_id type;
# remove it for cleanliness rather than leave a schema entry for a key
# that will never actually appear on this node type's config.
del FIELD_OVERRIDES["TriggerToggleNode"]["action"]


# Media plan phase 2 - verified against media_file_input.py,
# input_directory.py and base64_decode_node.py (each validates these
# values in init()).
FIELD_OVERRIDES["MediaFileInputNode"] = {
    "emit_on": {"kind": "enum", "options": ["change", "start"]},
}
FIELD_OVERRIDES["DirectoryInputNode"] = {
    "emit_as": {"kind": "enum", "options": ["path", "media"]},
}
FIELD_OVERRIDES["Base64DecodeNode"] = {
    "output": {"kind": "enum", "options": ["auto", "text", "bytes", "media"]},
}


# Media plan phase 3 - verified against nodes/image_nodes.py (each value
# is validated in that node's configure()).
FIELD_OVERRIDES["ImageResizeNode"] = {
    "resample": {"kind": "enum", "options": ["lanczos", "bicubic", "bilinear", "nearest"]},
}
FIELD_OVERRIDES["ImageFlipNode"] = {
    "direction": {"kind": "enum", "options": ["horizontal", "vertical", "both"]},
}
FIELD_OVERRIDES["ImageConvertNode"] = {
    "format": {"kind": "enum", "options": ["keep", "png", "jpeg", "webp", "gif", "bmp", "tiff"]},
    "mode": {"kind": "enum", "options": ["keep", "RGB", "RGBA", "L"]},
}
FIELD_OVERRIDES["ImageFilterNode"] = {
    "filter": {"kind": "enum", "options": [
        "none", "blur", "gaussian_blur", "box_blur", "sharpen", "unsharp_mask",
        "edges", "edge_enhance", "contour", "emboss", "smooth", "detail",
    ]},
}
FIELD_OVERRIDES["ImageThumbnailNode"] = {
    "format": {"kind": "enum", "options": ["webp", "jpeg", "png"]},
}


# Media plan phase 4 - verified against nodes/audio_nodes.py and
# nodes/speech_to_text.py (validated in init()/configure()).
FIELD_OVERRIDES["AudioEncodeNode"] = {
    "format": {"kind": "enum", "options": ["wav", "flac", "ogg", "mp3", "m4a", "opus"]},
}
FIELD_OVERRIDES["AudioSegmentNode"] = {
    "mode": {"kind": "enum", "options": ["silence", "time"]},
}
FIELD_OVERRIDES["AudioNormalizeNode"] = {
    "mode": {"kind": "enum", "options": ["peak", "rms"]},
}
FIELD_OVERRIDES["SpeechToTextNode"] = {
    "backend": {"kind": "enum", "options": ["api", "local"]},
    "device": {"kind": "enum", "options": ["auto", "cpu", "cuda"]},
}


# Media plan phase 5 - verified against nodes/video_nodes.py.
_VIDEO_BACKEND = {"kind": "enum", "options": ["auto", "pyav", "ffmpeg"]}
FIELD_OVERRIDES["VideoDecodeNode"] = {
    "frame_format": {"kind": "enum", "options": ["jpeg", "png"]},
    "backend": _VIDEO_BACKEND,
}
FIELD_OVERRIDES["VideoFrameSampleNode"] = {
    "mode": {"kind": "enum", "options": ["every_n", "fps", "keyframes"]},
}
FIELD_OVERRIDES["VideoEncodeNode"] = {
    "format": {"kind": "enum", "options": ["mp4", "webm"]},
    "backend": _VIDEO_BACKEND,
}
FIELD_OVERRIDES["VideoInfoNode"] = {"backend": _VIDEO_BACKEND}
FIELD_OVERRIDES["VideoThumbnailNode"] = {"backend": _VIDEO_BACKEND}


def get_field_schema(type_name: str) -> dict[str, dict]:
    """Field-level overrides for one node type, global ones first so a
    type-specific override (none currently collide, but future ones
    might) takes precedence."""
    merged = dict(GLOBAL_FIELD_OVERRIDES)
    merged.update(FIELD_OVERRIDES.get(type_name, {}))
    return merged


def all_field_schemas() -> dict[str, dict[str, dict]]:
    """Field schema for every node type known to the shared registry."""
    from .registry import build_node_registry

    return {name: get_field_schema(name) for name in build_node_registry()}
