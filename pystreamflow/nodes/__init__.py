from .input_file import FileInputNode
from .input_directory import DirectoryInputNode
from .media_file_input import MediaFileInputNode
from .media_file_output import MediaFileOutputNode
from .input_web import WebInputNode
from .input_api import ApiInputNode
from .output_api import ApiOutputNode
from .llm_lmstudio import LMStudioNode
from .input_process import ProcessInputNode
from .input_shell import ShellInputNode
from .input_socket import SocketInputNode
from .input_python import PythonScriptInputNode
from .input_generator import GeneratorInputNode
from .input_log import LogInputNode
from .json_input import JSONInputNode
from .input_script import ScriptInputNode
from .mqtt_input import MQTTInputNode
from .mqtt_output import MQTTOutputNode
from .output_file import FileOutputNode
from .output_process import ProcessOutputNode
from .output_socket import SocketOutputNode
from .output_log import LogOutputNode
from .json_output import JSONOutputNode
from .modifier_grep import GrepNode
from .modifier_merge import MergeNode
from .modifier_fork import ForkNode
from .modifier_template import TemplateNode
from .modifier_json import JSONExtractNode
from .modifier_script import ScriptNode
from .json_modify import JSONModifyNode
from .html_scraper_node import HTMLScraperNode
from .base64_decode_node import Base64DecodeNode
from .base64_encode_node import Base64EncodeNode
from .user_prompt_node import UserPromptNode
from .user_input_node import UserInputNode
from .rolling_window_buffer_node import RollingWindowBufferNode
from .scripted_output import ScriptedOutputNode
from .web_output import WebOutputNode
from .web_output_json import WebOutputJSONNode
from .logic_and import AndNode
from .logic_or import OrNode
from .logic_not import NotNode
from .logic_compare import CompareNode
from .logic_math import MathNode
from .logic_xor import XorNode
from .logic_nand import NandNode
from .logic_nor import NorNode
from .logic_xnor import XnorNode
from .numeric_add import NumericAddNode
from .numeric_sub import NumericSubNode
from .numeric_mul import NumericMulNode
from .numeric_div import NumericDivNode
from .numeric_mod import NumericModNode
from .numeric_pow import NumericPowNode
from .numeric_min import NumericMinNode
from .numeric_max import NumericMaxNode
from .numeric_clamp import NumericClampNode
from .numeric_round import NumericRoundNode
from .numeric_abs import NumericAbsNode
from .text_upper import TextUpperNode
from .text_lower import TextLowerNode
from .text_trim import TextTrimNode
from .text_replace import TextReplaceNode
from .text_substring import TextSubstringNode
from .text_reverse import TextReverseNode
from .text_title import TextTitleNode
from .text_strip import TextStripNode
from .text_split import TextSplitNode
from .text_join import TextJoinNode
from .encoding_convert import EncodingConvertNode
from .subgraph import SubgraphNode
from .modifier_split_by_value import SplitByValueNode
from .mcp_client_node import MCPClientNode
from .cron_node import CronNode
from .sync_barrier_node import SyncBarrierNode
from .queue_gate_node import QueueGateNode
from .led_activity_node import LedActivityNode
from .round_robin_node import RoundRobinNode
from .http_post_node import HttpPostNode
from .line_splitter import LineSplitterNode
from .tokenizer import TokenizerNode
from .line_buffer import LineBufferNode
from .trim_string import TrimStringNode
from .url_input import UrlInputNode
from .trigger import TriggerNode, TimerTriggerNode, TriggerOnNode, TriggerOffNode, TriggerPauseNode
from .trigger_advanced import TriggerIfNode, TriggerThresholdNode, TriggerDebounceNode, TriggerPulseNode, TriggerToggleNode
from .timer import TimerNode
from .stack_node import StackNode
from .queue_fifo_node import FIFOQueueNode
from .queue_lifo_node import LIFOQueueNode
from .clock_node import ClockNode
from .list_strings import ListStringsNode
from .display import DisplayNode
from .table import TableNode
from .value_constant import ConstantValueNode

__all__ = [
    "FileInputNode","DirectoryInputNode","WebInputNode","ApiInputNode","ApiOutputNode","LMStudioNode",
    "StackNode","FIFOQueueNode","LIFOQueueNode","ClockNode",
    "HTMLScraperNode",
    "MediaFileInputNode","MediaFileOutputNode",
    "Base64DecodeNode",
    "Base64EncodeNode",
    "UserPromptNode",
    "UserInputNode",
    "RollingWindowBufferNode",
    "ProcessInputNode","ShellInputNode","SocketInputNode","PythonScriptInputNode","GeneratorInputNode","LogInputNode","JSONInputNode","ScriptInputNode","MQTTInputNode","MQTTOutputNode",
    "FileOutputNode","ProcessOutputNode","SocketOutputNode","LogOutputNode","JSONOutputNode",
    "GrepNode","MergeNode","ForkNode","TemplateNode","JSONExtractNode","ScriptNode","JSONModifyNode","ScriptedOutputNode","WebOutputNode","WebOutputJSONNode",
    "AndNode","OrNode","NotNode","CompareNode","MathNode","XorNode","NandNode","NorNode","XnorNode",
    "NumericAddNode","NumericSubNode","NumericMulNode","NumericDivNode","NumericModNode","NumericPowNode","NumericMinNode","NumericMaxNode","NumericClampNode","NumericRoundNode","NumericAbsNode",
    "TextUpperNode","TextLowerNode","TextTrimNode","TextReplaceNode","TextSubstringNode","TextReverseNode","TextTitleNode","TextStripNode","TextSplitNode","TextJoinNode",
    "EncodingConvertNode",
    "SubgraphNode",
    "SplitByValueNode","MCPClientNode","CronNode","SyncBarrierNode","QueueGateNode","LedActivityNode","RoundRobinNode","HttpPostNode",
    "LineSplitterNode","TokenizerNode","LineBufferNode",
    "TrimStringNode",
    "UrlInputNode",
    "TriggerNode","TimerNode","TimerTriggerNode","TriggerOnNode","TriggerOffNode","TriggerPauseNode",
    "TriggerIfNode","TriggerThresholdNode","TriggerDebounceNode","TriggerPulseNode","TriggerToggleNode",
    "ListStringsNode","DisplayNode","TableNode",
    "ConstantValueNode",
]

# Node types whose optional dependency (media plan phase 3: the
# `pystreamflow[image]` extra, i.e. Pillow) isn't installed. They are not
# in __all__ - so not in the registry and not creatable - and
# GET /node-availability reports them with an install hint so the editor
# can grey them out instead of offering nodes that can't run.
IMAGE_NODE_TYPES = (
    "ImageDecodeNode", "ImageResizeNode", "ImageCropNode", "ImageRotateNode", "ImageFlipNode",
    "ImageConvertNode", "ImageFilterNode", "ImageInfoNode", "ImageThumbnailNode",
)
UNAVAILABLE_NODE_TYPES: dict[str, str] = {}
try:
    from . import image_nodes as _image_nodes
except ImportError as _e:  # Pillow missing
    for _name in IMAGE_NODE_TYPES:
        UNAVAILABLE_NODE_TYPES[_name] = f"needs Pillow: pip install 'pystreamflow[image]' ({_e})"
else:
    for _name in IMAGE_NODE_TYPES:
        globals()[_name] = getattr(_image_nodes, _name)
    __all__ += list(IMAGE_NODE_TYPES)
