#!/usr/bin/env python3
"""常驻 ASR 服务：silk / wav 音频 → 文字。

给 dsh-qq-bridge 的 `qq_bridge_transcribe` 工具用（机器人需要"听"的时候才调）。

为什么必须常驻：faster-whisper 冷加载模型要 4 秒，按需每次加载就完全没法用。
常驻后一条 10 秒语音约 0.6 秒（CPU int8，fw-small）。

接口（只监听 127.0.0.1）：

    GET  /health
      -> {"ok":true,"model":"fw-small","device":"cpu","loaded":true}

    POST /transcribe?fmt=silk&lang=zh     body = 音频二进制（silk 或 wav）
      -> {"ok":true,"text":"...","seconds":6.0,"elapsed_ms":800}

    fmt: silk（QQ/微信语音，腾讯 SILK v3）| wav（视频音轨抽出来直接给）
"""
import io
import json
import os
import sys
import tempfile
import time
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# 默认用 faster-whisper 的模型名 small（首次运行自动从 HF 下载）；
# 想用本地已下好的目录（省流量、离线）就用 ASR_MODEL=/path/to/model 覆盖。
MODEL_DIR = os.environ.get('ASR_MODEL', 'small')
DEVICE = os.environ.get('ASR_DEVICE', 'cpu')
COMPUTE_TYPE = os.environ.get('ASR_COMPUTE', 'int8')
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8799
MAX_BYTES = 32 * 1024 * 1024

_model = None


def get_model():
    """进程内单例：第一次调用加载，之后所有请求复用。"""
    global _model
    if _model is None:
        from faster_whisper import WhisperModel
        _model = WhisperModel(MODEL_DIR, device=DEVICE, compute_type=COMPUTE_TYPE)
    return _model


def silk_to_wav_bytes(silk: bytes) -> bytes:
    """腾讯 SILK v3 → 24kHz 单声道 16bit WAV。

    抄 AstrBot 的 tencent_record_helper.tencent_silk_to_wav：
    QQ 的语音负载前面可能带一个 0x02 / 0x03 标记，不剥掉 pysilk 会解不动。
    """
    import pysilk
    data = silk
    if data.startswith((b'\x02#!SILK_V3', b'\x03#!SILK_V3')):
        data = data[1:]
    decoded = io.BytesIO()
    pysilk.decode(io.BytesIO(data), decoded, 24000)
    pcm = decoded.getvalue()
    buf = io.BytesIO()
    with wave.open(buf, 'wb') as handle:
        handle.setnchannels(1)
        handle.setsampwidth(2)
        handle.setframerate(24000)
        handle.writeframes(pcm)
    return buf.getvalue()


def transcribe(audio: bytes, fmt: str, lang: str) -> dict:
    wav = silk_to_wav_bytes(audio) if fmt == 'silk' else audio
    tmp = os.path.join(tempfile.gettempdir(), f'asr-{os.getpid()}-{int(time.time() * 1000)}.wav')
    with open(tmp, 'wb') as handle:
        handle.write(wav)
    try:
        model = get_model()
        started = time.time()
        segments, info = model.transcribe(
            tmp, language=(lang or None), beam_size=1, vad_filter=True,
        )
        text = ''.join(segment.text for segment in segments).strip()
        return {
            'ok': True,
            'text': text,
            'seconds': round(float(getattr(info, 'duration', 0) or 0), 2),
            'elapsed_ms': int((time.time() - started) * 1000),
        }
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    server_version = 'dsh-qq-asr/1.0'

    def log_message(self, fmt, *args):  # noqa: A003  别把每条请求刷进 journal
        return

    def _json(self, code: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(code)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        if urlparse(self.path).path == '/health':
            self._json(200, {
                'ok': True,
                'model': os.path.basename(MODEL_DIR),
                'device': DEVICE,
                'compute': COMPUTE_TYPE,
                'loaded': _model is not None,
            })
        else:
            self._json(404, {'ok': False, 'error': 'not found'})

    def do_POST(self):  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != '/transcribe':
            self._json(404, {'ok': False, 'error': 'not found'})
            return
        try:
            length = int(self.headers.get('Content-Length') or 0)
        except ValueError:
            length = 0
        if length <= 0:
            self._json(400, {'ok': False, 'error': '空请求体'})
            return
        if length > MAX_BYTES:
            self._json(413, {'ok': False, 'error': f'音频太大（{length} 字节）'})
            return
        audio = self.rfile.read(length)
        query = parse_qs(parsed.query)
        fmt = (query.get('fmt', ['wav'])[0] or 'wav').lower()
        lang = (query.get('lang', ['zh'])[0] or 'zh').lower()
        try:
            self._json(200, transcribe(audio, fmt, lang))
        except Exception as exc:  # 解码/模型失败都只回错误，服务继续活着
            self._json(500, {'ok': False, 'error': str(exc)[:300]})


def main() -> None:
    started = time.time()
    get_model()  # 启动即加载，别让第一条语音等 4 秒
    print(f'asr ready on 127.0.0.1:{PORT} model={MODEL_DIR} device={DEVICE} '
          f'load={time.time() - started:.1f}s', flush=True)
    ThreadingHTTPServer(('127.0.0.1', PORT), Handler).serve_forever()


if __name__ == '__main__':
    main()
