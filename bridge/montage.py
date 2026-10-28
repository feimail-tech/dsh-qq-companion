#!/usr/bin/env python3
"""动图 / 视频 → 3x3 九宫格拼图，供视觉模型理解。

思路抄 AstrBot 的 animation montage（astrbot/core/utils/media_utils.py）：
均匀取帧（含首尾）、白底、每格等比缩放、整张不超过预算。
不抄 MaiBot 的横向长条拼接——超长图发进模型前会被归一化压扁，每帧糊成一团。

用法（node 侧 spawn 调用，参数走单个 JSON 字符串）：
    python3 montage.py '{"kind":"image","src":"/a.gif","out":"/a-3x3.jpg"}'
    python3 montage.py '{"kind":"video","src":"/a.mp4","out":"/a-3x3.jpg","maxSeconds":60}'

stdout 只输出一行 JSON：
    {"ok":true,"frames":9,"width":1268,"height":719,"duration":104.8,"out":"..."}
    {"ok":false,"reason":"not-animated"}     # 静态图，不用拼
    {"ok":false,"error":"..."}
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

GRID = 3
FRAME_COUNT = GRID * GRID
CELL_MAX = 480          # 每格最长边；3*480=1440，跟一次普通看图的 token 量差不多（实测 1200 token）
JPEG_Q = 82
MAX_SECONDS_DEFAULT = 60

FFMPEG = os.environ.get('MONTAGE_FFMPEG') or 'ffmpeg'
FFPROBE = os.environ.get('MONTAGE_FFPROBE') or 'ffprobe'


def _even_indices(total, count):
    """均匀取帧，含首尾，去重（同 AstrBot 的 _even_frame_indices）。"""
    count = min(count, total)
    if count <= 1:
        return [0]
    return sorted({round(i * (total - 1) / (count - 1)) for i in range(count)})


def _tile(images, out_path):
    """把若干帧拼成 3x3 白底九宫格，返回 (宽, 高, 帧数)。"""
    if not images:
        raise RuntimeError('没有可用帧')
    images = images[:FRAME_COUNT]
    w, h = images[0].size
    if w <= 0 or h <= 0:
        raise RuntimeError('帧尺寸为 0')
    scale = min(1.0, CELL_MAX / max(w, h))
    cell_w = max(1, int(w * scale))
    cell_h = max(1, int(h * scale))
    from PIL import Image
    canvas = Image.new('RGB', (cell_w * GRID, cell_h * GRID), (255, 255, 255))
    for index, image in enumerate(images):
        if image.size != (cell_w, cell_h):
            image = image.resize((cell_w, cell_h), Image.Resampling.LANCZOS)
        canvas.paste(image, ((index % GRID) * cell_w, (index // GRID) * cell_h))
    canvas.save(out_path, 'JPEG', quality=JPEG_Q, optimize=True)
    return cell_w * GRID, cell_h * GRID, len(images)


def animation_frames(path):
    """动图取 9 帧；静态图返回 None。"""
    from PIL import Image, ImageOps
    with Image.open(path) as image:
        total = getattr(image, 'n_frames', 1)
        if total <= 1:
            return None
        # APNG 的 default_image 是封面不是动画帧
        first = 1 if image.info.get('default_image', False) else 0
        frames = []
        for index in _even_indices(total - first, FRAME_COUNT):
            image.seek(first + index)
            try:
                oriented = ImageOps.exif_transpose(image)
            except Exception:
                oriented = image
            frames.append(oriented.convert('RGB').copy())
        return frames


def video_frames(path, tmpdir, max_seconds):
    """用 ffmpeg 均匀抽 9 帧，返回 (帧路径列表, 原始时长)。"""
    probe = subprocess.run(
        [FFPROBE, '-v', 'error', '-select_streams', 'v:0',
         '-show_entries', 'format=duration', '-of', 'json', path],
        capture_output=True, text=True, timeout=25,
    )
    try:
        duration = float(json.loads(probe.stdout or '{}').get('format', {}).get('duration') or 0)
    except Exception:
        duration = 0.0
    if duration <= 0:
        raise RuntimeError('读不到视频时长（可能不是视频文件）')
    span = min(duration, float(max_seconds or MAX_SECONDS_DEFAULT))
    if span <= 0:
        raise RuntimeError('视频时长无效')
    # 均匀 9 帧：按 span 定 fps，取前 9 帧
    fps = FRAME_COUNT / span
    pattern = os.path.join(tmpdir, 'f%02d.jpg')
    subprocess.run(
        [FFMPEG, '-y', '-v', 'error', '-t', f'{span:.3f}', '-i', path,
         '-vf', f'fps={fps:.6f},scale=640:-2:force_original_aspect_ratio=decrease',
         '-frames:v', str(FRAME_COUNT), '-q:v', '3', pattern],
        capture_output=True, timeout=120,
    )
    files = sorted(
        os.path.join(tmpdir, name) for name in os.listdir(tmpdir)
        if name.startswith('f') and name.endswith('.jpg')
    )
    return files, duration


def main():
    try:
        args = json.loads(sys.argv[1]) if len(sys.argv) > 1 else {}
    except Exception as exc:
        print(json.dumps({'ok': False, 'error': f'参数不是 JSON：{exc}'}, ensure_ascii=False))
        return
    kind = args.get('kind') or 'image'
    src = args.get('src') or ''
    out = args.get('out') or ''
    max_seconds = args.get('maxSeconds') or MAX_SECONDS_DEFAULT
    if not src or not out:
        print(json.dumps({'ok': False, 'error': 'src / out 必填'}, ensure_ascii=False))
        return
    if not os.path.exists(src):
        print(json.dumps({'ok': False, 'error': f'源文件不存在：{src}'}, ensure_ascii=False))
        return

    try:
        from PIL import Image  # noqa: F401  先确认依赖在
    except Exception as exc:
        print(json.dumps({'ok': False, 'error': f'PIL 不可用：{exc}'}, ensure_ascii=False))
        return

    tmpdir = tempfile.mkdtemp(prefix='montage-')
    try:
        if kind == 'image':
            frames = animation_frames(src)
            if not frames:
                print(json.dumps({'ok': False, 'reason': 'not-animated'}, ensure_ascii=False))
                return
            width, height, count = _tile(frames, out)
            print(json.dumps({'ok': True, 'frames': count, 'width': width,
                              'height': height, 'out': out}, ensure_ascii=False))
        elif kind == 'video':
            files, duration = video_frames(src, tmpdir, max_seconds)
            images = []
            for path in files:
                try:
                    with Image.open(path) as image:
                        images.append(image.convert('RGB').copy())
                except Exception:
                    continue
            width, height, count = _tile(images, out)
            print(json.dumps({'ok': True, 'frames': count, 'width': width,
                              'height': height, 'duration': duration, 'out': out},
                             ensure_ascii=False))
        else:
            print(json.dumps({'ok': False, 'error': f'未知 kind：{kind}'}, ensure_ascii=False))
    except subprocess.TimeoutExpired:
        print(json.dumps({'ok': False, 'error': 'ffmpeg / ffprobe 超时'}, ensure_ascii=False))
    except Exception as exc:
        print(json.dumps({'ok': False, 'error': str(exc)}, ensure_ascii=False))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    main()
