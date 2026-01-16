# dbx-cv

Computer vision utilities for RTSP stream processing, image hashing, and structural similarity analysis.

## Overview

`dbx-cv` provides tools for processing live video feeds, detecting changes between frames, and extracting visual fingerprints. It is designed for monitoring scenarios where detecting significant visual changes is required.

## Features

### Video Processing (`app.py`, `cams.py`)

High performance RTSP stream handling:

* **RTSP Reader**: Efficient frame extraction from RTSP streams using `ffmpeg`.
* **Producer/Consumer**: Asyncio based architecture for decoupled frame capture and processing.

### Fingerprinting and Change Detection

Multiple strategies for detecting visual differences:

* **HashService**: Uses perceptual hashing (`imagehash.phash`) for fast, robust image comparison.
* **SSIMService**: Implements Structural Similarity Index (SSIM) for high quality change detection.
* **Bounding Boxes**: Automatically identifies and highlights regions where changes are detected.

## Usage

```python
from dbx_cv import cams

# Run the computer vision application
# (Requires a running RTSP stream at rtsp://localhost:8554/cam)
# python -m dbx_cv.app
```

## Dependencies

* `dbx-core` (workspace dependency)
* `scikit-image`
* `imagehash`
* `opencv-python-headless`

