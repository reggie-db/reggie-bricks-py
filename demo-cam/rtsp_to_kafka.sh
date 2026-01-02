#!/bin/bash
#
# RTSP to Kafka Stream Publisher
#
# Captures an RTSP stream using ffmpeg, converts it to 1fps at 480p resolution,
# and publishes JPEG frames to a Kafka topic via rpk (Redpanda Keeper CLI).
#
# Usage: ./rtsp_to_kafka.sh <stream-id> <rtsp-url>
# Example: ./rtsp_to_kafka.sh cam-lobby rtsp://192.168.1.100:554/stream
#

set -euo pipefail

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
KAFKA_BROKERS="${KAFKA_BROKERS:-kafka.lfpconnect.io:443}"
KAFKA_TOPIC="${KAFKA_TOPIC:-source}"
FPS="${FPS:-1}"
RESOLUTION="${RESOLUTION:-480}"
JPEG_QUALITY="${JPEG_QUALITY:-5}"

# ---------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------
if [ $# -lt 2 ]; then
    echo "Usage: $0 <stream-id> <rtsp-url>"
    echo ""
    echo "Arguments:"
    echo "  stream-id   Identifier for this stream (e.g., cam-lobby)"
    echo "  rtsp-url    Full RTSP URL (e.g., rtsp://192.168.1.100:554/stream)"
    echo ""
    echo "Environment variables:"
    echo "  KAFKA_BROKERS  Kafka broker address (default: kafka.lfpconnect.io:443)"
    echo "  KAFKA_TOPIC    Target topic (default: source)"
    echo "  FPS            Frames per second (default: 1)"
    echo "  RESOLUTION     Target height in pixels (default: 480)"
    echo "  JPEG_QUALITY   JPEG quality 1-31, lower is better (default: 5)"
    exit 1
fi

STREAM_ID="$1"
RTSP_URL="$2"

echo "============================================"
echo "RTSP to Kafka Stream Publisher"
echo "============================================"
echo "Stream ID:    $STREAM_ID"
echo "RTSP URL:     $RTSP_URL"
echo "Kafka Broker: $KAFKA_BROKERS"
echo "Kafka Topic:  $KAFKA_TOPIC"
echo "FPS:          $FPS"
echo "Resolution:   ${RESOLUTION}p"
echo "============================================"

# ---------------------------------------------------------
# Temp directory for frame storage
# ---------------------------------------------------------
FRAME_DIR=$(mktemp -d)
cleanup() {
    echo ""
    echo "Cleaning up..."
    [ -n "${FFMPEG_PID:-}" ] && kill "$FFMPEG_PID" 2>/dev/null || true
    rm -rf "$FRAME_DIR"
    echo "Done."
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------
# Start ffmpeg to capture RTSP and output JPEG frames
# ---------------------------------------------------------
echo "Starting ffmpeg capture..."

ffmpeg -hide_banner -loglevel error \
    -fflags +discardcorrupt \
    -err_detect ignore_err \
    -rtsp_transport tcp \
    -i "$RTSP_URL" \
    -vf "fps=${FPS},scale=-1:${RESOLUTION}" \
    -f image2 \
    -q:v "$JPEG_QUALITY" \
    -strftime 0 \
    "${FRAME_DIR}/frame_%08d.jpg" &
FFMPEG_PID=$!

# Allow ffmpeg to initialize
sleep 2

if ! kill -0 "$FFMPEG_PID" 2>/dev/null; then
    echo "ERROR: ffmpeg failed to start. Check RTSP URL and network connectivity."
    exit 1
fi

echo "ffmpeg started (PID: $FFMPEG_PID)"
echo "Publishing frames to Kafka..."


# ---------------------------------------------------------
# Monitor for new frames and publish to Kafka
# ---------------------------------------------------------
LAST_FRAME_NUM=0
FRAMES_SENT=0

while kill -0 "$FFMPEG_PID" 2>/dev/null; do
    # Find and process new frames in order
    for frame_file in "$FRAME_DIR"/frame_*.jpg; do
        [ -f "$frame_file" ] || continue
        
        # Extract frame number from filename
        filename=$(basename "$frame_file" .jpg)
        frame_num="${filename#frame_}"
        # Remove leading zeros for numeric comparison
        frame_num=$((10#$frame_num))
        
        if [ "$frame_num" -gt "$LAST_FRAME_NUM" ]; then
            # Get image dimensions using file command or default to resolution
            if command -v identify &>/dev/null; then
                dimensions=$(identify -format "%wx%h" "$frame_file" 2>/dev/null || echo "")
                WIDTH="${dimensions%x*}"
                HEIGHT="${dimensions#*x}"
            fi
            # Fallback to configured resolution if identify fails
            WIDTH="${WIDTH:-854}"
            HEIGHT="${HEIGHT:-$RESOLUTION}"

            # Encode frame as base64
            BASE64_DATA=$(base64 -i "$frame_file")
            TIMESTAMP=$(date +%s%3N)
            MESSAGE_ID="${STREAM_ID}_${TIMESTAMP}"
            
            # Print headers and payload summary
            echo ""
            echo "--- Frame $frame_num ---"
            echo "Kafka Headers:"
            echo "  stream_id: $STREAM_ID"
            echo "  width: $WIDTH"
            echo "  height: $HEIGHT"
            echo "  message_id: $MESSAGE_ID"
            echo "Value (base64): ${#BASE64_DATA} chars"
            
            # Send to Kafka with headers and TLS (port 443 requires it)
            if echo "$BASE64_DATA" | timeout 10 rpk topic produce "$KAFKA_TOPIC" \
                --brokers "$KAFKA_BROKERS" \
                --tls-enabled \
                -H "stream_id:$STREAM_ID" \
                -H "width:$WIDTH" \
                -H "height:$HEIGHT" \
                -H "message_id:$MESSAGE_ID"; then
                FRAMES_SENT=$((FRAMES_SENT + 1))
                echo "Status: SENT (total: $FRAMES_SENT)"
            else
                echo "Status: FAILED (exit code: $?)"
            fi
            
            # Clean up processed frame
            rm -f "$frame_file"
            LAST_FRAME_NUM=$frame_num
        fi
    done
    
    # Brief sleep to avoid busy loop
    sleep 0.1
done

echo ""
echo "Stream ended. Total frames sent: $FRAMES_SENT"
