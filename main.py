import os
import platform
import cv2
import signal
import sys
import select
import threading
import time
import subprocess
import argparse
import psutil  # used to monitor CPU
from datetime import datetime


system_platform = platform.system()
if system_platform == "Windows":
    os.environ["OPENCV_VIDEOIO_MSMF_ENABLE_HW_TRANSFORMS"] = "0"


# UHD
TARGET_RES = (1920, 1080)
FPS = 24
MAX_CAM_CHECK = 3
USB_INIT_DELAY = 0.5
FFMPEG_PATH = "ffmpeg"
OUTPUT_FOLDER = "output"
BOARD_ID='D'
VELOCITA='veryfast' #'ultrafast' oppure 'veryfast'


CPU_THRESHOLD = 80  # percentage above which to warn


CAM_AUDIO_MAP = {
    0: "hw:2,0",
    2: "hw:3,0",
}


"""
High quality (H.264 with CPU monitor):
→ saves .mp4, more compressed, higher CPU usage.
→ every 5s prints CPU load.

Low CPU load (MJPEG copy):
→ saves .avi files, huge, almost zero CPU.
→ no CPU monitor (not needed).
"""


class CameraSystem:
    def __init__(self, mode="hq"):
        self.mode = mode  # "hq" = H.264, "fast" = MJPEG copy
        self.running = True
        self.cameras = []
        self.ffmpeg_processes = []
        self.api_preference = cv2.CAP_V4L2 if system_platform != "Windows" else cv2.CAP_MSMF


        # output folder with timestamp
        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.output_subfolder = os.path.join(OUTPUT_FOLDER, now_str)
        os.makedirs(self.output_subfolder, exist_ok=True)


        signal.signal(signal.SIGTERM, self.signal_handler)
        self.initialize_cameras()
        self.preview_cameras()
        self.start_recording()


    def initialize_cameras(self):
        print("[SYSTEM] Scanning for connected cameras...")
        for index in range(MAX_CAM_CHECK):
            try:
                cap = cv2.VideoCapture(index, self.api_preference)
                if not cap.isOpened():
                    cap.release()
                    continue
                cap.set(cv2.CAP_PROP_FRAME_WIDTH, TARGET_RES[0])
                cap.set(cv2.CAP_PROP_FRAME_HEIGHT, TARGET_RES[1])
                cap.set(cv2.CAP_PROP_FPS, FPS)
                time.sleep(0.5)
                ret, frame = cap.read()
                if not ret or frame is None:
                    cap.release()
                    continue
                self.cameras.append(index)
                cap.release()
                print(f"[STATUS] Camera {index} detected and ready.")
                time.sleep(USB_INIT_DELAY)
            except Exception as e:
                print(f"[ERROR] Checking Camera {index}: {str(e)}")


        if not self.cameras:
            print("[ERROR] No cameras detected.")
            sys.exit(1)


    


    def build_ffmpeg_command(self, cam_index, audio_device):
        video_device = f"/dev/video{cam_index}"


        # Force MJPEG video format for both cameras (more stable)
        input_format = "mjpeg"


        # Set same FPS for all
        framerate = str(FPS)


        if self.mode == "fast":
            output_path = os.path.join(self.output_subfolder, f"camera_{BOARD_ID}_{cam_index}.avi")
            cmd = [
                FFMPEG_PATH,
                '-y',
                '-f', 'v4l2',
                '-thread_queue_size', '2048',
                '-input_format', input_format,
                '-framerate', framerate,
                '-video_size', f"{TARGET_RES[0]}x{TARGET_RES[1]}",
                '-i', video_device,
                '-f', 'alsa',
                '-thread_queue_size', '2048',
                '-ac', '2',
                '-ar', '48000',
                '-i', audio_device,
                '-c:v', 'copy',
                '-vf', 'transpose=2,transpose=2',
                '-c:a', 'aac',
                '-b:a', '192k',
                output_path
            ]
        else:  # HQ mode
            output_path = os.path.join(self.output_subfolder, f"camera_{BOARD_ID}{cam_index}%02d.mp4") #ho aggiornato il nome con id scheda @fg
            cmd = [
                FFMPEG_PATH,
                '-fflags', '+genpts',
                '-y',
                '-f', 'v4l2',
                '-thread_queue_size', '2048',
                '-input_format', input_format,
                '-framerate', framerate,
                '-video_size', f"{TARGET_RES[0]}x{TARGET_RES[1]}",
                '-i', video_device,
                '-f', 'alsa',
                '-thread_queue_size', '2048',
                '-ac', '2',
                '-ar', '48000',
                '-i', audio_device,
                '-c:v', 'libx264',  # H.264 AVC1
                '-preset', VELOCITA,
                '-crf', '23',
                '-r', framerate,  # force output to same FPS
                '-vf', 'transpose=2,transpose=2',
                '-c:a', 'aac',
                '-b:a', '192k',
                '-pix_fmt', 'yuv420p',
                "-f", "segment",  # aggiunta segmentazione ogno 30minuti @fg
                "-segment_time", "1800", # seg @fg
                "-reset_timestamps", "1", #seg @fg
                "-segment_format", "mp4", #seg coerente con il formato @fg
                # ho tolto '-movflags', '+faststart', @fg
                output_path
            ]
        return cmd



    def ffmpeg_worker(self, cam_index, audio_device):
        cmd = self.build_ffmpeg_command(cam_index, audio_device)
        print(f"[STATUS] Starting FFmpeg for Camera {cam_index} in mode {self.mode.upper()}...")
        
        log_out_path = os.path.join(self.output_subfolder, f"ffmpeg_camera_{BOARD_ID}{cam_index}_stdout.log") # ho aggiornato il nome con la scheda @FG
        log_err_path = os.path.join(self.output_subfolder, f"ffmpeg_camera_{BOARD_ID}{cam_index}_stderr.log")


        with open(log_out_path, "wb") as log_out_file, open(log_err_path, "wb") as log_err_file:
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
            self.ffmpeg_processes.append(proc)


            def log_stream(stream, log_file):
                while True:
                    line = stream.readline()
                    if not line:
                        break
                    log_file.write(line)
                    log_file.flush()


            stdout_thread = threading.Thread(target=log_stream, args=(proc.stdout, log_out_file), daemon=True)
            stderr_thread = threading.Thread(target=log_stream, args=(proc.stderr, log_err_file), daemon=True)
            stdout_thread.start()
            stderr_thread.start()


            while self.running:
                retcode = proc.poll()
                if retcode is not None:
                    print(f"[WARNING] FFmpeg process for Camera {cam_index} exited with code {retcode}")
                    self.running = False
                    break
                time.sleep(0.5)


            stdout_thread.join()
            stderr_thread.join()


    def cpu_monitor(self):
        while self.running and self.mode == "hq":
            cpu_usage = psutil.cpu_percent(interval=5)
            if cpu_usage > CPU_THRESHOLD:
                print(f"[⚠ WARNING] High CPU usage detected: {cpu_usage}% (threshold {CPU_THRESHOLD}%). Consider using --mode fast.")
            else:
                print(f"[INFO] CPU usage OK: {cpu_usage}%")
            time.sleep(1)


    def preview_cameras(self):
        print("[SYSTEM] Starting camera preview...")
        caps = []
        for cam_index in self.cameras:
            cap = cv2.VideoCapture(cam_index, self.api_preference)
            cap.set(cv2.CAP_PROP_FRAME_WIDTH, TARGET_RES[0])
            cap.set(cv2.CAP_PROP_FRAME_HEIGHT, TARGET_RES[1])
            cap.set(cv2.CAP_PROP_FPS, FPS)
            caps.append((cam_index, cap))


        print("[INFO] Press ENTER or 'q' to close preview and start recording.")
        while True:
            frames = []
            for cam_index, cap in caps:
                ret, frame = cap.read()
                if ret:
                    # Resize to 640x360 (modifiable)
                    frame = cv2.resize(frame, (640, 360))
                    frame = cv2.rotate(frame, cv2.ROTATE_180)
                    frames.append(frame)


            if frames:
                if len(frames) == 2:
                    combined = cv2.hconcat(frames)  # side-by-side
                else:
                    combined = frames[0]
                cv2.imshow("Camera Preview", combined)


            key = cv2.waitKey(1) & 0xFF
            if key == ord('q') or key == 13:  # 'q' or Enter
                break


        for _, cap in caps:
            cap.release()
        cv2.destroyAllWindows()
        print("[SYSTEM] Preview ended, starting recording...")



    def start_recording(self):
        print(f"[SYSTEM] Starting recording on {len(self.cameras)} cameras with FFmpeg ({self.mode.upper()} mode)...")

        if self.mode == "hq":
            threading.Thread(target=self.cpu_monitor, daemon=True).start()

        self.threads = []
        for cam_index in self.cameras:
            audio_dev = CAM_AUDIO_MAP.get(cam_index, "default")
            t = threading.Thread(target=self.ffmpeg_worker, args=(cam_index, audio_dev), daemon=True)
            t.start()
            self.threads.append(t)
            time.sleep(0.5)

        print("[SYSTEM] Recording started. Press 'q' + Enter to stop.")


        try:
            while self.running:
                # Non-blocking input check
                if sys.stdin in select.select([sys.stdin], [], [], 0)[0]:
                    line = sys.stdin.readline().strip()
                    if line.lower() == 'q':
                        print("[SYSTEM] Stop command received ('q'). Stopping...")
                        self.signal_handler(None, None)
                        break


                for i, proc in enumerate(self.ffmpeg_processes):
                    retcode = proc.poll()
                    if retcode is not None:
                        print(f"[WARNING] FFmpeg process for Camera {self.cameras[i]} terminated with code {retcode}")
                        self.running = False
                        break


                time.sleep(0.5)
        except KeyboardInterrupt:
            self.signal_handler(None, None)


    def signal_handler(self, sig, frame):
        import signal as py_signal
        print("\n[SYSTEM] Gracefully stopping all FFmpeg processes...")
        self.running = False


        for i, proc in enumerate(self.ffmpeg_processes):
            if proc.poll() is None:
                print(f"[STATUS] Asking FFmpeg (Camera {self.cameras[i]}) to stop cleanly...")
                try:
                    # Send 'q' to stdin to terminate safely (writes moov atom)
                    proc.stdin.write(b"q\n")
                    proc.stdin.flush()
                    proc.wait() #ho tolto il timeout @FG
                except Exception as e:
                    print(f"[WARNING] Graceful stop failed for Camera {self.cameras[i]}: {e}")
                    print(f"[STATUS] Sending SIGINT to FFmpeg process for Camera {self.cameras[i]}...")
                    try:
                        proc.send_signal(py_signal.SIGINT)
                        proc.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        print(f"[WARNING] FFmpeg process for Camera {self.cameras[i]} unresponsive, killing...")
                        proc.kill()
                        proc.wait()


        print("[SYSTEM] All FFmpeg processes terminated cleanly.")
        sys.exit(0)


if __name__ == "__main__":
    # Select recording mode here
    # "fast" = MJPEG copy (huge files, low CPU)
    # "hq"   = H.264 encode (smaller files, high CPU)
    MODE = "hq"
    CameraSystem(mode=MODE)
