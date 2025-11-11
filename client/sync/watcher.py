from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import os

class SyncEventHandler(FileSystemEventHandler):
    def __init__(self, sync_manager, debounce_ms=400):
        self.sync_manager = sync_manager
        self.debounce_ms = debounce_ms
        self._last_event_ts = {}

    def _should_process(self, path):
        # 忽略常见临时文件
        name = os.path.basename(path)
        if name.startswith('~$') or name.endswith(('.swp', '.tmp', '.temp')):
            return False
        now = time.time() * 1000
        last = self._last_event_ts.get(path, 0)
        if now - last < self.debounce_ms:
            return False
        self._last_event_ts[path] = now
        return True

    def on_created(self, event):
        if event.is_directory:
            self.sync_manager.create_folder(event.src_path)
        else:
            if self._should_process(event.src_path):
                self.sync_manager.upload_file(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            if self._should_process(event.src_path):
                self.sync_manager.upload_file(event.src_path)

    def on_deleted(self, event):
        if self._should_process(event.src_path):
            self.sync_manager.delete_file(event.src_path)

    def on_moved(self, event):
        # 对移动/重命名不做去抖，多数情况下一次事件即可
        self.sync_manager.rename_file(event.src_path, event.dest_path)


class FolderWatcher:
    def __init__(self, folder_path, sync_manager):
        self.folder_path = folder_path
        self.sync_manager = sync_manager
        self.observer = Observer()

    def start(self):
        event_handler = SyncEventHandler(self.sync_manager)
        self.observer.schedule(event_handler, self.folder_path, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()
