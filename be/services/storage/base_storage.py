# services/storage/base_storage.py
from abc import ABC, abstractmethod

class BaseStorage(ABC):
    @abstractmethod
    def upload_file(self, user_id, file_obj, folder):
        pass

    @abstractmethod
    def download_file(self, user_id, filename, folder):
        pass

    @abstractmethod
    def list_files(self, user_id, folder):
        pass
    @abstractmethod
    def delete_file(self, user_id, filename, folder):
        pass

    @abstractmethod
    def create_folder(self, user_id, foldername):
        pass

    @abstractmethod
    def rename_file(self, user_id, old_path, new_path):
        """Rename or move a file within user's namespace. Paths are relative to user root."""
        pass

    @abstractmethod
    def create_archive(self, user_id, folder, archive_name):
        """Create a zip archive from a folder. Return archive relative path."""
        pass

    @abstractmethod
    def extract_archive(self, user_id, archive_path, dest_folder):
        """Extract a zip archive into destination folder. Return True/False."""
        pass
