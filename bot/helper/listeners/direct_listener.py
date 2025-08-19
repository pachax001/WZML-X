from time import sleep

from bot import LOGGER, aria2
from bot.helper.ext_utils.bot_utils import async_to_sync, sync_to_async


class DirectListener:
    def __init__(self, foldername, total_size, path, listener, a2c_opt):
        self.__path = path
        self.__listener = listener
        self.__is_cancelled = False
        self.__a2c_opt = a2c_opt
        self.__proc_bytes = 0
        self.__failed = 0
        self.task = None
        self.name = foldername
        self.total_size = total_size

    @property
    def processed_bytes(self):
        if self.task:
            return self.__proc_bytes + self.task.completed_length
        return self.__proc_bytes

    @property
    def speed(self):
        return self.task.download_speed if self.task else 0

    def download(self, contents):
        self.is_downloading = True
        for content in contents:
            if self.__is_cancelled:
                break

            filename = content["filename"]
            success = False

            # Try progressive fallback if url_variants exist
            if 'url_variants' in content:
                success = self._download_with_fallback(content)
            else:
                # Fallback to original method
                success = self._download_original_method(content)

            if not success:
                self.__failed += 1

        if self.__is_cancelled:
            return
        if self.__failed == len(contents):
            async_to_sync(
                self.__listener.onDownloadError, "All files are failed to download!"
            )
            return
        async_to_sync(self.__listener.onDownloadComplete)

    def _download_with_fallback(self, content):
        """Download using progressive fallback method"""
        filename = content["filename"]
        url_variants = content.get("url_variants", [])
        file_id = content.get("file_id")
        api_key = content.get("api_key")

        for attempt, (url, headers, method) in enumerate(url_variants, 1):
            if self.__is_cancelled:
                return False

            LOGGER.info(f"Attempting download #{attempt} for {filename} using {method} method")

            # Create file-specific options
            file_a2c_opt = self.__a2c_opt.copy()

            # Set directory and filename
            if content["path"]:
                file_a2c_opt["dir"] = f"{self.__path}/{content['path']}"
            else:
                file_a2c_opt["dir"] = self.__path
            file_a2c_opt["out"] = filename

            # Add headers if present
            if headers:
                if isinstance(headers, dict):
                    header_strings = [f"{k}: {v}" for k, v in headers.items()]
                    file_a2c_opt["header"] = header_strings
                    LOGGER.info(f"Using {method} headers for {filename}")
                else:
                    file_a2c_opt["header"] = headers

            try:
                self.task = aria2.add_uris([url], file_a2c_opt, position=0)
                LOGGER.info(f"Started download attempt #{attempt}: {filename} with {method} method")

                if self._wait_for_download_completion(filename, method):
                    return True  # Success
                else:
                    # This attempt failed, continue to next variant
                    continue

            except Exception as e:
                LOGGER.error(f"Download attempt #{attempt} failed for {filename} using {method}: {e}")
                continue

        # All attempts failed
        if not api_key and len(url_variants) < 3:  # No API key was available
            LOGGER.error(
                f"All download attempts failed for {filename}. API key not set - cannot try authenticated download.")
            async_to_sync(self.__listener.onDownloadError,
                          f"Download failed for {filename}. Consider setting up Pixeldrain API key for authenticated downloads.")
        else:
            LOGGER.error(f"All download attempts failed for {filename} including authenticated method.")

        return False

    def _download_original_method(self, content):
        """Fallback to original download method"""
        filename = content["filename"]

        file_a2c_opt = self.__a2c_opt.copy()

        if content["path"]:
            file_a2c_opt["dir"] = f"{self.__path}/{content['path']}"
        else:
            file_a2c_opt["dir"] = self.__path
        file_a2c_opt["out"] = filename

        if file_headers := content.get("headers"):
            if isinstance(file_headers, dict):
                header_strings = [f"{k}: {v}" for k, v in file_headers.items()]
                file_a2c_opt["header"] = header_strings
            else:
                file_a2c_opt["header"] = file_headers

        try:
            self.task = aria2.add_uris([content["url"]], file_a2c_opt, position=0)
            LOGGER.info(f"Started download (original method): {filename}")

            return self._wait_for_download_completion(filename, "original")

        except Exception as e:
            LOGGER.error(f"Unable to download {filename} due to: {e}")
            return False

    def _wait_for_download_completion(self, filename, method):
        """Wait for download completion and handle errors"""
        self.task = self.task.live
        while True:
            if self.__is_cancelled:
                if self.task:
                    self.task.remove(True, True)
                return False

            self.task = self.task.live
            if error_message := self.task.error_message:
                LOGGER.error(f"Download failed for {filename} using {method} method: {error_message}")
                self.task.remove(True, True)
                return False
            elif self.task.is_complete:
                self.__proc_bytes += self.task.total_length
                self.task.remove(True)
                LOGGER.info(f"Successfully downloaded: {filename} using {method} method")
                self.task = None
                return True
            sleep(1)

    async def cancel_download(self):
        self.__is_cancelled = True
        LOGGER.info(f"Cancelling Download: {self.name}")
        await self.__listener.onDownloadError("Download Cancelled by User!")
        if self.task:
            await sync_to_async(self.task.remove, force=True, files=True)
