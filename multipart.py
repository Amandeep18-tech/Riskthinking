import boto3
from boto3.s3.transfer import TransferConfig
import os
import sys
import threading
MB = 1024 * 1024
s3 = boto3.resource('s3')
class TransferCallback:
    """
    Handle callbacks from the transfer manager.

    The transfer manager periodically calls the __call__ method throughout
    the upload and download process so that it can take action, such as
    displaying progress to the user and collecting data about the transfer.
    """

    def __init__(self, target_size):
        self._target_size = target_size
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}

    def __call__(self, bytes_transferred):
        """
        The callback method that is called by the transfer manager.

        Display progress during file transfer and collect per-thread transfer
        data. This method can be called by multiple threads, so shared instance
        data is protected by a thread lock.
        """
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size * MB
            sys.stdout.write(
                f"\r{self._total_transferred} of {target} transferred "
                f"({(self._total_transferred / target) * 100:.2f}%).")
            sys.stdout.flush()

def upload_with_chunksize_and_meta(file_size_mb):
    """
    Upload a file from a local folder to an Amazon S3 bucket, setting a
    multipart chunk size and adding metadata to the Amazon S3 object.

    The multipart chunk size controls the size of the chunks of data that are
    sent in the request. A smaller chunk size typically results in the transfer
    manager using more threads for the upload.

    The metadata is a set of key-value pairs that are stored with the object
    in Amazon S3.
    """
    transfer_callback = TransferCallback(file_size_mb)
    bucket_name = 'myregbucketmodel'
    object_key = 'finalized_model.joblib'
    local_file_path='finalized_model.joblib'
    config = TransferConfig(multipart_chunksize=1 * MB)
    s3.Bucket(bucket_name).upload_file(
        local_file_path,
        object_key,
        Config=config,
        Callback=transfer_callback)
    return transfer_callback.thread_info

file_size = os.path.getsize('finalized_model.joblib')
size = file_size / 1024 ** 2

upload_with_chunksize_and_meta(size)

