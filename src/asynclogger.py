"""
AsyncLoggeer

Create a simple async logger that does not block.

"""
import threading
import queue
import os

class AsyncLogger:
    def __init__(self, log_file):
        self.log_file = log_file


        # Start the tread and create a message queue
        self.queue = queue.Queue()
        self.worker_thread = threading.Thread(target=self._worker)
        self.worker_thread.daemon = True
        self.worker_thread.start()

        # Create the log file if it doesn't exist
        # if not os.path.exists(self.log_file):
        #     with open(self.log_file, "w") as f:
        #         f.write('Initialized logger.')
        #         pass
        with open(self.log_file, "w") as f:
            f.write('Initialized logger.\n')

    def _worker(self):
        """
        Worker that writes to the log file.
        """
        while True:

            message = self.queue.get()

            # Special message that terminates the thread
            if message == "$":
                self.queue.task_done()
                break

            with open(self.log_file, "a") as f:
                f.write(message + "\n")


    def write_log(self, message):
        """
        Puts the log message in a queue for writing to a file.
        """
        self.queue.put(message)

    def stop(self):
        """Flush the queue and stop the worker thread."""

        # The last message in the queue is always the $ , which tells the thread's loop to break.
        self.write_log("$")

        # Wait for the thread to exit.
        self.worker_thread.join()