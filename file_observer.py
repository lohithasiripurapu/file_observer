import time
from argparse import ArgumentParser
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class ActivityLogger(FileSystemEventHandler):
    def on_created(self, event):
        print(f'{event.src_path} has been created.')

    def on_deleted(self, event):
        print(f'{event.src_path} has been deleted.')

    def on_modified(self, event):
        print(f'{event.src_path} has been modified.')

    def on_moved(self, event):
        print(f'{event.src_path} has been moved to {event.dest_path}.')


class ObserverService:
    def __init__(self, directory):
        self.directory = directory
        self.event_handler = ActivityLogger()
        self.observer = Observer()

    def start(self):
        self.observer.schedule(self.event_handler, path=self.directory, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
            self.observer.join()


def main():
    parser = ArgumentParser(description='File Watcher Agent')
    parser.add_argument('directory', help='Directory to watch')
    args = parser.parse_args()

    observer_service = ObserverService(args.directory)
    observer_service.start()


if __name__ == '__main__':
    main()
