import fcntl
import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path

_DEFAULT_PATH = Path(__file__).parent.joinpath('slices.json')

class SliceRegistry:

    def __init__(self, pipeline: str, path: Path | str | None = None):
        self.pipeline = pipeline
        self._path = Path(path or os.environ.get('SLICE_REGISTRY_PATH') or _DEFAULT_PATH)
        self._lock_path = self._path.with_name(f'{self._path.name}.lock')

    def read(self, obj: str) -> str | None:
        return self._load().get(self.pipeline, {}).get(obj)

    def advance(self, obj: str, value: str) -> bool:
        with self._locked():
            slices = self._load()
            current = slices.get(self.pipeline, {}).get(obj)
            if current is not None and value <= current:
                return False
            slices.setdefault(self.pipeline, {})[obj] = value
            self._dump(slices)
        return True

    def reset(self, obj: str, value: str | None = None) -> None:
        with self._locked():
            slices = self._load()
            if value is None:
                slices.get(self.pipeline, {}).pop(obj, None)
            else:
                slices.setdefault(self.pipeline, {})[obj] = value
            self._dump(slices)

    @contextmanager
    def _locked(self):
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._lock_path, 'a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def _load(self) -> dict:
        try:
            with open(self._path, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError as e:
            raise ValueError(f'slice registry {self._path} is corrupted') from e

    def _dump(self, slices: dict) -> None:
        tmp_path = self._path.with_name(f'{self._path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp')
        with open(tmp_path, 'w') as file:
            json.dump(slices, file)
            file.flush()
            os.fsync(file.fileno())
        tmp_path.replace(self._path)
