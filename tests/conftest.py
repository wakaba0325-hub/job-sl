import io

import pytest

import job_sl


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def iter_lines(self, chunk_size=1024, keepends=False):
        yield from (
            io.BytesIO(self._data).readlines() if keepends else self._data.splitlines()
        )

    def read(self):
        return self._data


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def get_paginator(self, name):
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):
                yield {
                    "Contents": [
                        {"Key": k}
                        for k in sorted(outer.objects)
                        if k.startswith(Prefix)
                    ]
                }

        return P()

    def get_object(self, Bucket, Key):
        return {"Body": _Body(self.objects[Key])}

    def put_object(self, Bucket, Key, Body):
        self.objects[Key] = Body

    def upload_file(self, path, Bucket, Key):
        with open(path, "rb") as f:
            self.objects[Key] = f.read()


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(job_sl, "_s3", lambda: fake)
    monkeypatch.setattr(job_sl, "_today", lambda: "20261020")
    return fake
