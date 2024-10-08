# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import io
import locale
import logging
import os
import re
import sys
from typing import Callable, List, Optional, Tuple, Union
from tailer.version import VERSION
from io import SEEK_SET, SEEK_CUR, SEEK_END


__version__ = VERSION


LOG = logging.getLogger("tailer")


def power_of_2(x: int) -> int:
    """Retrieve the Next power of 2, if value is not already one."""
    x -= 1
    mod: int = 1
    size: int = sys.getsizeof(x)
    while mod < size:
        x |= x >> mod
        mod *= 2

    return x + 1


class Tailer(object):
    """Implements tailing and heading functionality like GNU tail and head commands.

    Args:
        stream (io.IOBase | io.TextIOBase): Binary File Stream
        size (int): Size in bytes, to chunk reading
        end (bool): If true, read stream from the end of stream

    Raises:
        ValueError: When stream is not opened in binary mode.

    """

    _size: int
    stream: io.IOBase | io.TextIOBase
    _terminators: tuple[bytes] = (b"\r\n", b"\n", b"\r")

    def __init__(
        self,
        stream: Union[io.IOBase, io.TextIOBase],
        size: int = 1024,
        end: bool = False,
    ) -> None:
        if not isinstance(stream, (io.IOBase, io.TextIOBase)):
            raise ValueError(f"IO object must be in the binary mode: {type(stream)}")

        self.size = size
        self.stream = stream

        if end:
            self.stream.seek(0, SEEK_END)

    @property
    def size(self) -> int:
        """Default read size, in bytes."""
        return self._size

    @size.setter
    def size(self, value: int):
        self._size = power_of_2(value)

    def seek(self, offset: int, whence: int = SEEK_SET) -> int:
        """Fast forward (or reverse) to a specific position in the stream."""
        return self.stream.seek(offset, whence)

    def tell(self) -> int:
        """Current Cursor Position in stream."""
        return self.stream.tell()

    def splitlines(self, data: Union[bytes, str]) -> Optional[List[bytes]]:
        """Splits data into lines where lines are separated by _terminators.

        Args:
            data (bytes | str): Any chunk of binary data.

        Returns:
            (list[bytes]) List of lines without any characters at _terminators.

        """
        return re.split(b"|".join(self._terminators), data)

    def read(self, read_size: int = -1) -> Tuple[int, Union[bytes, str]]:
        """Reads given number of bytes from stream.

        Args:
            read_size (int): Number of bytes to read. -1 to read all.

        Returns:
            Number of bytes read and data that was read.

        """
        read_str = self.stream.read(read_size)
        return len(read_str), read_str

    def term(self, method: Callable[[bytes], bool]) -> Optional[bytes]:
        for t in self._terminators:
            if method(t):
                return t

    def prefix(self, data: Union[bytes, str]) -> Optional[bytes]:
        """Return line terminator data begins with if found."""
        return self.term(data.startswith)

    def suffix(self, data: Union[bytes, str]) -> Optional[bytes]:
        """Return line terminator data ends with if found."""
        return self.term(data.endswith)

    def next(self) -> int:
        """Seek next line relative to the current stream position.

        Returns:
            Position of the line or -1 if next line was not found.

        """
        where = self.stream.tell()
        offset = 0

        while True:
            data_len, data = self.read(self.size)
            data_where = 0

            if not data_len:
                break

            # Consider the following example: Foo\r | \nBar where " | " denotes current
            # position in the stream. "Foo\r" is the read part and "\nBar" is the
            # remaining part. We should completely consume terminator "\r\n" by reading
            # one extra byte.
            if b"\r\n" in self._terminators and data[-1] == b"\r"[0]:
                terminator_where = self.tell()
                length, terminator_data = self.read(1)

                if length and terminator_data[0] == b"\n"[0]:
                    data_len += 1
                    data += b"\n"
                else:
                    self.seek(terminator_where)

            while data_where < data_len:
                terminator = self.prefix(data[data_where:])
                if terminator:
                    self.seek(where + offset + data_where + len(terminator))
                    return self.tell()
                else:
                    data_where += 1

            offset += data_len
            self.seek(where + offset)

        return -1

    def previous(self) -> int:
        """Seeks previous line relative to the current stream position.

        :return: Position of the line or -1 if previous line was not found.
        """
        where = self.stream.tell()
        offset = 0

        while True:
            if offset == where:
                break

            read_size = self.size if self.size <= where else where
            self.seek(where - offset - read_size, SEEK_SET)
            data_len, data = self.read(read_size)

            # Consider the following example: Foo\r | \nBar where " | " denotes current
            # position, "\nBar" is the read part and "Foo\r" is the remaining part.
            # We should completely consume terminator "\r\n" by reading one extra byte.
            if b"\r\n" in self._terminators and data[0] == b"\n"[0]:
                terminator_where = self.tell()
                if terminator_where > data_len + 1:
                    self.seek(where - offset - data_len - 1, SEEK_SET)
                    _, terminator_data = self.read(1)

                    if terminator_data[0] == b"\r"[0]:
                        data_len += 1
                        data = b"\r" + data

                    self.seek(terminator_where)

            data_where = data_len

            while data_where > 0:
                terminator = self.suffix(data[:data_where])
                if terminator and offset == 0 and data_where == data_len:
                    # The last character is a line terminator that finishes current
                    # line. Ignore it.
                    data_where -= len(terminator)
                elif terminator:
                    self.seek(where - offset - (data_len - data_where))
                    return self.tell()
                else:
                    data_where -= 1

            offset += data_len

        if where == 0:
            # Nothing more to read.
            return -1
        else:
            # Very first line.
            self.seek(0)
            return 0

    def tail(self, lines: int = 10) -> List[bytes]:
        """Returns the last lines of the stream."""
        self.seek(0, SEEK_END)

        while lines and self.previous() != -1:
            lines -= 1

        data = self.stream.read()

        for t in self._terminators:
            if not data.endswith(t):
                continue
            # Only terminators _between_ lines should be preserved. Otherwise,
            # terminator of the last line will be treated as separating line and empty
            # line.
            data = data[: -len(t)]
            break

        return self.splitlines(data) if data else []

    def head(self, lines: int = 10) -> List[bytes]:
        """Reads and returns the top n lines of the stream."""
        self.seek(0)

        while lines and self.next() != -1:
            lines -= 1

        end_pos = self.tell()

        self.seek(0)
        data = self.stream.read(end_pos)

        for t in self._terminators:
            if data.endswith(t):
                # Only terminators _between_ lines should be preserved.
                # Otherwise, terminator of the last line will be treated as separating
                # line and empty line.
                data = data[: -len(t)]
                break

        return self.splitlines(data) if data else []

    def follow(self):
        """Iterator generator that returns lines as data is added to the stream.

        None will be yielded if no new line is available.
        Caller may either wait and re-try or end iteration.
        """
        trailing = True

        while True:
            where = self.tell()

            if where > os.fstat(self.stream.fileno()).st_size:
                # File was truncated.
                where = 0
                self.seek(where)

            line = self.stream.readline()

            if line:
                if trailing and line in self._terminators:
                    # This is just the line terminator added to the end of the stream
                    # before a new line, ignore.
                    trailing = False
                    continue

                terminator = self.suffix_line_terminator(line)
                if terminator:
                    line = line[: -len(terminator)]

                trailing = False
                yield line
            else:
                trailing = True
                self.seek(where)
                yield None


def tail(file, lines: int = 10, read_size: int = 1024) -> List[bytes]:
    """Returns the last lines of the stream.

    >>> import io
    >>>
    ... with io.open("test_tail.txt", "w+") as fw:
    ...     with io.open("test_tail.txt", "rb") as fr:
    ...         _ = fw.write("\\r")
    ...         _ = fw.write("Line 1\\r\\n")
    ...         _ = fw.write("Line 2\\n")
    ...         _ = fw.write("Line 3\\r")
    ...         _ = fw.write("Line 4\\r\\n")
    ...         _ = fw.write("\\n")
    ...         _ = fw.write("\\r\\n")
    ...         _ = fw.write("\\r\\n")
    ...         fw.flush()
    ...         tail(fr, 6, 1)  # doctest: +ELLIPSIS
    [..."Line 2", ..."Line 3", ..."Line 4", ..."", ..."", ...""]
    >>> os.remove("test_tail.txt")

    >>> import io
    >>>
    ... with io.open("test_tail.txt", "w+") as fw:
    ...     with io.open("test_tail.txt", "rb") as fr:
    ...         _ = fw.write("Line 1")
    ...         fw.flush()
    ...         tail(fr, 6, 1)  # doctest: +ELLIPSIS
    [..."Line 1"]
    >>> os.remove("test_tail.txt")
    """
    return Tailer(file, read_size).tail(lines)


def head(file: str, lines: int = 10, read_size: int = 1024) -> List[bytes]:
    """Returns the top lines of the stream.

    >>> import io
    >>>
    ... with io.open("test_head.txt", "w+") as fw:
    ...     with io.open("test_head.txt", "rb") as fr:
    ...         _ = fw.write("\\r\\n")
    ...         _ = fw.write("\\r\\n")
    ...         _ = fw.write("\\r")
    ...         _ = fw.write("Line 1\\r\\n")
    ...         _ = fw.write("Line 2\\r\\n")
    ...         _ = fw.write("Line 3\\r")
    ...         _ = fw.write("Line 4\\r\\n")
    ...         _ = fw.write("\\n")
    ...         _ = fw.write("\\r")
    ...         fw.flush()
    ...         head(fr, 6, 1)  # doctest: +ELLIPSIS
    [..."", ..."", ..."", ..."Line 1", ..."Line 2", ..."Line 3"]
    >>> os.remove("test_head.txt")
    """
    if not isinstance(file, str):
        return Tailer(file, read_size).head(lines)
    with open(file, "rb") as stream:
        return Tailer(stream, read_size).head(lines)


def follow(file):
    """
    Generator that returns lines as data is added to the stream.

    Returned generator yields bytes.

    >>> import io
    >>> import os
    >>> f = io.open("test_follow.txt", "w+")
    >>> fo = io.open("test_follow.txt", "rb")
    >>> generator = follow(fo)
    >>> _ = f.write("Line 1\\n")
    >>> f.flush()
    >>> print(next(generator).decode("utf-8"))
    Line 1
    >>> _ = f.write("Line 2\\n")
    >>> f.flush()
    >>> print(next(generator).decode("utf-8"))
    Line 2
    >>> _ = f.truncate(0)
    >>> _ = f.seek(0)
    >>> _ = f.write("Line 3\\n")
    >>> f.flush()
    >>> print(next(generator).decode("utf-8"))
    Line 3
    >>> print(next(generator))
    None
    >>> f.close()
    >>> fo.close()
    >>> os.remove("test_follow.txt")
    """
    return Tailer(file, end=True).follow()


def follow_path(file_path, buffering=-1, encoding=None, errors=None):
    """
    Similar to follow, but also looks up if inode of stream is changed
    e.g. if it was re-created.

    Returned generator yields strings encoded by using encoding.
    If encoding is not specified, it defaults to locale.getpreferredencoding()

    >>> import io
    >>> import os
    >>> f = io.open("test_follow_path.txt", "w+")
    >>> generator = follow_path("test_follow_path.txt")
    >>> _ = f.write("Line 1\\n")
    >>> f.flush()
    >>> print(next(generator))
    Line 1
    >>> _ = f.write("Line 2\\n")
    >>> f.flush()
    >>> print(next(generator))
    Line 2
    >>> _ = f.truncate(0)
    >>> _ = f.seek(0)
    >>> _ = f.write("Line 3\\n")
    >>> f.flush()
    >>> print(next(generator))
    Line 3
    >>> f.close()
    >>> os.remove("test_follow_path.txt")
    >>> f = io.open("test_follow_path.txt", "w+")
    >>> _ = f.write("Line 4\\n")
    >>> f.flush()
    >>> print(next(generator))
    Line 4
    >>> print(next(generator))
    None
    >>> f.close()
    >>> os.remove("test_follow_path.txt")
    """
    if encoding is None:
        encoding = locale.getpreferredencoding()

    if not errors:
        errors = "strict"

    class FollowPathGenerator(object):
        def __init__(self):
            if os.path.isfile(file_path):
                self.following_file = io.open(file_path, "rb", buffering)
                self.follow_generator = Tailer(self.following_file, end=True).follow()
                self.follow_from_end_on_open = False
            else:
                self.following_file = None
                self.follow_generator = None
                self.follow_from_end_on_open = True

        def next(self):
            while True:
                if self.follow_generator:
                    line = next(self.follow_generator)
                else:
                    line = None

                if line is None:
                    if self.follow_generator:
                        try:
                            is_file_changed = (
                                not os.path.isfile(file_path)
                                or os.stat(file_path).st_ino
                                != os.fstat(self.following_file.fileno()).st_ino
                            )
                        except OSError:
                            # File could be deleted between isfile and stat invocations,
                            # which will make the latter to fail.
                            is_file_changed = True

                        if is_file_changed:
                            # File was deleted or re-created.
                            self.following_file.close()
                            self.following_file = None
                            self.follow_generator = None

                    if not self.follow_generator and os.path.isfile(file_path):
                        # New stream is available. Open it.
                        try:
                            self.following_file = io.open(file_path, "rb", buffering)
                            self.follow_generator = Tailer(
                                self.following_file, end=self.follow_from_end_on_open
                            ).follow()
                            # something could be written before we notice the change
                            self.follow_from_end_on_open = False
                        except (IOError, OSError) as e:
                            LOG.info("Unable to tail stream: %s", e)
                            if self.following_file:
                                self.following_file.close()

                            self.following_file = None
                            self.follow_generator = None
                            line = None
                        else:
                            line = next(self.follow_generator)

                return line.decode(encoding, errors) if line is not None else line

        def __iter__(self):
            return self

        def __next__(self):
            return self.next()

    return FollowPathGenerator()
