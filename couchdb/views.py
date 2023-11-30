import collections


class ViewResult(object):
    """Result of view query; contains rows, offset, total_rows.
    Instances of this class are not supposed to be created by client software.
    """

    def __init__(self, rows, offset, total_rows):
        self.rows = rows
        self.offset = offset
        self.total_rows = total_rows

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, i):
        return self.rows[i]

    def __iter__(self):
        return iter(self.rows)

    def json(self):
        "Return data in a JSON-like representation."
        result = dict()
        result["total_rows"] = self.total_rows
        result["offset"] = self.offset
        return result


Row = collections.namedtuple("Row", ["id", "key", "value", "doc"])
