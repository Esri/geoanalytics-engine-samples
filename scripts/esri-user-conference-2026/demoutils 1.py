from pathlib import Path
from IPython.display import display, HTML
import json


def print_file_tree(root, glob="*", include_parent=True, limit=None):
    """
    Prints a tree of files in the given directory that match the provided glob pattern. Uses Spark to 
    read the file paths, which allows it to work with both local and cloud storage.

    :param root: The root directory to list files from.
    :param glob: A glob pattern to filter files (e.g., "*.parquet")
    """
    from fnmatch import fnmatch
    from pyspark.sql import SparkSession

    # Note that `binaryFile` does not attempt to read the file contents if the `content` column is not selected.
    rows = SparkSession.active().read.format("binaryFile").load(root).select("path", "length").collect()

    # normalize root and capture parent name
    if not root.endswith('/'):
        root = root + '/'
    parent_name = root.rstrip('/').split('/')[-1]

    files = []
    for r in rows:
        full = r.path
        rel = full[len(root):] if full.startswith(root) else full
        if fnmatch(rel, glob):
            files.append((rel, int(r.length)))

    # build nested dict
    tree = {}
    for rel, size in files:
        parts = rel.split('/')
        node = tree
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        node[parts[-1]] = size

    tree_parent_name = parent_name if include_parent else None

    def human_size(n):
        for unit in ['B','KB','MB','GB','TB','PB']:
            if n < 1024:
                return f"{n:.0f}{unit}"
            n /= 1024.0
        return f"{n:.0f}EB"

    limit_n = None if limit is None else int(limit)
    if limit_n is not None and limit_n < 0:
      limit_n = 0

    def print_node(node, prefix=""):
      names = sorted(node.keys())

      if limit_n is None:
        visible_names = names
        elided_count = 0
      else:
        visible_names = names[:limit_n]
        elided_count = max(0, len(names) - len(visible_names))

      total_lines = len(visible_names) + (1 if elided_count else 0)

      for i, name in enumerate(visible_names):
        last = (i == total_lines - 1) and (elided_count == 0)
        connector = "└─ " if last else "├─ "
        if isinstance(node[name], dict):
          print(prefix + connector + f"📂 {name}/")
          new_prefix = prefix + ("   " if last else "│  ")
          print_node(node[name], new_prefix)
        else:
          print(prefix + connector + f"📄 {name}  ({human_size(node[name])})")

      if elided_count:
        connector = "└─ "
        print(prefix + connector + f"({elided_count} items elided)")

    if tree_parent_name is not None:
      print(f"└─ 📂 {tree_parent_name}/")
      print_node(tree, "   ")
    else:
      print_node(tree)

