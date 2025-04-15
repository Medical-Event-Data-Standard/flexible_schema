"""Generate the code reference pages."""

import re
from pathlib import Path

import mkdocs_gen_files

root = Path(__file__).parent.parent
readme = root / "README.md"

# Src links are of the form `(src/.../*.py)`.
SRC_LINK_RE = re.compile(r"\(src/[^)]+\.py\)")

if not readme.is_file():
    raise FileNotFoundError(f"{readme} not found")

readme_lines = readme.read_text().splitlines()

remapped_lines = []
for line in readme_lines:
    if SRC_LINK_RE.search(line):
        # Replace src/.../*.py with the corresponding markdown file
        line = SRC_LINK_RE.sub(lambda m: f"(api/{m.group(0)[5:-4]})", line)
    remapped_lines.append(line)

home_path = "index.md"
with mkdocs_gen_files.open(home_path, "w") as fd:
    fd.write("\n".join(remapped_lines))
