# Template Synchronization Guide

## Overview
This document describes how to synchronize changes from a rendered Archetect project (`.contents`) back to the template source (`contents`).

## Background
Archetect (similar to Cookiecutter) uses template directories to generate projects. The template source lives in `contents/` with placeholder variables like `{{ project-name }}`, while `.contents/` contains the rendered output with actual values.

## Synchronization Process

### 1. Check Git Status in Rendered Directory
```bash
cd .contents && git status
```
This shows both staged and unstaged changes that need to be synchronized back to the template.

### 2. Identify Changes to Synchronize
Look for:
- **New files** (need to be created in template)
- **Modified files** (need changes applied to template)
- **Deleted files** (need to be removed from template)

Common changes include:
- Configuration files (`.gitignore`, `pyproject.toml`, lock files)
- IDE settings (`.vscode/`, `.idea/`)
- Source code formatting changes
- Documentation updates

### 3. Compare Files
For each changed file, examine the differences:
```bash
# For staged changes
git diff --cached path/to/file

# For unstaged changes
git diff path/to/file

# Direct comparison
diff -u "contents/{{ project-name }}/file" .contents/file
```

### 4. Apply Changes to Template

#### For New Files
Create the file in the template directory:
```bash
cp .contents/newfile "contents/{{ project-name }}/newfile"
```

#### For Modified Files
- **Binary files or lock files**: Copy directly
  ```bash
  cp .contents/file "contents/{{ project-name }}/file"
  ```
- **Text files with small changes**: Use Edit tool to apply specific changes
- **Files with extensive changes**: Copy the entire file

#### For Configuration Files
Pay special attention to:
- Version constraints (Python versions, dependency versions)
- Tool configurations (formatters, linters, test runners)
- Build system changes

### 5. Preserve Template Variables
Ensure template variables remain intact in the template directory:
- Keep `{{ project-name }}` and other placeholders
- Don't replace with rendered values from `.contents`

### 6. Verify Synchronization
After applying changes:
```bash
git status
```
Check that all necessary changes are reflected in the `contents/` directory.

## Common Patterns

### Formatting Changes
When code formatters (Black, Ruff, etc.) are run in `.contents`, they often create:
- Quote style changes (single to double quotes)
- Spacing and indentation adjustments
- Line break modifications
- Trailing newlines

### Configuration Updates
Look for:
- Python version updates
- New development dependencies
- Build system migrations (e.g., `[tool.uv]` → `[dependency-groups]`)
- New tool configurations (formatters, type checkers)

### IDE Settings
VSCode, PyCharm, and other IDEs may create:
- Extension recommendations
- Project-specific settings
- Formatter configurations
- Debug configurations

## Tips

1. **Use git diff effectively**: Always check both staged and unstaged changes
2. **Batch similar changes**: Apply all formatting changes together
3. **Test the template**: After synchronization, consider rendering a new project to verify the template works correctly
4. **Document significant changes**: If the synchronization introduces breaking changes, update the template's README

## Automation Potential
This process could potentially be automated with a script that:
1. Detects all changes in `.contents`
2. Maps them back to template paths
3. Applies changes while preserving template variables
4. Generates a summary of synchronized changes

---
*This guide helps maintain consistency between the Archetect template and its rendered output, ensuring the template captures all improvements made during development.*