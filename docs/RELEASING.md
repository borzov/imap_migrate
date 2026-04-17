# Releasing imap_migrate

## GitHub Release (recommended)

1. Update [CHANGELOG.md](../CHANGELOG.md) with a `## [x.y.z]` section and ensure [pyproject.toml](../pyproject.toml) / [imap_migrate/__init__.py](../imap_migrate/__init__.py) use the same version.
2. Commit and push to the default branch.
3. Create an annotated tag: `git tag -a vx.y.z -m "Release x.y.z"` then `git push origin vx.y.z`.
4. The [release.yml](../.github/workflows/release.yml) workflow runs tests and checks that the tag matches `pyproject.toml`.
5. On GitHub: **Releases → Draft a new release** → choose the tag → title `x.y.z` → paste the changelog section for the release notes → publish.

## Local install from source

```bash
pip install -e ".[dev]"
pytest tests/
```
