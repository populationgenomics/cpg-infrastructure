from pathlib import Path

from cpg_infra.utils import archive_folder


def test_archive_folder_skips_empty_files(tmp_path: Path):
    (tmp_path / 'main.py').write_text('print("hi")\n')
    (tmp_path / '__init__.py').write_text('')

    archive = archive_folder(str(tmp_path))

    assert sorted(archive.assets) == ['main.py']
