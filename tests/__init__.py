import pathlib
import sys

# чтобы `python -m unittest` работал из корня репозитория без установки пакета
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))
