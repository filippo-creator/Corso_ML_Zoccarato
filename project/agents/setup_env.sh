#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_VERSION="3.11.6"
PYENV_ENV_DIR=".venv"

if ! command -v pyenv >/dev/null 2>&1; then
  echo "pyenv is required to run this script; install it (https://github.com/pyenv/pyenv) first." >&2
  exit 1
fi

echo "Installing Python $PYTHON_VERSION via pyenv (skips if already installed)..."
pyenv install -s "$PYTHON_VERSION"

export PYENV_VERSION="$PYTHON_VERSION"

if [ ! -d "$PYENV_ENV_DIR" ]; then
  echo "Creating a virtual environment at $PYENV_ENV_DIR using Python $PYTHON_VERSION..."
  python -m venv "$PYENV_ENV_DIR"
else
  echo "Reusing existing virtual environment at $PYENV_ENV_DIR."
fi

ENV_PYTHON="$PYENV_ENV_DIR/bin/python"

echo "Installing lesson dependencies..."
"$ENV_PYTHON" -m pip install --upgrade pip
"$ENV_PYTHON" -m pip install -r requirements.txt

cat <<'EOF'
Environment ready!
• Activate it with: source .venv/bin/activate
• To persist the Python version for this directory run: pyenv local 3.11.6
• Remember to export GROQ_API_KEY and GEMINI_API_KEY before running the exercises.
EOF
