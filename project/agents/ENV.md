# Lesson 01 environment setup (project)

1. Ensure `pyenv` is installed and sourced so its shims are available (`pyenv init`, `pyenv virtualenv-init` as needed).
2. From `project/`, run `bash setup_env.sh`. The script installs Python 3.11.6 via pyenv, creates a `.venv`, and installs the dependencies listed in `requirements.txt`.
3. Activate the virtual environment before running any lesson scripts with `source .venv/bin/activate`.
4. To keep the pyenv shim consistent inside this directory, optionally run `pyenv local 3.11.6`.
5. Export the provider API keys (`GROQ_API_KEY` and `GEMINI_API_KEY`) before executing `lesson_01_working_code.py` or any notebook so the calls to Groq/Gemini succeed.

The `.venv/` folder is ignored by `.gitignore`, so re-run the setup script if you ever remove it.
