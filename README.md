executar em WSL 
uv init 
uv venv --python=3.13
.venv/bin/activate
uv pip install -r stock-market-etl/requirements.txt


# Realizar a extração dos dados do yahoo e do founds explorer
uv run python stock-market-etl/src/main.py


