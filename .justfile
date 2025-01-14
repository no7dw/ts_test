set dotenv-load := true

branch := "dev"

default:
    @just --list --justfile {{ justfile() }}

sync:
    rye sync

install:
    rye sync --no-lock

start:
    docker compose up --build -d

stop:
    docker compose down

log:
    docker compose logs -f

pull branch=branch:
    git checkout {{ branch }}
    git pull origin {{ branch }}

update branch=branch: stop (pull branch) start


migrate:
    rye run python src/xcelsior_gateway/migrations/mongodb.py

test:
    rye run python -m unittest discover tests -v

format:
    rye run isort --profile=black --skip-gitignore .
    rye run ruff check --fix --exit-zero .
    rye run ruff format .

format-file PATH:
    rye run isort --profile=black --skip-gitignore {{PATH}}
    rye run ruff check --fix --exit-zero {{PATH}}
    rye run ruff format {{PATH}}

setup:
    # Rye
    curl -sSf https://rye-up.com/get | bash
    echo 'source "$HOME/.rye/env"' >> ~/.bashrc

    # Direnv
    curl -sfL https://direnv.net/install.sh | bash
    echo 'eval "$(direnv hook bash)"' >> ~/.bashrc

    @echo "Restart your shell to finish setup!"
