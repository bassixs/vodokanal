# Инструкция по развертыванию на сервере

См. [DEPLOY.md](https://github.com/bassixs/vodokanal/blob/main/DEPLOY.md) (Этот файл будет доступен после пуша, если вы просто читаете файл, откройте DEPLOY.md в корне).

Кратко:
1. `git clone https://github.com/bassixs/vodokanal.git`
2. `cd vodokanal`
3. `python3 -m venv .venv && source .venv/bin/activate`
4. `pip install -r requirements.txt`
5. `cp .env.example .env` и настроить ключи.
6. Настроить systemd (см. `systemd/bot.service`).
