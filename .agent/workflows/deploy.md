---
description: Деплой изменений на GitHub и сервер
---

# Workflow: Деплой Изменений

## 1. Отправка изменений на GitHub

```bash
git add .
git commit -m "Описание, что сделали"
git push
```

## 2. Обновление версии на сервере

```bash
cd /root/vodokanal
sudo systemctl stop bot.service
git pull origin main
sudo systemctl start bot.service
```

## 3. Проверка статуса (опционально)

```bash
sudo systemctl status bot.service
```

---

**Готово!** Бот обновлён и работает с новыми изменениями.
