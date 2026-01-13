#!/bin/bash

# Проверяем наличие async_start.py
if [[ ! -f "async_start.py" ]]; then
    echo "Ошибка: Файл async_start.py отсутствует."
    exit 1
fi

# Основной цикл
while true; do
    python -u async_start.py || sleep 10
done
