#!/bin/bash

# Файл с логами
LOG_FILE="access.log"
REPORT_FILE="report.txt"

# 1. Подсчет общего количества запросов
total_requests=$(wc -l < "$LOG_FILE")

# 2. Подсчет количества уникальных IP-адресов с использованием awk
unique_ips=$(awk '{print $1}' "$LOG_FILE" | sort | uniq | wc -l)

# 3. Подсчет количества запросов по методам (GET, POST и т.д.) с использованием awk
request_methods=$(awk '{print $6}' "$LOG_FILE" | tr -d '"' | sort | uniq -c)

# 4. Поиск самого популярного URL с использованием awk
popular_url=$(awk '{print $7}' "$LOG_FILE" | sort | uniq -c | sort -nr | head -n 1 | awk '{print $2}')

# 5. Создание отчета
echo "Отчет о логе веб-сервера" > "$REPORT_FILE"
echo "========================" >> "$REPORT_FILE"
echo "Общее количество запросов: $total_requests" >> "$REPORT_FILE"
echo "Количество уникальных IP-адресов: $unique_ips" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "Количество запросов по методам:" >> "$REPORT_FILE"
echo "$request_methods" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "Самый популярный URL: $popular_url" >> "$REPORT_FILE"

# Сообщение об успешном завершении
echo "Отчет создан в файл $REPORT_FILE"