@echo off
echo 正在移除G7-2025触发器...

mysql -h 你的数据库地址 -P 3306 -u 你的用户名 -p你的密码 你的数据库名 -e "DROP TRIGGER IF EXISTS g7_guard_insert; DROP TRIGGER IF EXISTS g7_guard_update; DROP TRIGGER IF EXISTS prevent_g7_2025_insert; DROP TRIGGER IF EXISTS prevent_g7_2025_update; DROP TRIGGER IF EXISTS g7_2025_guard; DROP TRIGGER IF EXISTS block_g7_2025;"

echo 触发器移除完成
pause