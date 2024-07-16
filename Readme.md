## Разработка базы данных для управления концертами

### Созданы 4 таблицы:
1. Концерты
2. Организаторы
3. Исполнитель
4. Билеты

Таблица с концертами связана по внешнему ключу с таблицей организаторов и исполнителей по fk_performer и fk_organizer
Таблица с билетами связана по внешнему ключу с таблицей концертами по fk_concert

