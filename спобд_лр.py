import mysql.connector
from mysql.connector import Error
import csv
from typing import List, Tuple, Optional, Any


class SQL:
    def __init__(self, db_config, table_name):
        self.connection = None
        self.config = None
        self.conn = mysql.connector.connect(**db_config)
        self.table_name = table_name
        self.db_config = db_config
        self.table_name = table_name
        self.cursor = self.conn.cursor()
        self.columns = []

    def execute(self, query: str, params: tuple = ()):
        try:
            self.connect()
            self.cursor.execute(query, params)
            self.connection.commit()
            return self.cursor.fetchall()
        except Error as e:
            self.connection.rollback()
            print(f"Error: {e}")
            return []
        finally:
            self.disconnect()

    def connect(self):
        self.connection = mysql.connector.connect(**self.config)
        self.cursor = self.connection.cursor()

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()

    # создаём таблицу, если она не существует, с автоматически увеличивающимся первичным ключом
    def create_table(self, columns):
        column_definition = ', '.join(f"`{name}` {type}" for name, type in columns.items())
        query = f'''
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
             {column_definition}
                )
                '''
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            self.conn.commit()
        finally:
            cursor.close()
        print(f"таблица '{self.table_name}' cсоздана с колонаками {self.columns}")

    # обновляет столбец, используя первичный ключ
    def update_column(self):
        query = f"columns from {self.table_name}"
        self.cursor.execute(query)
        self.columns = [row[0] for row in self.cursor.fetchall()]

    # ищет похожую таблицу, если она есть
    def chek_table_exist(self, table_name):
        query = f"show table like '{self.table_name}'"
        self.cursor.execute(query)
        return self.cursor.fetchone() is not None

    # удаляет таблицу
    def drop_table(self):
        cursor = self.conn.cursor()
        try:
            query = f"DROP TABLE IF EXISTS {self.table_name}"
            cursor.execute(query)
            self.conn.commit()
        finally:
            cursor.close()
        print(f"таблица '{self.table_name}' удалена")

    # чтение данных
    def select_table(self, columns='*', where=None, params=None):
        query = f"SELECT {columns} FROM `{self.table_name}`"
        if where:
            query += f" WHERE {where}"
        try:
            self.cursor.execute(query, params or ())
            return self.cursor.fetchall()
        except mysql.connector.Error as err:
            print(f"ошибка при выборке данных: {err}")
            return []

    # заполняем таблицу
    def insert(self, data):
        if not data:
            print("нет данных для вставки")
            return
        columns = ', '.join(f"`{key}`" for key in data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        query = f"INSERT INTO `{self.table_name}` ({columns}) VALUES ({placeholders})"
        try:
            self.cursor.execute(query, tuple(data.values()))
            self.conn.commit()
            print(f"добавлена запись в таблицу '{self.table_name}'")
        except mysql.connector.Error as err:
            print(f"что-то пошло не так: {err}")

    def get_column(self, table: str, column: str,
                   order: str = 'ASC') -> List[Tuple]:
        #Вывод конкретного столбца в порядке возрастания
        if order.upper() not in ['ASC', 'DESC']:
            order = 'ASC'
        query = f"SELECT `{column}` FROM `{table}` ORDER BY `{column}` {order.upper()}"
        return self.execute(query)

    def get_rows(self, table: str, id_column: str,
                             start_id: int, end_id: int) -> List[Tuple]:
        #Вывод диапазона строк по ID
        query = f"SELECT * FROM `{table}` WHERE `{id_column}` BETWEEN %s AND %s"
        return self.execute(query, (start_id, end_id))

    def delete_rows(self, table: str, id_column: str,
                                start_id: int, end_id: int) -> int:
        #Удаление диапазона строк по ID
        query = f"DELETE FROM `{table}` WHERE `{id_column}` BETWEEN %s AND %s"
        self.connect()
        try:
            self.cursor.execute(query, (start_id, end_id))
            self.connection.commit()
            return self.cursor.rowcount
        except mysql.connector.Error as err:
            if self.connection:
                self.connection.rollback()
            print(f"Ошибка выполнения запроса: {err}")
            return 0

    def get_table_structure(self, table: str) -> List[Tuple]:
        #Вывод структуры таблицы
        query = f"DESCRIBE `{table}`"
        return self.execute(query)

    def get_row_by_value(self, table: str, column: str, value: Any) -> Optional[Tuple]:
        #Вывод строки, содержащей конкретное значение в конкретном столбце
        query = f"SELECT * FROM `{table}` WHERE `{column}` = %s LIMIT 1"
        result = self.execute(query, (value,))
        return result[0] if result else None

    def drop_table(self):
        cursor = self.conn.cursor()
        try:
            query = f"DROP TABLE IF EXISTS {self.table_name}"
            cursor.execute(query)
            self.conn.commit()
        finally:
            cursor.close()
        print(f"таблица '{self.table_name}' удалена")

    def add_column(self, table: str, column: str, dtype: str,
                   default: Any = None, not_null: bool = False) -> bool:
        #Добавление нового столбца
        constraints = []
        if not_null:
            constraints.append("NOT NULL")
        if default is not None:
            if isinstance(default, str):
                constraints.append(f"DEFAULT '{default}'")
            else:
                constraints.append(f"DEFAULT {default}")
        constraint_str = " ".join(constraints)
        query = f"ALTER TABLE `{table}` ADD COLUMN `{column}` {dtype} {constraint_str}"
        try:
            self.connect()
            self.cursor.execute(query)
            self.connection.commit()
            return True
        except Error as e:
            print(f"Error: {e}")
            return False
        finally:
            self.disconnect()

    def drop_column(self, table: str, column: str) -> bool:
        # Удаление столбца
        query = f"ALTER TABLE `{table}` DROP COLUMN `{column}`"
        try:
            self.connect()
            self.cursor.execute(query)
            self.connection.commit()
            return True
        except Error as e:
            print(f"Error: {e}")
            return False
        finally:
            self.disconnect()

    def export_to_csv(self, table: str, filename: str) -> bool:
        # Экспорт таблицы в csv
        try:
            self.connect()
            self.cursor.execute(f"SELECT * FROM `{table}`")
            rows = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]

            with open(filename, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            return True
        except Exception as e:
            print(f"Error: {e}")
            return False
        finally:
            self.disconnect()

    def import_from_csv(self, table: str, filename: str,
                        create_table: bool = False) -> bool:
        # Импорт таблицы из csv
        try:
            with (open(filename, 'r', encoding='utf-8') as f):
                reader = csv.reader(f)
                headers = next(reader)
                self.connect()
                if create_table:
                    column_defs = [f"`{col}` TEXT" for col in headers]
                    self.cursor.execute(f"""
                                CREATE TABLE IF NOT EXISTS `{table}` 
                                ({', '.join(column_defs)})
                            """)
                placeholders = ', '.join(['%s' for _ in headers])
                query = f"""
                            INSERT INTO `{table}` ({', '.join([f'`{h}`' for h in headers])}) 
                            VALUES ({placeholders})
                        """
                self.cursor.executemany(query, list(reader))
                self.connection.commit()
                return True
        except Exception as e:
                print(f"Error: {e}")
                return False
        finally:
            self.disconnect()


















